"""
同步因子数据模块。

该模块提供了从 AlphaPower API 同步因子数据到数据库的功能，支持全量和增量同步。
主要功能包括：
1. 获取因子数据并处理（支持并行分片处理）。
2. 将因子数据插入或更新到数据库。
3. 支持因子分类和竞赛数据的关联处理。
4. 提供日志记录，支持调试、信息、警告和错误级别的日志输出。

模块特点：
- 使用异步 IO 提高数据同步效率。
- 支持通过信号处理器优雅地终止同步操作。
- 提供详细的日志记录，便于问题排查和性能监控。
"""

import asyncio
import signal
import types
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

from structlog.stdlib import BoundLogger

from alphapower.client import (
    AlphaView,
    ClassificationView,
    CompetitionRefView,
    RegularView,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
    WorldQuantClient,
    wq_client,
)
from alphapower.constants import Color, Database, Grade
from alphapower.dal.alphas import (
    AlphaDAL,
    ClassificationDAL,
    CompetitionDAL,
)
from alphapower.dal.base import DALFactory
from alphapower.entity import (
    Alpha,
    Classification,
    Competition,
    Regular,
    Setting,
)
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import get_logger

from .sync_competition import competition_data_expire_check, sync_competition
from .utils import create_sample

# 配置日志
console_logger: BoundLogger = get_logger(__name__, enable_console=True)
file_logger: BoundLogger = get_logger(__name__, enable_console=False)

# TODO(Ball Chang): 支持全量和增量同步，努力提高数据同步并发度和写入性能
# TODO(Ball Chang): 找一个好的解决方案来判断因子回测配置是否相同

# 全局事件，用于通知所有协程终止操作
exit_event: asyncio.Event = asyncio.Event()


def create_alphas_settings(alpha_data: AlphaView) -> Setting:
    """
    创建 AlphaSettings 实例。

    Args:
        alpha_data (AlphaView): 包含因子设置信息的数据对象。

    Returns:
        Setting: 创建的因子设置实例。

    Raises:
        AttributeError: 如果 alpha_data 中缺少必要的字段。

    说明:
        该函数将 AlphaView 中的设置信息提取并转换为 Setting 实例。
        主要用于因子数据的标准化处理。
    """
    try:
        return Setting(
            # 提取因子设置中的各个字段
            instrument_type=alpha_data.settings.instrument_type,
            region=alpha_data.settings.region,
            universe=alpha_data.settings.universe,
            delay=alpha_data.settings.delay,
            decay=alpha_data.settings.decay,
            neutralization=alpha_data.settings.neutralization,
            truncation=alpha_data.settings.truncation,
            pasteurization=alpha_data.settings.pasteurization,
            unit_handling=alpha_data.settings.unit_handling,
            nan_handling=alpha_data.settings.nan_handling,
            language=alpha_data.settings.language,
            visualization=alpha_data.settings.visualization,
            test_period=getattr(alpha_data.settings, "test_period", None),  # 可选字段
        )
    except AttributeError as e:
        raise AttributeError(f"因子数据缺少必要字段: {e}") from e


def create_alphas_regular(regular: RegularView) -> Regular:
    """
    创建 AlphaRegular 实例。

    Args:
        regular (RegularView): 包含因子规则详细信息的对象。

    Returns:
        Regular: 创建的因子规则实例。

    说明:
        该函数将 RegularView 中的规则信息提取并转换为 Regular 实例。
        主要用于因子规则的标准化处理。
    """
    return Regular(
        code=regular.code,  # 因子规则代码
        description=getattr(regular, "description", None),  # 可选描述
        operator_count=regular.operator_count,  # 操作符数量
    )


async def create_alpha_classifications(
    classifications_data: Optional[List[ClassificationView]],
) -> List[Classification]:
    """
    创建或获取 Classification 实例列表。

    Args:
        classifications_data (Optional[List[ClassificationView]]): 分类数据列表。

    Returns:
        List[Classification]: Classification 实例列表。

    说明:
        该函数会根据分类数据创建或更新数据库中的分类记录。
        如果分类数据为空，则返回空列表。
    """
    if classifications_data is None:
        return []

    entity_objs: List[Classification] = []

    async with get_db_session(Database.ALPHAS) as session:
        # 使用 DALFactory 创建 DAL 实例
        classification_dal: ClassificationDAL = DALFactory.create_dal(
            ClassificationDAL, session
        )

        for data in classifications_data:
            classification = Classification(
                classification_id=data.id,  # 分类唯一标识
                name=data.name,  # 分类名称
            )

            # 根据唯一键插入或更新分类记录
            classification = await classification_dal.upsert_by_unique_key(
                classification, "classification_id"
            )
            entity_objs.append(classification)

    return entity_objs


async def query_alpha_competitions(
    competitions_data: Optional[List[CompetitionRefView]],
) -> List[Competition]:
    """
    创建或获取 AlphaCompetition 实例列表。

    Args:
        competitions_data (Optional[List[CompetitionRefView]]): 比赛数据列表。

    Returns:
        List[Competition]: Competition 实例列表。

    Raises:
        ValueError: 如果未找到任何比赛数据。
        RuntimeError: 如果数据库查询失败。

    说明:
        该函数会根据比赛数据查询数据库中的比赛记录。
        如果未找到任何比赛记录，则抛出异常。
    """
    if competitions_data is None:
        return []

    competition_ids: List[str] = [
        competition.id for competition in competitions_data if competition.id
    ]

    try:
        async with get_db_session(Database.ALPHAS) as session:
            # 使用 DALFactory 创建 DAL 实例
            competition_dal: CompetitionDAL = DALFactory.create_dal(
                CompetitionDAL, session
            )

            entity_objs: List[Competition] = await competition_dal.find_by(
                in_={"competition_id": competition_ids}
            )
            if not entity_objs:
                raise ValueError("没有找到任何比赛数据，请检查比赛数据是否正确。")
    except Exception as e:
        raise RuntimeError(f"查询比赛数据时发生错误: {e}") from e

    return entity_objs


def create_alphas(
    alpha_data: AlphaView,
    settings: Setting,
    regular: Regular,
    classifications: List[Classification],
    competitions: List[Competition],
) -> Alpha:
    """
    创建 Alpha 实例。

    Args:
        alpha_data (AlphaView): 包含因子详细信息的对象。
        settings (Setting): 因子设置实例。
        regular (Regular): 因子规则实例。
        classifications (List[Classification]): 分类实例列表。
        competitions (List[Competition]): 比赛实例列表。

    Returns:
        Alpha: 创建的 Alpha 实例。

    说明:
        该函数将因子数据、设置、规则、分类和比赛信息整合为一个 Alpha 实例。
        主要用于因子数据的标准化处理。
    """
    return Alpha(
        alpha_id=alpha_data.id,  # 因子唯一标识
        type=alpha_data.type,  # 因子类型
        author=alpha_data.author,  # 作者信息
        settings=settings,  # 因子设置
        regular=regular,  # 因子规则
        date_created=alpha_data.date_created,  # 创建日期
        date_submitted=getattr(alpha_data, "date_submitted", None),  # 提交日期（可选）
        date_modified=alpha_data.date_modified,  # 修改日期
        name=getattr(alpha_data, "name", None),  # 因子名称（可选）
        favorite=alpha_data.favorite,  # 是否收藏
        hidden=alpha_data.hidden,  # 是否隐藏
        color=alpha_data.color if alpha_data.color else Color.NONE,  # 因子颜色
        category=getattr(alpha_data, "category", None),  # 因子类别（可选）
        tags=alpha_data.tags,  # 因子标签
        grade=alpha_data.grade if alpha_data.grade else Grade.DEFAULT,  # 因子等级
        stage=alpha_data.stage,  # 因子阶段
        status=alpha_data.status,  # 因子状态
        in_sample=create_sample(alpha_data.in_sample),  # 样本内数据
        out_sample=create_sample(alpha_data.out_sample),  # 样本外数据
        train=create_sample(alpha_data.train),  # 训练数据
        test=create_sample(alpha_data.test),  # 测试数据
        prod=create_sample(alpha_data.prod),  # 生产数据
        competitions=competitions,  # 关联的比赛
        classifications=classifications,  # 关联的分类
        themes=",".join(alpha_data.themes) if alpha_data.themes else None,  # 主题
        # TODO(Ball Chang): pyramids 字段需要重新设计
        pyramids=None,
        team=",".join(alpha_data.team) if alpha_data.team else None,  # 团队信息
    )


async def fetch_last_sync_time_range(
    client: WorldQuantClient,
) -> Tuple[datetime, datetime]:
    """
    获取上次同步的时间范围。

    Args:
        client (WorldQuantClient): 客户端实例。

    Returns:
        Tuple[datetime, datetime]: 上次同步的开始和结束时间。

    Raises:
        RuntimeError: 如果数据库查询或 API 请求失败。

    说明:
        该函数会从数据库或 API 获取最近的因子同步时间范围。
    """
    await file_logger.adebug(
        "进入 fetch_last_sync_time_range 函数", client=str(client), emoji="🔍"
    )

    try:
        async with get_db_session(Database.ALPHAS) as session:
            alpha_dal: AlphaDAL = DALFactory.create_dal(AlphaDAL, session)
            last_alpha: Optional[Alpha] = await alpha_dal.find_one_by(
                order_by=Alpha.date_created.desc(),
            )

            start_time: datetime
            end_time: datetime = datetime.now()

            if last_alpha:
                start_time = last_alpha.date_created
                await file_logger.adebug(
                    "找到最近的因子记录",
                    last_alpha_id=last_alpha.alpha_id,
                    last_alpha_date_created=last_alpha.date_created,
                    emoji="📅",
                )
            else:
                query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
                    limit=1,
                    offset=0,
                    order="dateCreated",
                )

                alphas_data_result: SelfAlphaListView = (
                    await client.alpha_get_self_list(query=query_params)
                )

                if alphas_data_result.count > 0:
                    start_time = alphas_data_result.results[0].date_created
                    await file_logger.adebug(
                        "从 API 获取最近的因子记录",
                        api_result_count=alphas_data_result.count,
                        start_time=start_time,
                        emoji="🌐",
                    )
                else:
                    start_time = datetime.now()
                    await file_logger.awarning(
                        "未找到任何因子记录，使用当前时间作为开始时间",
                        start_time=start_time,
                        emoji="⚠️",
                    )
    except Exception as e:
        raise RuntimeError(f"获取同步时间范围时发生错误: {e}") from e

    await file_logger.adebug(
        "退出 fetch_last_sync_time_range 函数",
        start_time=start_time,
        end_time=end_time,
        emoji="✅",
    )
    return start_time, end_time


async def process_alphas_page(alphas_results: List[AlphaView]) -> Tuple[int, int]:
    """
    异步处理单页 alphas 数据。

    Args:
        alphas_results (List[AlphaView]): 要处理的因子数据列表。

    Returns:
        Tuple[int, int]: 插入和更新的因子数量元组。

    Raises:
        RuntimeError: 如果数据库操作失败。

    说明:
        该函数会将单页因子数据插入或更新到数据库中。
    """
    inserted_alphas: int = 0
    updated_alphas: int = 0

    try:
        async with get_db_session(Database.ALPHAS) as session:
            alpha_dal: AlphaDAL = AlphaDAL(session)

            # 收集所有 competitions 和 classifications 的 ID
            competition_ids: List[str] = [
                competition.id
                for alpha_data in alphas_results
                if alpha_data.competitions
                for competition in alpha_data.competitions
                if competition.id
            ]
            classification_ids: List[str] = [
                classification.id
                for alpha_data in alphas_results
                if alpha_data.classifications
                for classification in alpha_data.classifications
                if classification.id
            ]

            # 批量查询 competitions 和 classifications
            competition_dal: CompetitionDAL = DALFactory.create_dal(
                CompetitionDAL, session
            )
            classification_dal: ClassificationDAL = DALFactory.create_dal(
                ClassificationDAL, session
            )

            competitions_dict: dict[str, Competition] = {
                competition.competition_id: competition
                for competition in await competition_dal.find_by(
                    in_={"competition_id": competition_ids}
                )
            }
            classifications_dict: dict[str, Classification] = {
                classification.classification_id: classification
                for classification in await classification_dal.find_by(
                    in_={"classification_id": classification_ids}
                )
            }

            for alpha_data in alphas_results:
                if exit_event.is_set():
                    await file_logger.awarning(
                        "检测到退出事件，中止处理因子页面", emoji="⚠️"
                    )
                    break
                alpha_id: str = alpha_data.id

                settings: Setting = create_alphas_settings(alpha_data)
                regular: Regular = create_alphas_regular(alpha_data.regular)

                # 填充 classifications 和 competitions 字段
                classifications: List[Classification] = [
                    classifications_dict[classification.id]
                    for classification in alpha_data.classifications or []
                    if classification.id in classifications_dict
                ]
                competitions: List[Competition] = [
                    competitions_dict[competition.id]
                    for competition in alpha_data.competitions or []
                    if competition.id in competitions_dict
                ]

                alpha: Alpha = create_alphas(
                    alpha_data, settings, regular, classifications, competitions
                )

                existing_alpha: Optional[Alpha] = await alpha_dal.find_by_alpha_id(
                    alpha_id
                )

                if existing_alpha:
                    alpha.id = existing_alpha.id
                    await alpha_dal.update(alpha)
                    updated_alphas += 1
                else:
                    await alpha_dal.create(alpha)
                    inserted_alphas += 1
    except Exception as e:
        raise RuntimeError(f"处理因子页面数据时发生错误: {e}") from e

    await file_logger.adebug(
        "处理因子页面数据完成",
        inserted=inserted_alphas,
        updated=updated_alphas,
        emoji="✅",
    )
    return inserted_alphas, updated_alphas


async def process_alphas_for_date(
    client: WorldQuantClient, cur_time: datetime, parallel: int
) -> Tuple[int, int, int]:
    """
    同步处理指定日期的 alphas 数据，支持分片并行处理。

    Args:
        client: WorldQuantClient 客户端实例
        cur_time: 指定处理的日期
        parallel: 并行处理任务数

    Returns:
        Tuple[int, int, int]: 获取、插入和更新的因子数量元组
    """
    fetched_alphas: int = 0
    inserted_alphas: int = 0
    updated_alphas: int = 0

    # 初始化时间范围
    start_time: datetime = cur_time
    end_time: datetime = cur_time + timedelta(days=1)

    while start_time < cur_time + timedelta(days=1):
        query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
            limit=1,
            date_created_gt=start_time.isoformat(),
            date_created_lt=end_time.isoformat(),
        )
        alphas_data_result: Any
        alphas_data_result, _ = await client.alpha_get_self_list(query=query_params)

        if alphas_data_result.count < 10000:
            # 使用正确的异步日志方法
            await file_logger.ainfo(
                "获取日期范围数据",
                start_time=start_time,
                end_time=end_time,
                count=alphas_data_result.count,
                emoji="📅",
            )

            # 分片处理
            tasks: List[asyncio.Task] = []
            page_size: int = 100
            total_pages: int = (alphas_data_result.count + page_size - 1) // page_size
            pages_per_task: int = (total_pages + parallel - 1) // parallel

            for i in range(parallel):
                start_page: int = i * pages_per_task + 1
                end_page: int = min((i + 1) * pages_per_task, total_pages)
                if start_page > end_page:
                    break

                task: asyncio.Task = asyncio.create_task(
                    process_alphas_pages(
                        client,
                        start_time,
                        end_time,
                        start_page,
                        end_page,
                        page_size,
                    )
                )

                tasks.append(task)

            results: List[Tuple[int, int, int]] = await asyncio.gather(*tasks)
            for fetched, inserted, updated in results:
                fetched_alphas += fetched
                inserted_alphas += inserted
                updated_alphas += updated

            # 更新时间范围，继续处理后续时间段
            start_time = end_time
            end_time = cur_time + timedelta(days=1)
        else:
            # 缩小时间范围
            mid_time: datetime = start_time + (end_time - start_time) / 2
            end_time = mid_time
            # 使用正确的异步日志方法
            await file_logger.ainfo(
                "数据量超过限制，缩小日期范围",
                start_time=start_time,
                end_time=end_time,
                emoji="⚠️",
            )

    return fetched_alphas, inserted_alphas, updated_alphas


async def process_alphas_pages(
    client: WorldQuantClient,
    start_time: datetime,
    end_time: datetime,
    start_page: int,
    end_page: int,
    page_size: int,
) -> Tuple[int, int, int]:
    """
    处理指定页范围内的 alphas 数据。

    Args:
        client: WorldQuantClient 客户端实例
        start_time: 开始时间
        end_time: 结束时间
        start_page: 起始页码
        end_page: 结束页码
        page_size: 每页大小

    Returns:
        Tuple[int, int, int]: 获取、插入和更新的因子数量元组
    """
    fetched_alphas: int = 0
    inserted_alphas: int = 0
    updated_alphas: int = 0

    for page in range(start_page, end_page + 1):
        if exit_event.is_set():
            await file_logger.awarning("检测到退出事件，中止处理因子页范围", emoji="⚠️")
            break
        query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
            limit=page_size,
            offset=(page - 1) * page_size,
            date_created_gt=start_time.isoformat(),
            date_created_lt=end_time.isoformat(),
            order="dateCreated",
        )

        alphas_data_result: Any
        alphas_data_result, _ = await client.alpha_get_self_list(query=query_params)

        if not alphas_data_result.results:
            break

        fetched_alphas += len(alphas_data_result.results)
        # 使用正确的异步日志方法
        await file_logger.ainfo(
            "获取因子页面数据",
            start_time=start_time,
            end_time=end_time,
            page=page,
            count=len(alphas_data_result.results),
            emoji="🔍",
        )

        inserted, updated = await process_alphas_page(alphas_data_result.results)
        inserted_alphas += inserted
        updated_alphas += updated

    return fetched_alphas, inserted_alphas, updated_alphas


async def fetch_and_process_alphas(
    client: WorldQuantClient,
    start_time: datetime,
    end_time: datetime,
    parallel: int,
) -> Tuple[int, int, int]:
    """
    获取并处理指定时间范围内的因子数据。

    Args:
        client: WorldQuantClient 客户端实例
        start_time: 开始时间
        end_time: 结束时间
        parallel: 并行处理任务数

    Returns:
        Tuple[int, int, int]: 获取、插入和更新的因子数量元组
    """
    fetched_alphas: int = 0
    inserted_alphas: int = 0
    updated_alphas: int = 0

    for cur_time in (
        start_time + timedelta(days=i) for i in range((end_time - start_time).days + 1)
    ):
        if exit_event.is_set():
            await file_logger.awarning(
                "检测到退出事件，中止因子同步",
                current_date=cur_time,
                module=__name__,
                emoji="⚠️",
            )
            break

        fetched, inserted, updated = await process_alphas_for_date(
            client, cur_time, parallel
        )
        fetched_alphas += fetched
        inserted_alphas += inserted
        updated_alphas += updated

        # 添加调试日志输出同步进度
        await file_logger.adebug(
            "同步进度更新",
            current_date=cur_time,
            fetched=fetched_alphas,
            inserted=inserted_alphas,
            updated=updated_alphas,
            module=__name__,
            emoji="📊",
        )

    return fetched_alphas, inserted_alphas, updated_alphas


def setup_exit_signal_handler() -> None:
    """
    设置退出信号处理器。

    在接收到退出信号时，执行资源清理操作并通知协程退出。
    """

    def handle_exit_signal(signum: int, _: Optional[types.FrameType]) -> None:
        file_logger.warning(
            "接收到退出信号，准备终止操作",
            signal=signum,
            module=__name__,
            emoji="🛑",
        )
        # 设置退出事件，通知所有协程停止操作
        exit_event.set()

    signal.signal(signal.SIGINT, handle_exit_signal)  # 处理 Ctrl+C
    signal.signal(signal.SIGTERM, handle_exit_signal)  # 处理终止信号


async def sync_alphas(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    increamental: bool = False,
    parallel: int = 1,
) -> None:
    """
    异步同步因子。

    Args:
        start_time: 开始时间。
        end_time: 结束时间。
        increamental: 是否增量同步。
        parallel: 并行任务数。

    Raises:
        ValueError: 当开始时间晚于或等于结束时间时抛出
    """
    if increamental:
        async with wq_client:
            sync_time_range: Tuple[datetime, datetime] = (
                await fetch_last_sync_time_range(wq_client)
            )

            start_time = (
                max(sync_time_range[0], start_time)
                if start_time
                else sync_time_range[0]
            )
            end_time = (
                min(sync_time_range[1], end_time) if end_time else sync_time_range[1]
            )
    else:
        if start_time is None:
            start_time = datetime.now() - timedelta(days=1)
        if end_time is None:
            end_time = datetime.now()

    if start_time >= end_time:
        raise ValueError("start_time 必须早于 end_time。")

    # 检查是否需要同步竞赛数据
    if competition_data_expire_check():
        await file_logger.ainfo(
            "竞赛数据过期，准备同步",
            start_time=start_time,
            end_time=end_time,
            emoji="🛠️",
        )
        await sync_competition()
        await file_logger.ainfo(
            "竞赛数据同步完成",
            start_time=start_time,
            end_time=end_time,
            emoji="✅",
        )

    # 设置退出信号处理器
    setup_exit_signal_handler()

    # 使用正确的异步日志方法
    await file_logger.ainfo("开始同步因子", emoji="🚀")

    async with wq_client:
        try:
            fetched_alphas, inserted_alphas, updated_alphas = (
                await fetch_and_process_alphas(
                    wq_client, start_time, end_time, parallel
                )
            )

            # 使用正确的异步日志方法
            await file_logger.ainfo(
                "因子同步完成",
                fetched=fetched_alphas,
                inserted=inserted_alphas,
                updated=updated_alphas,
                module=__name__,
                emoji="✅",
            )
        except Exception as e:
            # 使用正确的异步日志方法
            await file_logger.aerror(
                "同步因子时出错",
                error=str(e),
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
        finally:
            if exit_event.is_set():
                await file_logger.ainfo(
                    "因子同步被中止",
                    fetched=fetched_alphas,
                    inserted=inserted_alphas,
                    updated=updated_alphas,
                    module=__name__,
                    emoji="🛑",
                )
