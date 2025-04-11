"""
同步因子数据的模块。
该模块提供了从 AlphaPower API 同步因子数据到数据库的功能。
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
    CompetitionView,
    RegularView,
    SelfAlphaListQueryParams,
    WorldQuantClient,
    wq_client,
)
from alphapower.constants import Database
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
from alphapower.internal.logging import setup_logging

from .utils import create_sample

# 配置日志
console_logger: BoundLogger = setup_logging(__name__, enable_console=True)
file_logger: BoundLogger = setup_logging(__name__, enable_console=False)

# TODO(Ball Chang): 支持全量和增量同步，努力提高数据同步并发度和写入性能
# TODO(Ball Chang): 找一个好的解决方案来判断因子回测配置是否相同
# TODO(Ball Chang): 整理重复的公共逻辑，放到同一个模块里管理

# 全局事件，用于通知所有协程终止操作
exit_event: asyncio.Event = asyncio.Event()


def create_alphas_settings(alpha_data: AlphaView) -> Setting:
    """
    创建 AlphaSettings 实例。

    Args:
        alpha_data: 包含因子设置信息的数据对象

    Returns:
        Setting: 创建的因子设置实例
    """
    return Setting(
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
        test_period=getattr(alpha_data.settings, "test_period", None),
    )


def create_alphas_regular(regular: RegularView) -> Regular:
    """
    创建 AlphaRegular 实例。

    Args:
        regular: AlphaView.Regular 对象，包含因子规则的详细信息。

    Returns:
        Regular: 创建的因子规则实例
    """
    return Regular(
        code=regular.code,
        description=getattr(regular, "description", None),
        operator_count=regular.operator_count,
    )


async def create_alpha_classifications(
    classifications_data: Optional[List[ClassificationView]],
) -> List[Classification]:
    """
    创建或获取 Classification 实例列表。

    Args:
        classifications_data: 分类数据列表。

    Returns:
        List[Classification]: Classification 实例列表。
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
                classification_id=data.id,
                name=data.name,
            )

            classification = await classification_dal.upsert_by_unique_key(
                classification, "classification_id"
            )
            entity_objs.append(classification)

    return entity_objs


async def create_alpha_competitions(
    competitions_data: Optional[List[CompetitionView]],
) -> List[Competition]:
    """
    创建或获取 AlphaCompetition 实例列表。

    Args:
        competitions_data: 比赛数据列表。

    Returns:
        List[Competition]: Competition 实例列表。
    """
    if competitions_data is None:
        return []

    entity_objs: List[Competition] = []

    async with get_db_session(Database.ALPHAS) as session:
        # 使用 DALFactory 创建 DAL 实例
        competition_dal: CompetitionDAL = DALFactory.create_dal(CompetitionDAL, session)

        for data in competitions_data:
            competition = Competition(
                competition_id=data.id,
                name=data.name,
            )

            competition = await competition_dal.upsert_by_unique_key(
                competition, "competition_id"
            )
            entity_objs.append(competition)

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
        alpha_data: AlphaView 对象，包含因子详细信息。
        settings: AlphaSettings 实例。
        regular: AlphaRegular 实例。
        classifications: Classification 实例列表。
        competitions: AlphaCompetition 实例列表。

    Returns:
        Alpha: 创建的 Alpha 实例。
    """
    return Alpha(
        alpha_id=alpha_data.id,
        type=alpha_data.type,
        author=alpha_data.author,
        settings=settings,
        regular=regular,
        date_created=alpha_data.date_created,
        date_submitted=getattr(alpha_data, "date_submitted", None),
        date_modified=alpha_data.date_modified,
        name=getattr(alpha_data, "name", None),
        favorite=alpha_data.favorite,
        hidden=alpha_data.hidden,
        color=getattr(alpha_data, "color", None),
        category=getattr(alpha_data, "category", None),
        tags=",".join(alpha_data.tags) if alpha_data.tags else None,
        classifications=classifications,
        grade=alpha_data.grade,
        stage=alpha_data.stage,
        status=alpha_data.status,
        in_sample=create_sample(alpha_data.in_sample),
        out_sample=create_sample(alpha_data.out_sample),
        train=create_sample(alpha_data.train),
        test=create_sample(alpha_data.test),
        prod=create_sample(alpha_data.prod),
        competitions=competitions,
        themes=",".join(alpha_data.themes) if alpha_data.themes else None,
        # TODO(Ball Chang): pyramids 字段需要重新设计
        # pyramids=",".join(alpha_data.pyramids) if alpha_data.pyramids else None,
        pyramids=None,
        team=",".join(alpha_data.team) if alpha_data.team else None,
    )


async def process_alphas_page(alphas_results: List[AlphaView]) -> Tuple[int, int]:
    """
    异步处理单页 alphas 数据。

    Args:
        alphas_results: 要处理的因子数据列表

    Returns:
        Tuple[int, int]: 插入和更新的因子数量元组
    """
    inserted_alphas: int = 0
    updated_alphas: int = 0

    for alpha_data in alphas_results:
        if exit_event.is_set():
            await file_logger.awarning("检测到退出事件，中止处理因子页面", emoji="⚠️")
            break
        alpha_id: str = alpha_data.id

        settings: Setting = create_alphas_settings(alpha_data)
        regular: Regular = create_alphas_regular(alpha_data.regular)
        classifications: List[Classification] = await create_alpha_classifications(
            alpha_data.classifications
        )
        competitions: List[Competition] = await create_alpha_competitions(
            alpha_data.competitions
        )
        alpha: Alpha = create_alphas(
            alpha_data, settings, regular, classifications, competitions
        )

        async with get_db_session(Database.ALPHAS) as session:
            alpha_dal: AlphaDAL = AlphaDAL(session)
            existing_alpha: Optional[Alpha] = await alpha_dal.find_by_alpha_id(alpha_id)

            if existing_alpha:
                alpha.id = existing_alpha.id
                await alpha_dal.update(alpha)
                updated_alphas += 1
            else:
                await alpha_dal.create(alpha)
                inserted_alphas += 1

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
        alphas_data_result, _ = await client.get_self_alphas(query=query_params)

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
        alphas_data_result, _ = await client.get_self_alphas(query=query_params)

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


async def sync_alphas(start_time: datetime, end_time: datetime, parallel: int) -> None:
    """
    异步同步因子。

    Args:
        start_time: 开始时间。
        end_time: 结束时间。
        parallel: 并行任务数。

    Raises:
        ValueError: 当开始时间晚于或等于结束时间时抛出
    """
    if start_time >= end_time:
        raise ValueError("start_time 必须早于 end_time。")

    # 使用正确的异步日志方法
    await file_logger.ainfo("开始同步因子", emoji="🚀")

    def handle_exit_signal(signum: int, _: Optional[types.FrameType]) -> None:
        """
        处理退出信号的函数。

        在接收到退出信号时，执行资源清理操作并通知协程退出。

        Args:
            signum (int): 信号编号。
            frame (Optional[types.FrameType]): 信号处理的当前帧。
        """
        file_logger.warning(
            "接收到退出信号，准备终止操作",
            signal=signum,
            emoji="🛑",
        )
        # 设置退出事件，通知所有协程停止操作
        exit_event.set()

    signal.signal(signal.SIGINT, handle_exit_signal)  # 处理 Ctrl+C
    signal.signal(signal.SIGTERM, handle_exit_signal)  # 处理终止信号

    fetched_alphas: int = 0
    inserted_alphas: int = 0
    updated_alphas: int = 0

    async with wq_client:
        try:
            for cur_time in (
                start_time + timedelta(days=i)
                for i in range((end_time - start_time).days + 1)
            ):
                if exit_event.is_set():
                    await file_logger.awarning(
                        "检测到退出事件，中止因子同步", emoji="⚠️"
                    )
                    break
                fetched, inserted, updated = await process_alphas_for_date(
                    wq_client, cur_time, parallel
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
                    emoji="📊",
                )

            # 使用正确的异步日志方法
            await file_logger.ainfo(
                "因子同步完成",
                fetched=fetched_alphas,
                inserted=inserted_alphas,
                updated=updated_alphas,
                emoji="✅",
            )
        except Exception as e:
            # 使用正确的异步日志方法
            await file_logger.aerror(
                "同步因子时出错", error=str(e), exc_info=True, emoji="❌"
            )
        finally:
            if exit_event.is_set():
                await file_logger.ainfo("因子同步被中止", emoji="🛑")
