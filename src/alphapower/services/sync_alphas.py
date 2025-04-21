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
from typing import List, Optional, Tuple

from pydantic import TypeAdapter
from structlog.stdlib import BoundLogger

from alphapower.client import (
    AlphaView,
    ClassificationView,
    PyramidRefView,
    RegularView,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
    WorldQuantClient,
    wq_client,
)
from alphapower.constants import Color, Database, Grade, Status
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


class AlphaSyncService:
    """
    Alpha 同步服务类，提供因子数据的同步功能。
    """

    def __init__(self) -> None:
        """
        初始化 AlphaSyncService 服务类。
        """
        self.log: BoundLogger = get_logger(__name__)
        self.exit_event: asyncio.Event = asyncio.Event()
        self._db_lock: asyncio.Lock = asyncio.Lock()

    def setup_exit_signal_handler(self) -> None:
        """
        设置退出信号处理器。

        在接收到退出信号时，执行资源清理操作并通知协程退出。
        """

        def handle_exit_signal(signum: int, _: Optional[types.FrameType]) -> None:
            self.log.warning(
                "接收到退出信号，准备终止操作",
                signal=signum,
                module=__name__,
                emoji="🛑",
            )
            # 设置退出事件，通知所有协程停止操作
            self.exit_event.set()

        signal.signal(signal.SIGINT, handle_exit_signal)  # 处理 Ctrl+C
        signal.signal(signal.SIGTERM, handle_exit_signal)  # 处理终止信号

    def create_alphas_settings(self, alpha_data: AlphaView) -> Setting:
        """
        创建 AlphaSettings 实例。

        参数:
            alpha_data: 包含因子设置信息的数据对象。

        返回:
            创建的因子设置实例。

        异常:
            AttributeError: 如果 alpha_data 中缺少必要的字段。
        """
        try:
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
        except AttributeError as e:
            raise AttributeError(f"因子数据缺少必要字段: {e}") from e

    def create_alphas_regular(
        self, regular_view: Optional[RegularView]
    ) -> Optional[Regular]:
        """
        创建 AlphaRegular 实例。

        参数:
            regular: 包含因子规则详细信息的对象。

        返回:
            创建的因子规则实例。
        """
        if regular_view is None:
            return None

        regular: Regular = Regular(
            code=regular_view.code,
            description=regular_view.description,
            operator_count=regular_view.operator_count,
        )

        return regular

    async def create_alpha_classifications(
        self,
        classification_dal: ClassificationDAL,
        classifications_data: Optional[List[ClassificationView]],
    ) -> List[Classification]:
        """
        创建或获取 Classification 实例列表。

        参数:
            classifications_data: 分类数据列表。

        返回:
            Classification 实例列表。
        """
        if classifications_data is None:
            return []

        entity_objs: List[Classification] = []

        for data in classifications_data:
            classification = Classification(
                classification_id=data.id,
                name=data.name,
            )
            async with self._db_lock:
                classification = await classification_dal.upsert_by_unique_key(
                    classification, "classification_id"
                )
            entity_objs.append(classification)

        return entity_objs

    def create_alphas(
        self,
        alpha_data: AlphaView,
        settings: Setting,
        regular: Optional[Regular],
        combo: Optional[Regular],
        selection: Optional[Regular],
        classifications: List[Classification],
        competitions: List[Competition],
    ) -> Alpha:
        """
        创建 Alpha 实例。

        参数:
            alpha_data: 包含因子详细信息的对象。
            settings: 因子设置实例。
            regular: 因子规则实例。
            classifications: 分类实例列表。
            competitions: 比赛实例列表。

        返回:
            创建的 Alpha 实例。
        """

        pyramids_adapter: TypeAdapter[List[PyramidRefView]] = TypeAdapter(
            List[PyramidRefView]
        )

        alpha: Alpha = Alpha(
            alpha_id=alpha_data.id,
            type=alpha_data.type,
            author=alpha_data.author,
            settings=settings,
            regular=regular,
            combo=combo,
            selection=selection,
            date_created=alpha_data.date_created,
            date_submitted=getattr(alpha_data, "date_submitted", None),
            date_modified=alpha_data.date_modified,
            name=getattr(alpha_data, "name", None),
            favorite=alpha_data.favorite,
            hidden=alpha_data.hidden,
            color=alpha_data.color if alpha_data.color else Color.NONE,
            category=getattr(alpha_data, "category", None),
            tags=alpha_data.tags,
            grade=alpha_data.grade if alpha_data.grade else Grade.DEFAULT,
            stage=alpha_data.stage,
            status=alpha_data.status,
            in_sample=create_sample(alpha_data.in_sample),
            out_sample=create_sample(alpha_data.out_sample),
            train=create_sample(alpha_data.train),
            test=create_sample(alpha_data.test),
            prod=create_sample(alpha_data.prod),
            competitions=competitions,
            classifications=classifications,
            themes=",".join(alpha_data.themes) if alpha_data.themes else None,
            pyramids=None,
            team=",".join(alpha_data.team) if alpha_data.team else None,
        )
        if alpha_data.pyramids:
            alpha.pyramids = pyramids_adapter.dump_python(alpha_data.pyramids)

        return alpha

    async def fetch_last_sync_time_range(
        self, client: WorldQuantClient
    ) -> Tuple[datetime, datetime]:
        """
        获取上次同步的时间范围。

        参数:
            client: WorldQuantClient 客户端实例。

        返回:
            上次同步的开始和结束时间。
        """
        await self.log.adebug(
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
                    await self.log.adebug(
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
                        await self.log.adebug(
                            "从 API 获取最近的因子记录",
                            api_result_count=alphas_data_result.count,
                            start_time=start_time,
                            emoji="🌐",
                        )
                    else:
                        start_time = datetime.now()
                        await self.log.awarning(
                            "未找到任何因子记录，使用当前时间作为开始时间",
                            start_time=start_time,
                            emoji="⚠️",
                        )
        except Exception as e:
            raise RuntimeError(f"获取同步时间范围时发生错误: {e}") from e

        await self.log.adebug(
            "退出 fetch_last_sync_time_range 函数",
            start_time=start_time,
            end_time=end_time,
            emoji="✅",
        )
        return start_time, end_time

    async def process_alphas_page(
        self,
        alphas_results: List[AlphaView],
        competition_dal: CompetitionDAL,
        classification_dal: ClassificationDAL,
    ) -> Tuple[List[Alpha], int, int]:
        """
        异步处理单页 alphas 数据。

        参数:
            alphas_results: 要处理的因子数据列表。

        返回:
            插入和更新的因子数量元组。
        """
        inserted_alphas: int = 0
        updated_alphas: int = 0
        uncommitted_alphas: List[Alpha] = []

        try:
            competition_ids: List[str] = [
                competition.id
                for alpha_data in alphas_results
                if alpha_data.competitions
                for competition in alpha_data.competitions
                if competition.id
            ]

            async with self._db_lock:
                competitions_dict: dict[str, Competition] = {
                    competition.competition_id: competition
                    for competition in await competition_dal.find_by(
                        in_={"competition_id": competition_ids}
                    )
                }

            for alpha_data in alphas_results:
                if self.exit_event.is_set():
                    await self.log.awarning(
                        "检测到退出事件，中止处理因子页面", emoji="⚠️"
                    )
                    raise RuntimeError("退出事件触发，停止处理因子页面。")
                try:
                    settings: Setting = self.create_alphas_settings(alpha_data)
                    regular: Optional[Regular] = self.create_alphas_regular(
                        alpha_data.regular
                    )
                    combo: Optional[Regular] = self.create_alphas_regular(
                        alpha_data.combo
                    )
                    selection: Optional[Regular] = self.create_alphas_regular(
                        alpha_data.selection
                    )

                    classifications: List[Classification] = (
                        await self.create_alpha_classifications(
                            classification_dal=classification_dal,
                            classifications_data=alpha_data.classifications,
                        )
                    )
                    competitions: List[Competition] = [
                        competitions_dict[competition.id]
                        for competition in alpha_data.competitions or []
                        if competition.id in competitions_dict
                    ]

                    alpha: Alpha = self.create_alphas(
                        alpha_data=alpha_data,
                        settings=settings,
                        regular=regular,
                        combo=combo,
                        selection=selection,
                        classifications=classifications,
                        competitions=competitions,
                    )

                    uncommitted_alphas.append(alpha)
                except Exception as e:
                    await self.log.aerror(
                        "处理单个因子数据时发生错误",
                        alpha_id=alpha_data.id,
                        error=str(e),
                        exc_info=True,
                        emoji="❌",
                    )
                    raise
        except Exception as e:
            await self.log.aerror(
                "处理因子页面数据时发生错误",
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise RuntimeError(f"处理因子页面数据时发生错误: {e}") from e

        await self.log.adebug(
            "处理因子页面数据完成",
            inserted=inserted_alphas,
            updated=updated_alphas,
            emoji="✅",
        )
        return uncommitted_alphas, inserted_alphas, updated_alphas

    async def process_alphas_for_time_range(
        self,
        client: WorldQuantClient,
        start_time: datetime,
        end_time: datetime,
        status: Optional[Status],
        parallel: int,
    ) -> Tuple[int, int, int]:
        """
        同步处理指定日期的 alphas 数据，支持分片并行处理。

        参数:
            client: WorldQuantClient 客户端实例
            start_time: 开始时间
            end_time: 结束时间
            parallel: 并行处理任务数

        返回:
            获取、插入和更新的因子数量元组
        """
        fetched_alphas: int = 0
        inserted_alphas: int = 0
        updated_alphas: int = 0

        cur_time: datetime = start_time
        truncated_end_time: datetime = end_time

        try:
            while cur_time < end_time:
                if self.exit_event.is_set():
                    await self.log.awarning(
                        "检测到退出事件，中止处理日期范围",
                        start_time=start_time,
                        end_time=end_time,
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        emoji="⚠️",
                    )
                    break

                query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
                    limit=1,
                    date_created_gt=cur_time.isoformat(),
                    date_created_lt=truncated_end_time.isoformat(),
                    status_eq=status.value if status else None,
                )
                alphas_data_result: SelfAlphaListView
                alphas_data_result, _ = await client.alpha_get_self_list(
                    query=query_params
                )

                if alphas_data_result.count < 10000:
                    await self.log.ainfo(
                        "获取日期范围数据",
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        count=alphas_data_result.count,
                        emoji="📅",
                    )
                    tasks: List[asyncio.Task] = []
                    page_size: int = 100
                    total_pages: int = (
                        alphas_data_result.count + page_size - 1
                    ) // page_size
                    pages_per_task: int = (total_pages + parallel - 1) // parallel

                    async with get_db_session(Database.ALPHAS) as session:
                        alpha_dal: AlphaDAL = AlphaDAL(session)
                        competition_dal: CompetitionDAL = DALFactory.create_dal(
                            CompetitionDAL, session
                        )
                        classification_dal: ClassificationDAL = DALFactory.create_dal(
                            ClassificationDAL, session
                        )

                        for i in range(parallel):
                            start_page: int = i * pages_per_task + 1
                            end_page: int = min((i + 1) * pages_per_task, total_pages)
                            if start_page > end_page:
                                break

                            task: asyncio.Task = asyncio.create_task(
                                self.process_alphas_pages(
                                    client=client,
                                    start_time=cur_time,
                                    end_time=truncated_end_time,
                                    status=status,
                                    start_page=start_page,
                                    end_page=end_page,
                                    page_size=page_size,
                                    competition_dal=competition_dal,
                                    classification_dal=classification_dal,
                                )
                            )

                            tasks.append(task)

                        results: List[Tuple[List[Alpha], int, int, int]] = (
                            await asyncio.gather(*tasks)
                        )

                        for (
                            uncommitted_alphas,
                            fetched,
                            inserted,
                            updated,
                        ) in results:
                            if self.exit_event.is_set():
                                await self.log.awarning(
                                    "检测到退出事件，中止处理日期范围",
                                    start_time=start_time,
                                    end_time=end_time,
                                    cur_time=cur_time,
                                    truncated_end_time=truncated_end_time,
                                    emoji="⚠️",
                                )
                                break

                            fetched_alphas += fetched
                            inserted_alphas += inserted
                            updated_alphas += updated

                            async with self._db_lock:
                                await alpha_dal.bulk_upsert_by_unique_key(
                                    uncommitted_alphas, unique_key="alpha_id"
                                )
                                await alpha_dal.session.commit()

                    cur_time = truncated_end_time
                    truncated_end_time = end_time
                else:
                    mid_time: datetime = cur_time + (truncated_end_time - cur_time) / 2
                    truncated_end_time = mid_time
                    await self.log.ainfo(
                        "数据量超过限制，缩小日期范围",
                        start_time=start_time,
                        end_time=end_time,
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        emoji="⚠️",
                    )
        except Exception as e:
            await self.log.aerror(
                "处理日期范围内的因子数据时发生错误",
                start_time=start_time,
                end_time=end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise RuntimeError(f"处理日期范围内的因子数据时发生错误: {e}") from e

        return fetched_alphas, inserted_alphas, updated_alphas

    async def process_alphas_pages(
        self,
        client: WorldQuantClient,
        start_time: datetime,
        end_time: datetime,
        status: Optional[Status],
        start_page: int,
        end_page: int,
        page_size: int,
        competition_dal: CompetitionDAL,
        classification_dal: ClassificationDAL,
    ) -> Tuple[List[Alpha], int, int, int]:
        """
        处理指定页范围内的 alphas 数据。

        参数:
            client: WorldQuantClient 客户端实例
            start_time: 开始时间
            end_time: 结束时间
            start_page: 起始页码
            end_page: 结束页码
            page_size: 每页大小

        返回:
            获取、插入和更新的因子数量元组
        """
        fetched_alphas: int = 0
        inserted_alphas: int = 0
        updated_alphas: int = 0
        uncommited_alphas: List[Alpha] = []

        try:
            for page in range(start_page, end_page + 1):
                if self.exit_event.is_set():
                    await self.log.awarning(
                        "检测到退出事件，中止处理因子页范围", emoji="⚠️"
                    )
                    raise RuntimeError("退出事件触发，停止处理因子页范围。")
                query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
                    limit=page_size,
                    offset=(page - 1) * page_size,
                    date_created_gt=start_time.isoformat(),
                    date_created_lt=end_time.isoformat(),
                    order="dateCreated",
                    status_eq=status.value if status else None,
                )
                alphas_data_result: SelfAlphaListView
                alphas_data_result, _ = await client.alpha_get_self_list(
                    query=query_params
                )

                if not alphas_data_result.results:
                    break

                fetched_alphas += len(alphas_data_result.results)
                await self.log.ainfo(
                    "获取因子页面数据",
                    start_time=start_time,
                    end_time=end_time,
                    page=page,
                    count=len(alphas_data_result.results),
                    emoji="🔍",
                )
                alphas, inserted, updated = await self.process_alphas_page(
                    alphas_data_result.results,
                    competition_dal=competition_dal,
                    classification_dal=classification_dal,
                )
                inserted_alphas += inserted
                updated_alphas += updated
                uncommited_alphas.extend(alphas)
        except Exception as e:
            await self.log.aerror(
                "处理因子页范围数据时发生错误",
                start_time=start_time,
                end_time=end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise RuntimeError(f"处理因子页范围数据时发生错误: {e}") from e

        return uncommited_alphas, fetched_alphas, inserted_alphas, updated_alphas

    async def sync_alphas(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        status: Optional[Status] = None,
        increamental: bool = False,
        parallel: int = 1,
    ) -> None:
        """
        异步同步因子。

        参数:
            start_time: 开始时间。
            end_time: 结束时间。
            increamental: 是否增量同步。
            parallel: 并行任务数。
        """
        if increamental:
            async with wq_client:
                sync_time_range: Tuple[datetime, datetime] = (
                    await self.fetch_last_sync_time_range(wq_client)
                )

                start_time = (
                    max(sync_time_range[0], start_time)
                    if start_time
                    else sync_time_range[0]
                )
                end_time = (
                    min(sync_time_range[1], end_time)
                    if end_time
                    else sync_time_range[1]
                )
        else:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=1)
            if end_time is None:
                end_time = datetime.now()

        if start_time >= end_time:
            raise ValueError("start_time 必须早于 end_time。")

        if await competition_data_expire_check():
            await self.log.ainfo(
                "竞赛数据过期，准备同步",
                start_time=start_time,
                end_time=end_time,
                emoji="🛠️",
            )
            await sync_competition()
            await self.log.ainfo(
                "竞赛数据同步完成",
                start_time=start_time,
                end_time=end_time,
                emoji="✅",
            )

        self.setup_exit_signal_handler()

        await self.log.ainfo("开始同步因子", emoji="🚀")

        async with wq_client:
            try:
                for i in range((end_time - start_time).days):
                    cur_start_time: datetime = start_time + timedelta(days=i)
                    cur_end_time: datetime = cur_start_time + timedelta(days=1)
                    cur_end_time = min(cur_end_time, end_time)

                    await self.log.ainfo(
                        "处理时间范围",
                        start_time=cur_start_time,
                        end_time=cur_end_time,
                        emoji="🕒",
                    )

                    if self.exit_event.is_set():
                        await self.log.awarning(
                            "检测到退出事件，中止处理时间范围",
                            start_time=cur_start_time,
                            end_time=cur_end_time,
                            emoji="⚠️",
                        )
                        break

                    fetched_alphas, inserted_alphas, updated_alphas = (
                        await self.process_alphas_for_time_range(
                            client=wq_client,
                            start_time=cur_start_time,
                            end_time=cur_end_time,
                            status=status,
                            parallel=parallel,
                        )
                    )
                await self.log.ainfo(
                    "因子同步完成",
                    fetched=fetched_alphas,
                    inserted=inserted_alphas,
                    updated=updated_alphas,
                    module=__name__,
                    emoji="✅",
                )
            except ValueError as ve:
                await self.log.aerror(
                    "参数错误，无法同步因子",
                    error=str(ve),
                    start_time=start_time,
                    end_time=end_time,
                    module=__name__,
                    emoji="❌",
                )
                raise
            except RuntimeError as re:
                await self.log.aerror(
                    "运行时错误，因子同步失败",
                    error=str(re),
                    exc_info=True,
                    module=__name__,
                    emoji="❌",
                )
                raise
            except Exception as e:
                await self.log.acritical(
                    "未知错误，因子同步中止",
                    error=str(e),
                    exc_info=True,
                    module=__name__,
                    emoji="💥",
                )
                raise
            finally:
                if self.exit_event.is_set():
                    await self.log.ainfo(
                        "因子同步被中止",
                        module=__name__,
                        emoji="🛑",
                    )
