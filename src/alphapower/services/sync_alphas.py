import asyncio
import signal
import types
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from structlog.stdlib import BoundLogger

from alphapower.client import (
    WorldQuantClient,
    wq_client,
)
from alphapower.constants import (
    MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    Color,
    Database,
    Grade,
    Status,
)
from alphapower.dal.alphas import (
    AlphaDAL,
)
from alphapower.dal.base import DALFactory
from alphapower.dal.session_manager import session_manager
from alphapower.entity import (
    Alpha,
)
from alphapower.internal.logging import get_logger
from alphapower.view.alpha import (
    AlphaView,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
)

from .sync_competition import competition_data_expire_check, sync_competition
from .utils import create_aggregate_data


class AlphaSyncService:

    def __init__(self) -> None:

        self.log: BoundLogger = get_logger(__name__)
        self.exit_event: asyncio.Event = asyncio.Event()
        self._db_lock: asyncio.Lock = asyncio.Lock()

    def setup_exit_signal_handler(self) -> None:

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

    def create_alpha(
        self,
        alpha_view: AlphaView,
    ) -> Alpha:
        try:
            alpha: Alpha = Alpha(
                alpha_id=alpha_view.id,
                type=alpha_view.type,
                author=alpha_view.author,
                regular=alpha_view.regular,
                combo=alpha_view.combo,
                selection=alpha_view.selection,
                # 因子模拟配置
                language=alpha_view.settings.language,
                test_period=alpha_view.settings.test_period,
                decay=alpha_view.settings.decay,
                truncation=alpha_view.settings.truncation,
                visualization=alpha_view.settings.visualization,
                instrument_type=alpha_view.settings.instrument_type,
                region=alpha_view.settings.region,
                universe=alpha_view.settings.universe,
                delay=alpha_view.settings.delay,
                neutralization=alpha_view.settings.neutralization,
                pasteurization=alpha_view.settings.pasteurization,
                unit_handling=alpha_view.settings.unit_handling,
                nan_handling=alpha_view.settings.nan_handling,
                max_trade=alpha_view.settings.max_trade,
                # 因子模拟配置结束
                date_created=alpha_view.date_created,
                date_submitted=alpha_view.date_submitted,
                date_modified=alpha_view.date_modified,
                name=alpha_view.name,
                favorite=alpha_view.favorite,
                hidden=alpha_view.hidden,
                color=alpha_view.color if alpha_view.color else Color.NONE,
                category=alpha_view.category,
                tags=alpha_view.tags,
                grade=alpha_view.grade if alpha_view.grade else Grade.DEFAULT,
                stage=alpha_view.stage,
                status=alpha_view.status,
                in_sample=create_aggregate_data(alpha_view.in_sample),
                out_sample=create_aggregate_data(alpha_view.out_sample),
                train=create_aggregate_data(alpha_view.train),
                test=create_aggregate_data(alpha_view.test),
                prod=create_aggregate_data(alpha_view.prod),
                pyramids=alpha_view.pyramids,
                competitions=alpha_view.competitions,
                classifications=alpha_view.classifications,
                themes=",".join(alpha_view.themes) if alpha_view.themes else None,
                team=",".join(alpha_view.team) if alpha_view.team else None,
            )
            return alpha
        except AttributeError as e:
            self.log.error(
                "创建因子时发生属性错误",
                error=str(e),
                alpha_view=alpha_view.__dict__,
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
            raise
        except Exception as e:
            self.log.error(
                "创建因子时发生未知错误",
                error=str(e),
                alpha_view=alpha_view.__dict__,
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
            raise

    async def fetch_first_create_time(self) -> datetime:
        try:
            async with wq_client as client:
                query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
                    limit=1,
                    offset=0,
                    order="dateCreated",
                )
                alphas_data_result: SelfAlphaListView = (
                    await client.alpha_get_self_list(query=query_params)
                )
                if alphas_data_result.count > 0:
                    return alphas_data_result.results[0].date_created
                else:
                    return datetime.now()
        except Exception as e:
            await self.log.aerror(
                "获取第一个创建时间时发生错误",
                error=str(e),
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
            raise
        finally:
            await self.log.ainfo(
                "退出 fetch_first_create_time 函数",
                emoji="✅",
            )

    async def fetch_last_create_time(self) -> datetime:
        try:
            async with wq_client as client:
                query_params: SelfAlphaListQueryParams = SelfAlphaListQueryParams(
                    limit=1,
                    offset=0,
                    order="-dateCreated",
                )
                alphas_data_result: SelfAlphaListView = (
                    await client.alpha_get_self_list(query=query_params)
                )
                if alphas_data_result.count > 0:
                    return alphas_data_result.results[0].date_created
                else:
                    return datetime.now()
        except Exception as e:
            await self.log.aerror(
                "获取最后创建时间时发生错误",
                error=str(e),
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
            raise
        finally:
            await self.log.ainfo(
                "退出 fetch_last_create_time 函数",
                emoji="✅",
            )

    async def fetch_last_sync_time_range(
        self, client: WorldQuantClient
    ) -> Tuple[datetime, datetime]:

        await self.log.adebug(
            "进入 fetch_last_sync_time_range 函数", client=str(client), emoji="🔍"
        )

        try:
            async with session_manager.get_session(Database.ALPHAS) as session:
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
        except AttributeError as e:
            await self.log.aerror(
                "获取同步时间范围时发生属性错误",
                error=str(e),
                client=str(client),
                exc_info=True,
                module=__name__,
                emoji="❌",
            )
            raise
        except Exception as e:
            await self.log.acritical(
                "获取同步时间范围时发生未知错误",
                error=str(e),
                client=str(client),
                exc_info=True,
                module=__name__,
                emoji="💥",
            )
            raise

        await self.log.adebug(
            "退出 fetch_last_sync_time_range 函数",
            start_time=start_time,
            end_time=end_time,
            emoji="✅",
        )
        est: timezone = timezone(timedelta(hours=-5))  # 定义 EST 时区
        start_time = start_time.replace(tzinfo=est)
        end_time = end_time.replace(tzinfo=est)
        return start_time, end_time

    async def process_alphas_page(
        self,
        alpha_dal: AlphaDAL,
        alphas_results: List[AlphaView],
        dry_run: bool = False,
    ) -> Tuple[int, int, int]:
        new_alphas: List[Alpha] = []
        existing_alphas: List[Alpha] = []
        fetched: int = len(alphas_results)
        inserted: int = 0
        modified: int = 0

        try:
            async with session_manager.get_session(
                Database.ALPHAS, readonly=True
            ) as session:
                for alpha_view in alphas_results:
                    if self.exit_event.is_set():
                        await self.log.awarning(
                            "检测到退出事件，中止处理因子页面",
                            emoji="⚠️",
                        )
                        return 0, 0, 0

                    try:
                        alpha: Alpha = self.create_alpha(
                            alpha_view=alpha_view,
                        )

                        exist_id: Optional[int] = await alpha_dal.find_one_id_by(
                            session=session, alpha_id=alpha.alpha_id
                        )
                        if exist_id:
                            alpha.id = exist_id
                            existing_alphas.append(alpha)
                            modified += 1
                        else:
                            new_alphas.append(alpha)
                            inserted += 1

                    except Exception as e:
                        await self.log.aerror(
                            "处理单个因子数据时发生错误",
                            alpha_id=alpha_view.id,
                            error=str(e),
                            exc_info=True,
                            module=__name__,
                            emoji="❌",
                        )
                        raise  # 终止继续同步，抛出异常

            if dry_run:
                await self.log.ainfo(
                    "干运行模式，跳过数据库写入",
                    new_alphas_count=len(new_alphas),
                    existing_alphas_count=len(existing_alphas),
                    emoji="🛠️",
                )
                return fetched, inserted, modified

            async with (
                session_manager.get_session(Database.ALPHAS) as session,
                session.begin(),
            ):
                if new_alphas:
                    await alpha_dal.bulk_create(
                        session=session,
                        entities=new_alphas,
                    )

                    await self.log.ainfo(
                        "新因子数据插入完成",
                        count=len(new_alphas),
                        emoji="✅",
                    )

                if existing_alphas:
                    await alpha_dal.bulk_upsert(
                        session=session,
                        entities=existing_alphas,
                    )

                    await self.log.ainfo(
                        "现有因子数据更新完成",
                        count=len(existing_alphas),
                        emoji="✅",
                    )
        except Exception as e:
            await self.log.acritical(
                "单页数据处理时发生严重错误",
                error=str(e),
                exc_info=True,
                module=__name__,
                emoji="💥",
            )
            raise  # 终止继续同步，抛出异常

        await self.log.adebug(
            "单页数据处理完成",
            fetched=fetched,
            inserted=inserted,
            modified=modified,
            emoji="✅",
        )
        return fetched, inserted, modified

    async def process_alphas_for_time_range(
        self,
        alpha_dal: AlphaDAL,
        client: WorldQuantClient,
        start_time: datetime,
        end_time: datetime,
        status: Optional[Status],
        parallel: int,
        dry_run: bool,
        max_count_per_loop: int = MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    ) -> Tuple[int, int, int]:
        """处理指定时间范围内的因子数据"""
        cur_time: datetime = start_time
        truncated_end_time: datetime = end_time

        fetched: int = 0
        inserted: int = 0
        modified: int = 0

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

                query_params = self._build_query_params(
                    cur_time, truncated_end_time, status
                )
                alphas_data_result = await self._fetch_alphas_data(client, query_params)

                if alphas_data_result.count < min(
                    max_count_per_loop, MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY
                ):
                    range_fetched, range_inserted, range_modified = (
                        await self._process_alphas_in_range(
                            alpha_dal=alpha_dal,
                            client=client,
                            cur_time=cur_time,
                            truncated_end_time=truncated_end_time,
                            alphas_data_result=alphas_data_result,
                            status=status,
                            parallel=parallel,
                            dry_run=dry_run,
                        )
                    )
                    cur_time = truncated_end_time
                    truncated_end_time = end_time

                    fetched += range_fetched
                    inserted += range_inserted
                    modified += range_modified
                else:
                    truncated_end_time = cur_time + (truncated_end_time - cur_time) / 2
                    await self.log.ainfo(
                        "数据量超过限制，缩小日期范围",
                        start_time=start_time,
                        end_time=end_time,
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        emoji="⚠️",
                    )
        except asyncio.CancelledError:
            await self.log.awarning(
                "任务被取消，中止处理日期范围",
                start_time=start_time,
                end_time=end_time,
                emoji="⚠️",
            )
            raise
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
        finally:
            await self.log.ainfo(
                "退出 process_alphas_for_time_range 函数",
                start_time=start_time,
                end_time=end_time,
                emoji="✅",
            )

        return fetched, inserted, modified

    def _build_query_params(
        self, cur_time: datetime, truncated_end_time: datetime, status: Optional[Status]
    ) -> SelfAlphaListQueryParams:
        """构建查询参数"""
        try:
            return SelfAlphaListQueryParams(
                limit=1,
                date_created_gt=cur_time.isoformat(),
                date_created_lt=truncated_end_time.isoformat(),
                status_eq=status.value if status else None,
            )
        except Exception as e:
            self.log.error(
                "构建查询参数时发生错误",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise

    async def _fetch_alphas_data(
        self, client: WorldQuantClient, query_params: SelfAlphaListQueryParams
    ) -> SelfAlphaListView:
        """从 API 获取因子数据"""
        try:
            alphas_data_result, _ = await client.alpha_get_self_list(query=query_params)
            return alphas_data_result
        except asyncio.CancelledError:
            await self.log.awarning(
                "任务被取消，中止获取因子数据",
                query_params=query_params.__dict__,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.aerror(
                "从 API 获取因子数据时发生错误",
                query_params=query_params.__dict__,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise

    async def _process_alphas_in_range(
        self,
        alpha_dal: AlphaDAL,
        client: WorldQuantClient,
        cur_time: datetime,
        truncated_end_time: datetime,
        alphas_data_result: SelfAlphaListView,
        status: Optional[Status],
        parallel: int,
        dry_run: bool,
    ) -> Tuple[int, int, int]:
        """处理指定范围内的因子数据"""
        try:
            await self.log.ainfo(
                "获取日期范围数据",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                count=alphas_data_result.count,
                emoji="📅",
            )
            tasks = self._create_processing_tasks(
                alpha_dal=alpha_dal,
                client=client,
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                alphas_data_result=alphas_data_result,
                parallel=parallel,
                status=status,
                dry_run=dry_run,
            )
            results = await asyncio.gather(*tasks, return_exceptions=True)

            fetched: int = 0
            inserted: int = 0
            modified: int = 0

            for result in results:
                if self.exit_event.is_set():
                    await self.log.awarning(
                        "检测到退出事件，中止处理任务",
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        emoji="⚠️",
                    )
                    break

                if isinstance(result, Exception):
                    await self.log.awarning(
                        "处理任务中发生错误",
                        error=str(result),
                        cur_time=cur_time,
                        truncated_end_time=truncated_end_time,
                        emoji="❌",
                    )
                    raise result

                if isinstance(result, tuple):
                    task_fetched, task_inserted, task_modified = result
                    fetched += task_fetched
                    inserted += task_inserted
                    modified += task_modified
                else:
                    await self.log.awarning(
                        "处理任务返回了意外的结果类型",
                        result_type=type(result),
                        emoji="⚠️",
                    )
                    raise RuntimeError("处理任务返回了意外的结果类型，无法继续处理。")

            await self.log.ainfo(
                "处理范围内的因子数据完成",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                fetched=fetched,
                inserted=inserted,
                modified=modified,
                emoji="✅",
            )

            return fetched, inserted, modified
        except asyncio.CancelledError:
            await self.log.awarning(
                "任务被取消，中止处理范围内的因子数据",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.aerror(
                "处理范围内的因子数据时发生错误",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise
        finally:
            await self.log.ainfo(
                "退出 _process_alphas_in_range 函数",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                emoji="✅",
            )

    def _create_processing_tasks(
        self,
        alpha_dal: AlphaDAL,
        client: WorldQuantClient,
        cur_time: datetime,
        truncated_end_time: datetime,
        alphas_data_result: SelfAlphaListView,
        parallel: int,
        status: Optional[Status] = None,
        dry_run: bool = False,
    ) -> List[asyncio.Task[Tuple[int, int, int]]]:
        """创建处理任务"""
        try:
            tasks: List[asyncio.Task[Tuple[int, int, int]]] = []
            page_size = 100
            total_pages = (alphas_data_result.count + page_size - 1) // page_size
            pages_per_task = (total_pages + parallel - 1) // parallel

            for i in range(parallel):
                start_page = i * pages_per_task + 1
                end_page = min((i + 1) * pages_per_task, total_pages)
                if start_page > end_page:
                    break

                task: asyncio.Task[Tuple[int, int, int]] = asyncio.create_task(
                    self.process_alphas_pages(
                        alpha_dal=alpha_dal,
                        client=client,
                        start_time=cur_time,
                        end_time=truncated_end_time,
                        status=status,
                        start_page=start_page,
                        end_page=end_page,
                        page_size=page_size,
                        dry_run=dry_run,
                    )
                )
                tasks.append(task)
            return tasks
        except Exception as e:
            self.log.error(
                "创建处理任务时发生错误",
                cur_time=cur_time,
                truncated_end_time=truncated_end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise

    async def _write_alphas_to_db(self, uncommitted_alphas: List[Alpha]) -> None:
        """将因子数据写入数据库"""
        try:
            async with self._db_lock:
                async with (
                    session_manager.get_session(Database.ALPHAS) as session,
                    session.begin(),
                ):
                    alpha_dal = AlphaDAL(session)
                    await alpha_dal.bulk_upsert_by_unique_key(
                        session=session,
                        entities=uncommitted_alphas,
                        unique_key="alpha_id",
                    )
                    await self.log.ainfo(
                        "因子数据写入完成",
                        count=len(uncommitted_alphas),
                        emoji="✅",
                    )
        except asyncio.CancelledError:
            await self.log.awarning(
                "任务被取消，中止写入因子数据",
                count=len(uncommitted_alphas),
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.aerror(
                "写入因子数据时发生错误",
                count=len(uncommitted_alphas),
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise
        finally:
            await self.log.ainfo(
                "退出 _write_alphas_to_db 函数",
                count=len(uncommitted_alphas),
                emoji="✅",
            )

    async def process_alphas_pages(
        self,
        alpha_dal: AlphaDAL,
        client: WorldQuantClient,
        start_time: datetime,
        end_time: datetime,
        status: Optional[Status],
        start_page: int,
        end_page: int,
        page_size: int,
        dry_run: bool = False,
    ) -> Tuple[int, int, int]:

        fetched: int = 0
        inserted: int = 0
        modified: int = 0

        try:
            for page in range(start_page, end_page + 1):
                if self.exit_event.is_set():
                    await self.log.awarning(
                        "检测到退出事件，中止处理多页数据", emoji="⚠️"
                    )
                    break

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

                await self.log.ainfo(
                    "获取多页数据",
                    start_time=start_time,
                    end_time=end_time,
                    page=page,
                    count=len(alphas_data_result.results),
                    emoji="🔍",
                )
                page_fetched, page_inserted, page_modified = (
                    await self.process_alphas_page(
                        alpha_dal=alpha_dal,
                        alphas_results=alphas_data_result.results,
                        dry_run=dry_run,
                    )
                )
                fetched += page_fetched
                inserted += page_inserted
                modified += page_modified
        except asyncio.CancelledError:
            await self.log.awarning(
                "任务被取消，中止处理多页数据",
                start_time=start_time,
                end_time=end_time,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.aerror(
                "处理多页数据时发生错误",
                start_time=start_time,
                end_time=end_time,
                error=str(e),
                exc_info=True,
                emoji="❌",
            )
            raise RuntimeError(f"处理多页数据时发生错误: {e}") from e
        finally:
            await self.log.ainfo(
                "退出 process_alphas_pages 函数",
                start_time=start_time,
                end_time=end_time,
                emoji="✅",
            )

        return fetched, inserted, modified

    async def sync_alphas(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        status: Optional[Status] = None,
        increamental: bool = False,
        parallel: int = 1,
        dry_run: bool = False,
        max_count_per_loop: int = MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    ) -> None:

        if increamental:
            async with wq_client:
                sync_start_time, sync_end_time = await self.fetch_last_sync_time_range(
                    wq_client
                )

                start_time = (
                    max(sync_start_time, start_time) if start_time else sync_start_time
                )
                end_time = min(sync_end_time, end_time) if end_time else sync_end_time
        else:
            # 没有传入时间范围，则使用默认值
            # 时间范围实际上是必传参数，因为列表查询接口对过滤条件有限制
            # 最多只能过滤出 10000 条数据
            if start_time is None:
                start_time = await self.fetch_first_create_time()
            if end_time is None:
                end_time = await self.fetch_last_create_time()

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

        begin_time: datetime = datetime.now()
        fetched: int = 0
        inserted: int = 0
        modified: int = 0
        async with wq_client:
            try:
                alpha_dal: AlphaDAL = DALFactory.create_dal(AlphaDAL)
                days: int = (end_time.date() - start_time.date()).days
                for i in range(days + 1):
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

                    range_fetched, range_inserted, range_modified = (
                        await self.process_alphas_for_time_range(
                            alpha_dal=alpha_dal,
                            client=wq_client,
                            start_time=cur_start_time,
                            end_time=cur_end_time,
                            status=status,
                            parallel=parallel,
                            dry_run=dry_run,
                            max_count_per_loop=max_count_per_loop,
                        )
                    )

                    fetched += range_fetched
                    inserted += range_inserted
                    modified += range_modified

                    await self.log.ainfo(
                        "处理时间范围完成",
                        start_time=cur_start_time,
                        end_time=cur_end_time,
                        fetched=fetched,
                        inserted=inserted,
                        modified=modified,
                        module=__name__,
                        emoji="✅",
                    )

                elapsed_time: timedelta = datetime.now() - begin_time
                await self.log.ainfo(
                    "所有因子同步完成",
                    fetched=fetched,
                    inserted=inserted,
                    modified=modified,
                    elapsed_time=elapsed_time,
                    tps=f"{fetched / elapsed_time.total_seconds():.2f}",
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
