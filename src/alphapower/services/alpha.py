from datetime import datetime, tzinfo
from typing import Any, AsyncIterable, Awaitable, Dict, List, Optional, Tuple

from aiostream import stream

from alphapower.constants import (
    MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    MAX_PAGE_SIZE_IN_ALPHA_LIST_QUERY,
    LoggingEmoji,
    Status,
)
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.manager.alpha_manager import AlphaManagerFactory
from alphapower.manager.alpha_manager_abc import AbstractAlphaManager
from alphapower.view.alpha import AggregateDataView, AlphaView

from .alpha_abc import AbstractAlphaService


class AlphaService(AbstractAlphaService, BaseProcessSafeClass):
    """
    AlphaService class that implements the AbstractAlphaService interface.
    This class is responsible for managing alpha services.
    """

    def __init__(self, alpha_manager: AbstractAlphaManager) -> None:
        """
        Initialize the AlphaService with an AlphaManager instance.
        """
        self.alpha_manager: AbstractAlphaManager = alpha_manager

    async def sync_alphas(
        self,
        tz: tzinfo,
        competition: Optional[str] = None,
        date_created_gt: Optional[datetime] = None,
        date_created_lt: Optional[datetime] = None,
        hidden: Optional[bool] = None,
        name: Optional[str] = None,
        status_eq: Optional[Status] = None,
        status_ne: Optional[Status] = None,
        cocurrency: int = 1,
        aggregate_data_only: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        同步平台上的 alphas 数据。
        如果单次查询的 alphas_count 超过 MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY，
        则对时间范围进行二分查找，找到合适的区间进行同步。
        """
        await self.log.ainfo(
            event=f"进入 {self.sync_alphas.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.adebug(
            event=f"{self.sync_alphas.__qualname__} 入参",
            method=self.sync_alphas.__qualname__,
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            tz=tz,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            cocurrency=cocurrency,
            emoji=LoggingEmoji.DEBUG.value,
        )

        await self.log.adebug(
            event="初始化时间范围",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            emoji=LoggingEmoji.DATETIME.value,
        )

        if not date_created_gt:
            first_alpha: Optional[AlphaView] = (
                await self.alpha_manager.fetch_first_alpha_from_platform()
            )
            date_created_gt = first_alpha.date_created if first_alpha else datetime.min

        date_created_gt = date_created_gt.replace(tzinfo=tz)

        if not date_created_lt:
            last_alpha: Optional[AlphaView] = (
                await self.alpha_manager.fetch_last_alpha_from_platform()
            )
            date_created_lt = last_alpha.date_created if last_alpha else datetime.max

        date_created_lt = date_created_lt.replace(tzinfo=tz)

        if date_created_gt >= date_created_lt:
            await self.log.aerror(
                event="时间范围错误",
                message="开始时间必须小于结束时间",
                date_created_gt=date_created_gt,
                date_created_lt=date_created_lt,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("开始时间必须小于结束时间")

        await self.log.ainfo(
            event="开始同步",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            emoji=LoggingEmoji.SYNC.value,
        )

        # 使用非递归方式进行时间范围的二分查找
        stack = [(date_created_gt, date_created_lt)]
        while stack:
            try:
                current_gt, current_lt = stack.pop()

                alphas_count: int = (
                    await self.alpha_manager.fetch_alphas_total_count_from_platform(
                        competition=competition,
                        date_created_gt=current_gt,
                        date_created_lt=current_lt,
                        hidden=hidden,
                        name=name,
                        status_eq=status_eq,
                        status_ne=status_ne,
                        **kwargs,
                    )
                )

                await self.log.ainfo(
                    event="筛选条件查询数量",
                    alphas_count=alphas_count,
                    date_created_gt=current_gt,
                    date_created_lt=current_lt,
                    competition=competition,
                    hidden=hidden,
                    name=name,
                    status_eq=status_eq,
                    status_ne=status_ne,
                    emoji=LoggingEmoji.INFO.value,
                )

            except Exception as e:
                await self.log.aerror(
                    event="查询失败",
                    message="查询时间范围时发生错误",
                    date_created_gt=current_gt,
                    date_created_lt=current_lt,
                    competition=competition,
                    hidden=hidden,
                    name=name,
                    status_eq=status_eq,
                    status_ne=status_ne,
                    error=str(e),
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise

            if alphas_count == 0:
                await self.log.adebug(
                    event="无数据",
                    message="时间范围内无 alphas 数据",
                    date_created_gt=current_gt,
                    date_created_lt=current_lt,
                    competition=competition,
                    hidden=hidden,
                    name=name,
                    emoji=LoggingEmoji.INFO.value,
                )
                continue

            if alphas_count >= MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY:
                if date_created_gt == datetime.min or date_created_lt == datetime.max:
                    # 如果时间范围已经是最小或最大，则无法进一步二分
                    await self.log.aerror(
                        event="时间范围过大",
                        message="时间范围超过限制，无法进一步二分",
                        date_created_gt=current_gt,
                        date_created_lt=current_lt,
                        competition=competition,
                        hidden=hidden,
                        name=name,
                        status_eq=status_eq,
                        status_ne=status_ne,
                        alphas_count=alphas_count,
                        max_count=MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
                        emoji=LoggingEmoji.ERROR.value,
                    )
                    raise ValueError(
                        f"时间范围 {date_created_gt} - {date_created_lt} 超过限制，"
                        f"alphas_count={alphas_count}，无法进一步二分。"
                    )

                mid_time = current_gt + (current_lt - current_gt) / 2
                if mid_time == current_gt or mid_time == current_lt:
                    raise ValueError(
                        f"无法进一步二分时间范围：{current_gt} - {current_lt}，"
                        f"alphas_count={alphas_count} 超过限制。"
                    )
                stack.append((current_gt, mid_time))
                stack.append((mid_time, current_lt))
            else:
                # 在允许的范围内进行同步
                page_size: int = MAX_PAGE_SIZE_IN_ALPHA_LIST_QUERY
                page_count: int = (alphas_count + page_size - 1) // page_size

                @async_exception_handler
                async def fetch_page(page: int, *args: Any) -> List[AlphaView]:
                    return await self.alpha_manager.fetch_alphas_from_platform(
                        competition=competition,
                        date_created_gt=current_gt,
                        date_created_lt=current_lt,
                        hidden=hidden,
                        name=name,
                        status_eq=status_eq,
                        status_ne=status_ne,
                        limit=page_size,
                        offset=page * page_size,
                        order="dateCreated",
                        **kwargs,
                    )

                try:
                    # 使用 aiostream 按并发度请求数据
                    page_param_stream: AsyncIterable[int] = stream.iterate(
                        range(page_count)
                    )
                    pages_stream: Awaitable = stream.map(
                        page_param_stream, fetch_page, task_limit=cocurrency
                    )
                    alphas_view: List[AlphaView] = []
                    async with pages_stream.stream() as page_stream:  # type: ignore
                        async for page_result in page_stream:
                            alphas_view.extend(page_result)

                    if aggregate_data_only:
                        alpha_ids: List[str] = []
                        in_sample_view_map: Dict[str, Optional[AggregateDataView]] = {}
                        out_sample_view_map: Dict[str, Optional[AggregateDataView]] = {}
                        train_view_map: Dict[str, Optional[AggregateDataView]] = {}
                        test_view_map: Dict[str, Optional[AggregateDataView]] = {}
                        prod_view_map: Dict[str, Optional[AggregateDataView]] = {}

                        for alpha in alphas_view:
                            alpha_ids.append(alpha.id)
                            in_sample_view_map[alpha.id] = alpha.in_sample
                            out_sample_view_map[alpha.id] = alpha.out_sample
                            train_view_map[alpha.id] = alpha.train
                            test_view_map[alpha.id] = alpha.test
                            prod_view_map[alpha.id] = alpha.prod

                        await self.log.ainfo(
                            event="同步样本聚合数据字段",
                            alpha_ids=alpha_ids,
                            emoji=LoggingEmoji.INFO.value,
                        )

                        await self.alpha_manager.bulk_save_aggregate_data_to_db(
                            alpha_ids=alpha_ids,
                            in_sample_view_map=in_sample_view_map,
                            out_sample_view_map=out_sample_view_map,
                            train_view_map=train_view_map,
                            test_view_map=test_view_map,
                            prod_view_map=prod_view_map,
                        )
                    else:
                        batch_size = 1000
                        for i in range(0, len(alphas_view), batch_size):
                            batch = alphas_view[i:i + batch_size]
                            await self.alpha_manager.bulk_save_alpha_to_db(
                                alphas_view=batch
                            )
                            await self.log.ainfo(
                                event="批量保存 alphas 数据",
                                batch_size=len(batch),
                                batch_index=i,
                                emoji=LoggingEmoji.SAVE.value,
                            )
                except Exception as e:
                    await self.log.aerror(
                        event="同步失败",
                        date_created_gt=current_gt,
                        date_created_lt=current_lt,
                        competition=competition,
                        hidden=hidden,
                        name=name,
                        status_eq=status_eq,
                        status_ne=status_ne,
                        error=str(e),
                        emoji=LoggingEmoji.ERROR.value,
                    )
                    raise

                await self.log.ainfo(
                    event="同步完成",
                    date_created_gt=current_gt,
                    date_created_lt=current_lt,
                    alphas_count=alphas_count,
                    synced_count=len(alphas_view),
                    emoji=LoggingEmoji.FINISHED.value,
                )

        await self.log.ainfo(
            event=f"退出 {self.sync_alphas.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def sync_alphas_in_ranges(
        self,
        tz: tzinfo,
        competition: Optional[str] = None,
        created_time_ranges: List[Tuple[datetime, datetime]] = [],
        hidden: Optional[bool] = None,
        name: Optional[str] = None,
        status_eq: Optional[Status] = None,
        status_ne: Optional[Status] = None,
        aggregate_data_only: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        同步多个时间范围内的 alphas 数据。
        """
        await self.log.ainfo(
            event=f"进入 {self.sync_alphas_in_ranges.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self.sync_alphas_in_ranges.__qualname__} 入参",
            method=self.sync_alphas_in_ranges.__qualname__,
            competition=competition,
            created_time_ranges=created_time_ranges,
            tz=tz,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            aggregate_data_only=aggregate_data_only,
            emoji=LoggingEmoji.DEBUG.value,
        )

        # Implementation of the synchronization logic goes here.
        for time_range in created_time_ranges:
            await self.sync_alphas(
                competition=competition,
                date_created_gt=time_range[0],
                date_created_lt=time_range[1],
                tz=tz,
                hidden=hidden,
                name=name,
                status_eq=status_eq,
                status_ne=status_ne,
                aggregate_data_only=aggregate_data_only,
                **kwargs,
            )

        await self.log.ainfo(
            event="多个时间范围同步完成",
            created_time_ranges=created_time_ranges,
            competition=competition,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            emoji=LoggingEmoji.FINISHED.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.sync_alphas_in_ranges.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )


class AlphaServiceFactory(BaseProcessSafeFactory[AbstractAlphaService]):
    """
    Factory class for creating AlphaService instances.
    """

    def __init__(
        self, alpha_manager_factory: AlphaManagerFactory, **kwargs: Any
    ) -> None:
        """
        Initialize the factory with an AlphaManager instance.
        """
        super().__init__(**kwargs)
        self._alpha_manager: Optional[AbstractAlphaManager] = None
        self._alpha_manager_factory: AlphaManagerFactory = alpha_manager_factory

    def __getstate__(self) -> Dict[str, Any]:
        state: Dict[str, Any] = super().__getstate__()
        state.pop("_alpha_manager", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._alpha_manager = None

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        await self.log.ainfo(
            event=f"进入 {self._dependency_factories.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        factories: Dict[str, BaseProcessSafeFactory[Any]] = {
            "_alpha_manager": self._alpha_manager_factory
        }
        await self.log.ainfo(
            event=f"退出 {self._dependency_factories.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return factories

    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaService:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        if self._alpha_manager is None:
            await self.log.aerror(
                event=f"{AbstractAlphaManager.__name__} 未初始化",
                message=(
                    f"{AbstractAlphaManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractAlphaService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractAlphaManager.__name__} 未初始化")

        service: AbstractAlphaService = AlphaService(alpha_manager=self._alpha_manager)

        await self.log.ainfo(
            event=f"{AbstractAlphaService.__name__} 实例创建成功",
            message=(
                f"成功创建 {AbstractAlphaService.__name__} 实例，"
                f"使用的 {AbstractAlphaManager.__name__} 工厂为 {self._alpha_manager_factory.__class__.__name__}"
            ),
            emoji=LoggingEmoji.SUCCESS.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return service
