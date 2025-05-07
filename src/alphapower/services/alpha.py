from datetime import datetime, tzinfo
from typing import Any, AsyncIterable, Awaitable, List, Optional, Tuple

from aiostream import stream

from alphapower.constants import (
    MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    MAX_PAGE_SIZE_IN_ALPHA_LIST_QUERY,
    Status,
)
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.manager.alpha_manager import AlphaManagerFactory
from alphapower.manager.alpha_manager_abc import AbstractAlphaManager
from alphapower.view.alpha import AlphaView

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
        competition: Optional[str],
        date_created_gt: Optional[datetime],
        date_created_lt: Optional[datetime],
        tz: tzinfo,
        hidden: Optional[bool],
        name: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        concurrency: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        同步平台上的 alphas 数据。
        如果单次查询的 alphas_count 超过 MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY，
        则对时间范围进行二分查找，找到合适的区间进行同步。
        """
        await self.log.adebug(
            event="进入方法",
            message=f"进入 {self.sync_alphas.__qualname__} 方法",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            tz=tz,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            concurrency=concurrency,
            emoji="🔍",
        )

        await self.log.adebug(
            event="初始化时间范围",
            message=(
                f"初始化时间范围，competition={competition}, "
                f"date_created_gt={date_created_gt}, date_created_lt={date_created_lt}"
            ),
            emoji="⏰",
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
                message=(
                    f"开始时间 {date_created_gt} 大于等于结束时间 {date_created_lt}，"
                    f"无法继续执行。"
                ),
                date_created_gt=date_created_gt,
                date_created_lt=date_created_lt,
                emoji="❌",
            )
            raise ValueError("开始时间必须小于结束时间")

        await self.log.ainfo(
            event="开始同步",
            message=(
                f"开始同步 alphas 数据，competition={competition}, "
                f"date_created_gt={date_created_gt}, date_created_lt={date_created_lt}"
            ),
            emoji="🔄",
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
            except Exception as e:
                await self.log.aerror(
                    event="查询失败",
                    message=(
                        f"查询时间范围 {current_gt} - {current_lt} 时发生错误：{e}\n"
                        f"competition={competition}, hidden={hidden}, name={name}, "
                        f"status_eq={status_eq}, status_ne={status_ne}"
                    ),
                    date_created_gt=current_gt,
                    date_created_lt=current_lt,
                    emoji="❌",
                )
                raise

            if alphas_count == 0:
                await self.log.adebug(
                    event="无数据",
                    message=(
                        f"时间范围 {current_gt} - {current_lt} 内无 alphas 数据，"
                        f"competition={competition}, hidden={hidden}, name={name}"
                    ),
                    emoji="ℹ️",
                )
                continue

            if alphas_count >= MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY:
                if date_created_gt == datetime.min or date_created_lt == datetime.max:
                    # 如果时间范围已经是最小或最大，则无法进一步二分
                    await self.log.aerror(
                        "时间范围过大",
                        message=f"时间范围 {current_gt} - {current_lt} 超过限制，"
                        f"且其他参数筛选结果数量为 {alphas_count}，超过限制数量 {MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY}，"
                        f"无法进行时间范围二分减小筛选范围。",
                        competition=competition,
                        date_created_gt=date_created_gt,
                        date_created_lt=date_created_lt,
                        hidden=hidden,
                        name=name,
                        status_eq=status_eq,
                        status_ne=status_ne,
                        emoji="❌",
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
                        page_param_stream, fetch_page, task_limit=concurrency
                    )
                    alphas_view: List[AlphaView] = [
                        alpha for page in await pages_stream for alpha in page
                    ]
                    await self.alpha_manager.bulk_save_alpha_to_db(alphas_view=alphas_view)
                except Exception as e:
                    await self.log.aerror(
                        event="同步失败",
                        message=(
                            f"时间范围 {current_gt} - {current_lt} 同步时发生错误：{e}\n"
                            f"competition={competition}, hidden={hidden}, name={name}, "
                            f"status_eq={status_eq}, status_ne={status_ne}"
                        ),
                        date_created_gt=current_gt,
                        date_created_lt=current_lt,
                        emoji="❌",
                    )
                    raise

                await self.log.ainfo(
                    event="同步完成",
                    current_gt=current_gt,
                    current_lt=current_lt,
                    alphas_count=alphas_count,
                    alphas_view_count=len(alphas_view),
                    emoji="✅",
                )

        await self.log.ainfo(
            event="方法执行完成",
            qualname=self.sync_alphas.__qualname__,
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            alphas_count=alphas_count,
            emoji="✅",
        )

    @async_exception_handler
    async def sync_alphas_in_ranges(
        self,
        competition: Optional[str],
        created_time_ranges: List[Tuple[datetime, datetime]],
        tz: tzinfo,
        hidden: Optional[bool],
        name: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        **kwargs: Any,
    ) -> None:
        """
        同步多个时间范围内的 alphas 数据。
        """
        await self.log.ainfo(
            event="开始同步多个时间范围",
            message="开始同步多个时间范围内的 alphas 数据",
            qualname=self.sync_alphas_in_ranges.__qualname__,
            competition=competition,
            created_time_ranges=created_time_ranges,
            emoji="🔄",
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
                **kwargs,
            )

        await self.log.ainfo(
            event="方法执行完成",
            message=(
                f"{self.sync_alphas_in_ranges.__qualname__} 方法执行完成，"
                f"competition={competition}, 同步的时间范围数量={len(created_time_ranges)}"
            ),
            emoji="✅",
        )


class AlphaServiceFactory(BaseProcessSafeFactory[AbstractAlphaService]):
    """
    Factory class for creating AlphaService instances.
    """

    def __init__(self, alpha_manager_factory: AlphaManagerFactory) -> None:
        """
        Initialize the factory with an AlphaManager instance.
        """
        self.alpha_manager: Optional[AbstractAlphaManager] = None
        self.alpha_manager_factory: AlphaManagerFactory = alpha_manager_factory

    async def _dependency_factories(self) -> dict[str, BaseProcessSafeFactory]:
        return {"alpha_manager": self.alpha_manager_factory}

    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaService:
        if self.alpha_manager is None:
            await self.log.aerror(
                f"{AbstractAlphaManager.__name__} 未初始化",
                message=f"{AbstractAlphaManager.__name__} 依赖未注入，无法创建 {AbstractAlphaService.__name__} 实例",
                emoji="❌",
            )
            raise ValueError(f"{AbstractAlphaManager.__name__} 未初始化")

        service: AbstractAlphaService = AlphaService(alpha_manager=self.alpha_manager)

        return service
