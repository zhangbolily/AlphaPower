from datetime import datetime
from typing import Any, Dict, List, Optional

from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import Color, Database, Grade, Status
from alphapower.dal import alpha_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import AggregateData, Alpha
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.view.alpha import (
    AggregateDataView,
    AlphaView,
    UserAlphasQuery,
    UserAlphasView,
)

from .alpha_manager_abc import AbstractAlphaManager


class AlphaManager(BaseProcessSafeClass, AbstractAlphaManager):
    def __init__(self, brain_client: AbstractWorldQuantBrainClient) -> None:
        self._brain_client: Optional[AbstractWorldQuantBrainClient] = brain_client

    async def brain_client(self) -> AbstractWorldQuantBrainClient:
        await self.log.ainfo("进入 brain_client 方法", emoji="➡️")
        if self._brain_client is None:
            await self.log.aerror("WorldQuant Brain client 未设置", emoji="❌")
            raise ValueError("WorldQuant Brain client is not set.")
        await self.log.ainfo("退出 brain_client 方法", emoji="⬅️")
        return self._brain_client

    @async_exception_handler
    async def fetch_alphas_total_count_from_platform(
        self,
        competition: Optional[str],
        date_created_gt: Optional[datetime],
        date_created_lt: Optional[datetime],
        hidden: Optional[bool],
        name: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        **kwargs: Any,
    ) -> int:
        await self.log.ainfo(
            "进入 fetch_alphas_total_count_from_platform 方法", emoji="➡️"
        )
        await self.log.adebug(
            "fetch_alphas_total_count_from_platform 入参",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            kwargs=kwargs,
            emoji="🐛",
        )
        query: UserAlphasQuery = UserAlphasQuery(
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            **kwargs,
        )

        # 只是为了获取 count 字段，不需要实际的 alpha 数据
        # 这几个字段的值可以写死
        query.limit = 1
        query.offset = 0
        query.order = None

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        total: int = alphas_view.count

        await self.log.adebug(
            "fetch_alphas_total_count_from_platform 出参", total=total, emoji="🐛"
        )
        await self.log.ainfo(
            "退出 fetch_alphas_total_count_from_platform 方法", emoji="⬅️"
        )
        return total

    @async_exception_handler
    async def fetch_alphas_from_platform(
        self,
        competition: Optional[str],
        date_created_gt: Optional[datetime],
        date_created_lt: Optional[datetime],
        hidden: Optional[bool],
        limit: Optional[int],
        name: Optional[str],
        offset: Optional[int],
        order: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        **kwargs: Any,
    ) -> List[AlphaView]:
        await self.log.ainfo("进入 fetch_alphas_from_platform 方法", emoji="➡️")
        await self.log.adebug(
            "fetch_alphas_from_platform 入参",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            hidden=hidden,
            limit=limit,
            name=name,
            offset=offset,
            order=order,
            status_eq=status_eq,
            status_ne=status_ne,
            kwargs=kwargs,
            emoji="🐛",
        )
        query: UserAlphasQuery = UserAlphasQuery(
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            hidden=hidden,
            limit=limit,
            name=name,
            offset=offset,
            order=order,
            status_eq=status_eq,
            status_ne=status_ne,
            **kwargs,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        alphas: List[AlphaView] = alphas_view.results

        await self.log.adebug(
            "fetch_alphas_from_platform 出参",
            alpha_ids=[alpha.id for alpha in alphas],
            emoji="🐛",
        )
        await self.log.ainfo("退出 fetch_alphas_from_platform 方法", emoji="⬅️")
        return alphas

    @async_exception_handler
    async def fetch_first_alpha_from_platform(self) -> Optional[AlphaView]:
        await self.log.ainfo("进入 fetch_first_alpha_from_platform 方法", emoji="➡️")
        query: UserAlphasQuery = UserAlphasQuery(
            limit=1,
            offset=0,
            order="dataCreated",
        )
        await self.log.adebug("构建查询参数", query=query, emoji="🛠️")
        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        if alphas_view.count == 0:
            await self.log.awarning("未找到任何 Alpha 数据", emoji="⚠️")
            return None
        await self.log.adebug(
            "fetch_first_alpha_from_platform 出参",
            alpha_id=alphas_view.results[0].id,
            emoji="🐛",
        )
        await self.log.ainfo("退出 fetch_first_alpha_from_platform 方法", emoji="⬅️")
        return alphas_view.results[0]

    @async_exception_handler
    async def fetch_last_alpha_from_platform(self) -> Optional[AlphaView]:
        await self.log.ainfo("进入 fetch_last_alpha_from_platform 方法", emoji="➡️")
        query: UserAlphasQuery = UserAlphasQuery(
            limit=1,
            offset=0,
            order="-dataCreated",
        )
        await self.log.adebug("构建查询参数", query=query, emoji="🛠️")
        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        if alphas_view.count == 0:
            await self.log.awarning("未找到任何 Alpha 数据", emoji="⚠️")
            return None
        await self.log.adebug(
            "fetch_last_alpha_from_platform 出参",
            alpha_id=alphas_view.results[0].id,
            emoji="🐛",
        )
        await self.log.ainfo("退出 fetch_last_alpha_from_platform 方法", emoji="⬅️")
        return alphas_view.results[0]

    @async_exception_handler
    async def fetch_first_alpha_from_db(self) -> Optional[Alpha]:
        await self.log.ainfo("进入 fetch_first_alpha_from_db 方法", emoji="➡️")
        async with session_manager.get_session(
            Database.ALPHAS, readonly=True
        ) as session:
            alphas: List[Alpha] = await alpha_dal.find_by(
                Alpha.date_created.asc(),
                limit=1,
                session=session,
            )
        if len(alphas) == 0:
            await self.log.awarning("数据库中未找到任何 Alpha 数据", emoji="⚠️")
            return None
        await self.log.adebug(
            "fetch_first_alpha_from_db 出参",
            alpha_id=alphas[0].alpha_id,
            emoji="🐛",
        )
        await self.log.ainfo("退出 fetch_first_alpha_from_db 方法", emoji="⬅️")
        return alphas[0]

    @async_exception_handler
    async def fetch_last_alpha_from_db(self) -> Optional[Alpha]:
        await self.log.ainfo("进入 fetch_last_alpha_from_db 方法", emoji="➡️")
        async with session_manager.get_session(
            Database.ALPHAS, readonly=True
        ) as session:
            alphas: List[Alpha] = await alpha_dal.find_by(
                Alpha.date_created.desc(),
                limit=1,
                session=session,
            )
        if len(alphas) == 0:
            await self.log.awarning("数据库中未找到任何 Alpha 数据", emoji="⚠️")
            return None
        await self.log.adebug(
            "fetch_last_alpha_from_db 出参",
            alpha_id=alphas[0].alpha_id,
            emoji="🐛",
        )
        await self.log.ainfo("退出 fetch_last_alpha_from_db 方法", emoji="⬅️")
        return alphas[0]

    @async_exception_handler
    async def save_alphas_to_db(
        self,
        alphas_view: List[AlphaView],
    ) -> None:
        await self.log.ainfo("进入 save_alphas_to_db 方法", emoji="➡️")
        await self.log.adebug(
            "save_alphas_to_db 入参",
            alpha_ids=[alpha.id for alpha in alphas_view],
            emoji="🐛",
        )
        alphas: List[Alpha] = []
        for alpha_view in alphas_view:
            alpha: Alpha = await self.build_alpha_entity_from_view(
                alpha_view=alpha_view
            )
            alphas.append(alpha)
        await self.log.adebug(
            "save_alphas_to_db 转换为实体对象",
            alpha_ids=[alpha.alpha_id for alpha in alphas],
            emoji="🐛",
        )
        async with (
            session_manager.get_session(Database.ALPHAS) as session,
            session.begin(),
        ):
            await alpha_dal.bulk_create(
                session=session,
                entities=alphas,
            )
            await session.commit()
        await self.log.ainfo("退出 save_alphas_to_db 方法", emoji="⬅️")

    @async_exception_handler
    async def build_alpha_entity_from_view(
        self,
        alpha_view: AlphaView,
    ) -> Alpha:

        @async_exception_handler
        async def build_aggregate_data_entity_from_view(
            sample_data: Optional[AggregateDataView],
        ) -> Optional[AggregateData]:
            """
            创建样本数据。

            参数:
            sample_data: 样本数据对象。

            返回:
            样本实体对象，或 None 如果样本数据为空。
            """
            if sample_data is None:
                return None

            aggregate_data: AggregateData = AggregateData(
                pnl=sample_data.pnl,
                book_size=sample_data.book_size,
                long_count=sample_data.long_count,
                short_count=sample_data.short_count,
                turnover=sample_data.turnover,
                returns=sample_data.returns,
                drawdown=sample_data.drawdown,
                margin=sample_data.margin,
                sharpe=sample_data.sharpe,
                fitness=sample_data.fitness,
                self_correration=sample_data.self_correlation,
                prod_correration=sample_data.prod_correlation,
                os_is_sharpe_ratio=sample_data.os_is_sharpe_ratio,
                pre_close_sharpe_ratio=sample_data.pre_close_sharpe_ratio,
                start_date=sample_data.start_date,
                checks=sample_data.checks,
            )

            return aggregate_data

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
            in_sample=await build_aggregate_data_entity_from_view(alpha_view.in_sample),
            out_sample=await build_aggregate_data_entity_from_view(
                alpha_view.out_sample
            ),
            train=await build_aggregate_data_entity_from_view(alpha_view.train),
            test=await build_aggregate_data_entity_from_view(alpha_view.test),
            prod=await build_aggregate_data_entity_from_view(alpha_view.prod),
            pyramids=alpha_view.pyramids,
            competitions=alpha_view.competitions,
            classifications=alpha_view.classifications,
            themes=alpha_view.themes,
            team=alpha_view.team,
        )
        return alpha


class AlphaManagerFactory(BaseProcessSafeFactory[AbstractAlphaManager]):
    def __init__(
        self,
        brain_client_factory: BaseProcessSafeFactory,
        **kwargs: Any,
    ) -> None:
        """
        初始化工厂类。
        """
        super().__init__(**kwargs)
        self.brain_client: Optional[AbstractWorldQuantBrainClient] = None
        self.brain_client_factory: BaseProcessSafeFactory = brain_client_factory

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        """
        返回依赖的工厂列表。
        """
        factories = {"brain_client": self.brain_client_factory}
        await self.log.adebug(
            f"{self._dependency_factories.__qualname__} 出参",
            factories=list(factories.keys()),
            emoji="🔗",
        )
        return factories

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaManager:
        await self.log.ainfo(
            f"进入 {self._build.__qualname__} 方法",
            emoji="➡️",
        )
        await self.log.adebug(
            f"{self._build.__qualname__} 入参",
            args=args,
            kwargs=kwargs,
            emoji="🐛",
        )

        if self.brain_client is None:
            await self.log.aerror("WorldQuant Brain client 未设置", emoji="❌")
            raise ValueError("WorldQuant Brain client is not set.")

        manager: AbstractAlphaManager = AlphaManager(brain_client=self.brain_client)
        await self.log.adebug(
            f"{self._build.__qualname__} 出参",
            manager_type=type(manager).__name__,
            emoji="🐛",
        )
        await self.log.ainfo(
            f"退出 {self._build.__qualname__} 方法",
            emoji="⬅️",
        )
        return manager
