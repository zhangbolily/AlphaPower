from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.client.worldquant_brain_client import WorldQuantBrainClientFactory
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import Color, Database, Grade, LoggingEmoji, Status
from alphapower.dal import aggregate_data_dal, alpha_dal
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
        await self.log.ainfo(
            event=f"获取 {AbstractWorldQuantBrainClient.__name__} 实例",
            message=f"进入 {self.brain_client.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        if self._brain_client is None:
            await self.log.aerror(
                event=f"{AbstractWorldQuantBrainClient.__name__} 实例未设置",
                message=f"{self.brain_client.__qualname__} 方法中发现未设置客户端",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractWorldQuantBrainClient.__name__} 实例未设置")
        await self.log.ainfo(
            event=f"获取 {AbstractWorldQuantBrainClient.__name__} 实例成功",
            message=f"退出 {self.brain_client.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
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
            event="从平台获取 Alpha 总数",
            message=f"进入 {self.fetch_alphas_total_count_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event="方法入参",
            message=f"{self.fetch_alphas_total_count_from_platform.__qualname__} 入参",
            competition=competition,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            hidden=hidden,
            name=name,
            status_eq=status_eq,
            status_ne=status_ne,
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
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
            event="方法出参",
            message=f"{self.fetch_alphas_total_count_from_platform.__qualname__} 出参",
            total=total,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event="从平台获取 Alpha 总数成功",
            message=f"退出 {self.fetch_alphas_total_count_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
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
        await self.log.ainfo(
            event=f"进入 {self.fetch_alphas_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self.fetch_alphas_from_platform.__qualname__} 入参",
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
            emoji=LoggingEmoji.DEBUG.value,
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
            event=f"{self.fetch_alphas_from_platform.__qualname__} 出参",
            alpha_ids=[alpha.id for alpha in alphas],
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.fetch_alphas_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas

    @async_exception_handler
    async def fetch_first_alpha_from_platform(self) -> Optional[AlphaView]:
        await self.log.ainfo(
            event=f"进入 {self.fetch_first_alpha_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        query: UserAlphasQuery = UserAlphasQuery(
            limit=1,
            offset=0,
            order="dateCreated",
        )
        await self.log.adebug(
            event="构建查询参数", query=query, emoji=LoggingEmoji.DEBUG.value
        )
        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        if alphas_view.count == 0:
            await self.log.awarning(
                "未找到任何 Alpha 数据", emoji=LoggingEmoji.WARNING.value
            )
            return None
        await self.log.adebug(
            event=f"{self.fetch_first_alpha_from_platform.__qualname__} 出参",
            alpha_id=alphas_view.results[0].id,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.fetch_first_alpha_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas_view.results[0]

    @async_exception_handler
    async def fetch_last_alpha_from_platform(self) -> Optional[AlphaView]:
        await self.log.ainfo(
            event=f"进入 {self.fetch_last_alpha_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        query: UserAlphasQuery = UserAlphasQuery(
            limit=1,
            offset=0,
            order="-dateCreated",
        )
        await self.log.adebug(
            event="构建查询参数", query=query, emoji=LoggingEmoji.DEBUG.value
        )
        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_view: UserAlphasView = await brain_client.fetch_user_alphas(query=query)
        if alphas_view.count == 0:
            await self.log.awarning(
                "未找到任何 Alpha 数据", emoji=LoggingEmoji.WARNING.value
            )
            return None
        await self.log.adebug(
            event=f"{self.fetch_last_alpha_from_platform.__qualname__} 出参",
            alpha_id=alphas_view.results[0].id,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.fetch_last_alpha_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas_view.results[0]

    @async_exception_handler
    async def fetch_first_alpha_from_db(self) -> Optional[Alpha]:
        await self.log.ainfo(
            event=f"进入 {self.fetch_first_alpha_from_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        async with session_manager.get_session(
            Database.ALPHAS, readonly=True
        ) as session:
            alphas: List[Alpha] = await alpha_dal.find_by(
                limit=1,
                order_by=Alpha.date_created.asc(),
                session=session,
            )
        if len(alphas) == 0:
            await self.log.awarning(
                "数据库中未找到任何 Alpha 数据", emoji=LoggingEmoji.WARNING.value
            )
            return None
        await self.log.adebug(
            event=f"{self.fetch_first_alpha_from_db.__qualname__} 出参",
            alpha_id=alphas[0].alpha_id,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.fetch_first_alpha_from_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas[0]

    @async_exception_handler
    async def fetch_last_alpha_from_db(self) -> Optional[Alpha]:
        await self.log.ainfo(
            event=f"进入 {self.fetch_last_alpha_from_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        async with session_manager.get_session(
            Database.ALPHAS, readonly=True
        ) as session:
            alphas: List[Alpha] = await alpha_dal.find_by(
                limit=1,
                order_by=Alpha.date_created.desc(),
                session=session,
            )
        if len(alphas) == 0:
            await self.log.awarning(
                "数据库中未找到任何 Alpha 数据", emoji=LoggingEmoji.WARNING.value
            )
            return None
        await self.log.adebug(
            "fetch_last_alpha_from_db 出参",
            alpha_id=alphas[0].alpha_id,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.fetch_last_alpha_from_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas[0]

    @async_exception_handler
    async def bulk_save_alpha_to_db(
        self,
        alphas_view: List[AlphaView],
    ) -> None:
        await self.log.ainfo(
            event=f"进入 {self.bulk_save_alpha_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self.bulk_save_alpha_to_db.__qualname__} 入参",
            alpha_ids=[alpha.id for alpha in alphas_view],
            emoji=LoggingEmoji.DEBUG.value,
        )
        alphas: List[Alpha] = []
        for alpha_view in alphas_view:
            alpha: Alpha = await self.build_alpha_entity_from_view(
                alpha_view=alpha_view
            )
            alphas.append(alpha)
        await self.log.adebug(
            event=f"{self.bulk_save_alpha_to_db.__qualname__} 转换为实体对象",
            alpha_ids=[alpha.alpha_id for alpha in alphas],
            emoji=LoggingEmoji.DEBUG.value,
        )
        async with (
            session_manager.get_session(Database.ALPHAS) as session,
            session.begin(),
        ):
            await alpha_dal.bulk_upsert_by_unique_key(
                session=session,
                entities=alphas,
                unique_key="alpha_id",
            )
            await session.commit()
        await self.log.ainfo(
            event=f"退出 {self.bulk_save_alpha_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def build_alpha_entity_from_view(
        self,
        alpha_view: AlphaView,
    ) -> Alpha:

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
            in_sample=(
                await self.build_aggregate_data_from_view(alpha_view.in_sample)
                if alpha_view.in_sample
                else None
            ),
            out_sample=(
                await self.build_aggregate_data_from_view(alpha_view.out_sample)
                if alpha_view.out_sample
                else None
            ),
            train=(
                await self.build_aggregate_data_from_view(alpha_view.train)
                if alpha_view.train
                else None
            ),
            test=(
                await self.build_aggregate_data_from_view(alpha_view.test)
                if alpha_view.test
                else None
            ),
            prod=(
                await self.build_aggregate_data_from_view(alpha_view.prod)
                if alpha_view.prod
                else None
            ),
            pyramids=alpha_view.pyramids,
            competitions=alpha_view.competitions,
            classifications=alpha_view.classifications,
            themes=alpha_view.themes,
            team=alpha_view.team,
        )
        return alpha

    @async_exception_handler
    async def build_aggregate_data_from_view(
        self,
        sample_data: AggregateDataView,
        primary_id: Optional[int] = None,
    ) -> AggregateData:
        """
        创建样本数据。

        参数:
        sample_data: 样本数据对象。

        返回:
        样本实体对象，或 None 如果样本数据为空。
        """
        if sample_data is None:
            await self.log.aerror("样本数据为空", emoji=LoggingEmoji.ERROR.value)
            raise ValueError("样本数据为空")

        aggregate_data: AggregateData = AggregateData(
            id=primary_id,
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

    @async_exception_handler
    async def _save_aggregate_data_to_db_in_session(
        self,
        session: AsyncSession,
        alpha_id: str,
        in_sample_view: Optional[AggregateDataView],
        out_sample_view: Optional[AggregateDataView],
        train_view: Optional[AggregateDataView],
        test_view: Optional[AggregateDataView],
        prod_view: Optional[AggregateDataView],
    ) -> Dict[str, AggregateData]:
        await self.log.ainfo(
            event=f"进入 {self._save_aggregate_data_to_db_in_session.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self._save_aggregate_data_to_db_in_session.__qualname__} 入参",
            alpha_id=alpha_id,
            in_sample=(
                in_sample_view.model_dump(mode="json") if in_sample_view else None
            ),
            out_sample=(
                out_sample_view.model_dump(mode="json") if out_sample_view else None
            ),
            train=train_view.model_dump(mode="json") if train_view else None,
            test=test_view.model_dump(mode="json") if test_view else None,
            prod=prod_view.model_dump(mode="json") if prod_view else None,
            emoji=LoggingEmoji.DEBUG.value,
        )

        in_sample_id: Optional[int] = None
        out_sample_id: Optional[int] = None
        train_id: Optional[int] = None
        test_id: Optional[int] = None
        prod_id: Optional[int] = None

        result: Dict[str, AggregateData] = {}

        aggregate_data_id_map: Dict[str, Optional[int]] = (
            await alpha_dal.get_aggregate_data_ids_by_alpha_id(
                alpha_id=alpha_id, session=session
            )
        )
        await self.log.adebug(
            event="获取 AggregateData ID 映射",
            aggregate_data_id_map=aggregate_data_id_map,
            emoji=LoggingEmoji.DEBUG.value,
        )

        in_sample_id = aggregate_data_id_map.get("in_sample_id", None)
        out_sample_id = aggregate_data_id_map.get("out_sample_id", None)
        train_id = aggregate_data_id_map.get("train_id", None)
        test_id = aggregate_data_id_map.get("test_id", None)
        prod_id = aggregate_data_id_map.get("prod_id", None)

        if in_sample_view:
            in_sample: AggregateData = await self.build_aggregate_data_from_view(
                sample_data=in_sample_view, primary_id=in_sample_id
            )
            in_sample = await aggregate_data_dal.upsert(
                session=session,
                entity=in_sample,
            )
            result["in_sample"] = in_sample
            await self.log.adebug(
                event="更新 in_sample 数据",
                in_sample_id=in_sample.id,
                emoji=LoggingEmoji.DEBUG.value,
            )

        if out_sample_view:
            out_sample: AggregateData = await self.build_aggregate_data_from_view(
                sample_data=out_sample_view, primary_id=out_sample_id
            )
            out_sample = await aggregate_data_dal.upsert(
                session=session,
                entity=out_sample,
            )
            result["out_sample"] = out_sample
            await self.log.adebug(
                event="更新 out_sample 数据",
                out_sample_id=out_sample.id,
                emoji=LoggingEmoji.DEBUG.value,
            )

        if train_view:
            train: AggregateData = await self.build_aggregate_data_from_view(
                sample_data=train_view, primary_id=train_id
            )
            train = await aggregate_data_dal.upsert(
                session=session,
                entity=train,
            )
            result["train"] = train
            await self.log.adebug(
                event="更新 train 数据",
                train_id=train.id,
                emoji=LoggingEmoji.DEBUG.value,
            )

        if test_view:
            test: AggregateData = await self.build_aggregate_data_from_view(
                sample_data=test_view, primary_id=test_id
            )
            test = await aggregate_data_dal.upsert(
                session=session,
                entity=test,
            )
            result["test"] = test
            await self.log.adebug(
                event="更新 test 数据",
                test_id=test.id,
                emoji=LoggingEmoji.DEBUG.value,
            )

        if prod_view:
            prod: AggregateData = await self.build_aggregate_data_from_view(
                sample_data=prod_view, primary_id=prod_id
            )
            prod = await aggregate_data_dal.upsert(
                session=session,
                entity=prod,
            )
            result["prod"] = prod
            await self.log.adebug(
                event="更新 prod 数据",
                prod_id=prod.id,
                emoji=LoggingEmoji.DEBUG.value,
            )

        await self.log.ainfo(
            event=f"保存 {AggregateData.__name__} 数据成功",
            alpha_id=alpha_id,
            in_sample_id=in_sample_id,
            out_sample_id=out_sample_id,
            train_id=train_id,
            test_id=test_id,
            prod_id=prod_id,
            emoji=LoggingEmoji.FINISHED.value,
        )

        await self.log.adebug(
            event=f"{self._save_aggregate_data_to_db_in_session.__qualname__} 出参",
            result={key: value.__dict__ for key, value in result.items()},
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._save_aggregate_data_to_db_in_session.__qualname__} 方法",
            keys=list(result.keys()),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return result

    @async_exception_handler
    async def save_aggregate_data_to_db(
        self,
        alpha_id: str,
        in_sample_view: Optional[AggregateDataView],
        out_sample_view: Optional[AggregateDataView],
        train_view: Optional[AggregateDataView],
        test_view: Optional[AggregateDataView],
        prod_view: Optional[AggregateDataView],
    ) -> Dict[str, AggregateData]:
        await self.log.ainfo(
            event=f"进入 {self.save_aggregate_data_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        result: Dict[str, AggregateData] = {}
        async with (
            session_manager.get_session(Database.ALPHAS) as session,
            session.begin(),
        ):
            result = await self._save_aggregate_data_to_db_in_session(
                session=session,
                alpha_id=alpha_id,
                in_sample_view=in_sample_view,
                out_sample_view=out_sample_view,
                train_view=train_view,
                test_view=test_view,
                prod_view=prod_view,
            )

        await self.log.adebug(
            event=f"{self.save_aggregate_data_to_db.__qualname__} 出参",
            result={key: value.__dict__ for key, value in result.items()},
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.save_aggregate_data_to_db.__qualname__} 方法",
            keys=list(result.keys()),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return result

    @async_exception_handler
    async def bulk_save_aggregate_data_to_db(
        self,
        alpha_ids: List[str],
        in_sample_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        out_sample_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        train_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        test_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        prod_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
    ) -> Dict[str, Dict[str, AggregateData]]:
        await self.log.ainfo(
            event=f"进入 {self.bulk_save_aggregate_data_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self.bulk_save_aggregate_data_to_db.__qualname__} 入参",
            alpha_ids=alpha_ids,
            in_sample_view_map=in_sample_view_map,
            out_sample_view_map=out_sample_view_map,
            train_view_map=train_view_map,
            test_view_map=test_view_map,
            prod_view_map=prod_view_map,
            emoji=LoggingEmoji.DEBUG.value,
        )

        result: Dict[str, Dict[str, AggregateData]] = {}

        async with (
            session_manager.get_session(Database.ALPHAS) as session,
            session.begin(),
        ):
            for alpha_id in alpha_ids:
                aggregate_data_dict: Dict[str, AggregateData] = (
                    await self._save_aggregate_data_to_db_in_session(
                        session=session,
                        alpha_id=alpha_id,
                        in_sample_view=(
                            in_sample_view_map.get(alpha_id, None)
                            if in_sample_view_map
                            else None
                        ),
                        out_sample_view=(
                            out_sample_view_map.get(alpha_id, None)
                            if out_sample_view_map
                            else None
                        ),
                        train_view=(
                            train_view_map.get(alpha_id, None)
                            if train_view_map
                            else None
                        ),
                        test_view=(
                            test_view_map.get(alpha_id, None) if test_view_map else None
                        ),
                        prod_view=(
                            prod_view_map.get(alpha_id, None) if prod_view_map else None
                        ),
                    )
                )
                result[alpha_id] = aggregate_data_dict
                await self.log.adebug(
                    event="批量保存数据",
                    alpha_id=alpha_id,
                    aggregate_data_dict={
                        key: value.__dict__
                        for key, value in aggregate_data_dict.items()
                    },
                    emoji=LoggingEmoji.DEBUG.value,
                )

        await self.log.ainfo(
            event=f"批量保存 {AggregateData.__name__} 完成",
            alpha_ids=alpha_ids,
            in_sample_view_map=(
                list(in_sample_view_map.keys()) if in_sample_view_map else None
            ),
            out_sample_view_map=(
                list(out_sample_view_map.keys()) if out_sample_view_map else None
            ),
            train_view_map=list(train_view_map.keys()) if train_view_map else None,
            test_view_map=list(test_view_map.keys()) if test_view_map else None,
            prod_view_map=list(prod_view_map.keys()) if prod_view_map else None,
            emoji=LoggingEmoji.FINISHED.value,
        )

        await self.log.adebug(
            event=f"{self.bulk_save_aggregate_data_to_db.__qualname__} 出参",
            result={
                key: {k: v.__dict__ for k, v in value.items()}
                for key, value in result.items()
            },
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.bulk_save_aggregate_data_to_db.__qualname__} 方法",
            keys=list(result.keys()),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return result


class AlphaManagerFactory(BaseProcessSafeFactory[AbstractAlphaManager]):
    def __init__(
        self,
        brain_client_factory: WorldQuantBrainClientFactory,
        **kwargs: Any,
    ) -> None:
        """
        初始化工厂类。
        """
        super().__init__(**kwargs)
        self._brain_client: Optional[AbstractWorldQuantBrainClient] = None
        self._brain_client_factory: WorldQuantBrainClientFactory = brain_client_factory

    def __getstate__(self) -> Dict[str, Any]:
        state: Dict[str, Any] = super().__getstate__()
        state.pop("_brain_client", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._brain_client = None

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        """
        返回依赖的工厂列表。
        """
        factories: Dict[str, BaseProcessSafeFactory] = {
            "_brain_client": self._brain_client_factory
        }
        await self.log.adebug(
            event=f"{self._dependency_factories.__qualname__} 出参",
            message="依赖工厂列表生成成功",
            factories=list(factories.keys()),
            emoji=LoggingEmoji.DEBUG.value,
        )
        return factories

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaManager:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__} 方法",
            message="开始构建 AlphaManager 实例",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self._build.__qualname__} 入参",
            message="构建方法参数",
            args=args,
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
        )

        if self._brain_client is None:
            await self.log.aerror(
                event="WorldQuant Brain client 未设置",
                message="无法构建 AlphaManager 实例，缺少必要的客户端依赖",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("WorldQuant Brain client is not set.")

        manager: AbstractAlphaManager = AlphaManager(brain_client=self._brain_client)
        await self.log.adebug(
            event=f"{self._build.__qualname__} 出参",
            message="AlphaManager 实例构建成功",
            manager_type=type(manager).__name__,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__} 方法",
            message="完成 AlphaManager 实例构建",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return manager
