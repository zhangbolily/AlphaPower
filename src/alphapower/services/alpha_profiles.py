import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from structlog.stdlib import BoundLogger

from alphapower.constants import Database, FastExpressionType, LoggingEmoji
from alphapower.dal import alpha_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alpha_profiles import AlphaProfile, AlphaProfileDataFields
from alphapower.entity.alphas import Alpha
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import get_logger
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
    process_async_runner,
)
from alphapower.manager.alpha_manager import AlphaManagerFactory
from alphapower.manager.alpha_manager_abc import AbstractAlphaManager
from alphapower.manager.alpha_profile_manager import AlphaProfileManagerFactory
from alphapower.manager.alpha_profile_manager_abc import AbstractAlphaProfileManager
from alphapower.manager.fast_expression_manager import (
    FastExpression,
    FastExpressionManagerFactory,
)
from alphapower.manager.fast_expression_manager_abc import AbstractFastExpressionManager
from alphapower.view.alpha import AlphaView

from .alpha_profiles_abc import AbstractAlphaProfilesService


@async_exception_handler
async def build_alpha_profiles_process_runner(
    alpha_ids: List[int],
    batch_size: int,
    fast_expression_manager_factory: FastExpressionManagerFactory,
    alpha_profile_manager_factory: AlphaProfileManagerFactory,
) -> None:
    logger: BoundLogger = get_logger(build_alpha_profiles_process_runner.__qualname__)
    await logger.ainfo(
        event=f"进入 {build_alpha_profiles_process_runner.__qualname__}",
        emoji=LoggingEmoji.STEP_IN_FUNC.value,
    )

    fast_expression_manager: AbstractFastExpressionManager = (
        await fast_expression_manager_factory()
    )
    alpha_profile_manager: AbstractAlphaProfileManager = (
        await alpha_profile_manager_factory()
    )

    if not alpha_ids:
        await logger.aerror(
            event="没有提供 alpha_ids",
            message=f"无法构建 {AlphaProfile.__name__}，因为没有提供 alpha_ids",
            emoji=LoggingEmoji.ERROR.value,
        )
        return

    total_batches: int = (len(alpha_ids) + batch_size - 1) // batch_size
    for batch_idx, i in enumerate(range(0, len(alpha_ids), batch_size), start=1):
        batch_alpha_ids: List[int] = alpha_ids[i : i + batch_size]
        await logger.adebug(
            event="处理 alpha_ids 批次",
            batch_index=batch_idx,
            total_batches=total_batches,
            alpha_ids=batch_alpha_ids,
            emoji=LoggingEmoji.DEBUG.value,
        )
        try:
            async with session_manager.get_session(
                db=Database.ALPHAS,
                readonly=True,
            ) as session:
                alphas: List[Alpha] = await alpha_dal.find_by(
                    Alpha.id.in_(batch_alpha_ids),
                    session=session,
                )
        except Exception as e:
            await logger.aerror(
                event="数据库查询异常",
                batch_index=batch_idx,
                error=str(e),
                emoji=LoggingEmoji.ERROR.value,
            )
            continue

        if not alphas:
            await logger.awarning(
                event="批次没有找到对应的 Alpha 数据",
                batch_index=batch_idx,
                alpha_ids=batch_alpha_ids,
                emoji=LoggingEmoji.WARNING.value,
            )
            continue

        alpha_profiles: List[AlphaProfile] = []
        alphas_data_fields: Dict[AlphaProfile, List[AlphaProfileDataFields]] = {}
        for alpha in alphas:
            profile, alpha_data_fields = await _build_alpha_profile(
                alpha=alpha,
                fast_expression_manager=fast_expression_manager,
                logger=logger,
            )
            if profile:
                alpha_profiles.append(profile)

                if alpha_data_fields:
                    alphas_data_fields[profile] = alpha_data_fields

        if alpha_profiles:
            await logger.ainfo(
                event="批次成功构建 AlphaProfile",
                batch_index=batch_idx,
                profile_count=len(alpha_profiles),
                alpha_ids=batch_alpha_ids,
                emoji=LoggingEmoji.INFO.value,
            )
            try:
                await alpha_profile_manager.bulk_save_profile_to_db(
                    profiles=alpha_profiles
                )
            except Exception as e:
                await logger.aerror(
                    event="批量保存 AlphaProfile 失败",
                    batch_index=batch_idx,
                    error=str(e),
                    emoji=LoggingEmoji.ERROR.value,
                )

            try:
                await alpha_profile_manager.bulk_save_profile_data_fields_to_db(
                    profile_data_fields=alphas_data_fields
                )
            except Exception as e:
                await logger.aerror(
                    event="批量保存 AlphaProfileDataFields 失败",
                    batch_index=batch_idx,
                    error=str(e),
                    emoji=LoggingEmoji.ERROR.value,
                )

    await logger.ainfo(
        event=f"退出 {build_alpha_profiles_process_runner.__qualname__}",
        emoji=LoggingEmoji.STEP_OUT_FUNC.value,
    )


@async_exception_handler
async def _build_alpha_profile(
    alpha: Alpha,
    fast_expression_manager: AbstractFastExpressionManager,
    logger: BoundLogger,
) -> Tuple[Optional[AlphaProfile], List[AlphaProfileDataFields]]:
    try:
        # 解析正则表达式（regular/selection/combo）
        regular: Optional[FastExpression] = (
            await fast_expression_manager.parse(
                alpha.regular.code, type=FastExpressionType.REGULAR
            )
            if alpha.regular
            else None
        )
        selection: Optional[FastExpression] = (
            await fast_expression_manager.parse(
                alpha.selection.code, type=FastExpressionType.SELECTION
            )
            if alpha.selection
            else None
        )
        combo: Optional[FastExpression] = (
            await fast_expression_manager.parse(
                alpha.combo.code, type=FastExpressionType.COMBO
            )
            if alpha.combo
            else None
        )

        alpha_profile: AlphaProfile = AlphaProfile(
            alpha_id=alpha.alpha_id,
            type=alpha.type,
            normalized_regular=regular.normalized_expression if regular else None,
            regular_hash=regular.hash if regular else None,
            regular_fingerprint=regular.fingerprint if regular else None,
            normalized_selection=selection.normalized_expression if selection else None,
            selection_hash=selection.hash if selection else None,
            selection_fingerprint=selection.fingerprint if selection else None,
            normalized_combo=combo.normalized_expression if combo else None,
            combo_hash=combo.hash if combo else None,
            combo_fingerprint=combo.fingerprint if combo else None,
            used_data_fields=list(regular.used_data_fields) if regular else [],
            used_operators=list(regular.used_operators) if regular else [],
        )

        alpha_data_fields: List[AlphaProfileDataFields] = []
        if regular:
            for data_field in regular.used_data_fields:
                alpha_data_fields.append(
                    AlphaProfileDataFields(
                        alpha_id=alpha.alpha_id,
                        data_field=data_field,
                    )
                )

        return (alpha_profile, alpha_data_fields)
    except Exception as e:
        await logger.aerror(
            event="解析正则表达式失败",
            alpha_id=getattr(alpha, "id", None),
            error=str(e),
            emoji=LoggingEmoji.ERROR.value,
        )
        return (None, [])


class AlphaProfilesService(AbstractAlphaProfilesService, BaseProcessSafeClass):
    def __init__(
        self,
        alpha_manager: AbstractAlphaManager,
        alpha_profile_manager: AbstractAlphaProfileManager,
        fast_expression_manager: AbstractFastExpressionManager,
    ) -> None:
        self.alpha_manager: AbstractAlphaManager = alpha_manager
        self.fast_expression_manager: AbstractFastExpressionManager = (
            fast_expression_manager
        )
        self.alpha_profile_manager: AbstractAlphaProfileManager = alpha_profile_manager
        super().__init__()

    @async_exception_handler
    async def build_alpha_profiles(
        self,
        fast_expression_manager_factory: FastExpressionManagerFactory,
        alpha_profile_manager_factory: AlphaProfileManagerFactory,
        date_created_gt: Optional[datetime] = None,
        date_created_lt: Optional[datetime] = None,
        parallel: int = 1,
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event=f"进入 {self.build_alpha_profiles.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self.build_alpha_profiles.__qualname__} 入参",
            method=self.build_alpha_profiles.__qualname__,
            date_created_gt=date_created_gt,
            date_created_lt=date_created_lt,
            parallel=parallel,
            emoji=LoggingEmoji.DEBUG.value,
        )

        # 自动获取时间范围
        if date_created_gt is None:
            first_alpha: Optional[AlphaView] = (
                await self.alpha_manager.fetch_first_alpha_from_platform()
            )
            date_created_gt = first_alpha.date_created if first_alpha else datetime.min
        if date_created_lt is None:
            last_alpha: Optional[AlphaView] = (
                await self.alpha_manager.fetch_last_alpha_from_platform()
            )
            date_created_lt = last_alpha.date_created if last_alpha else datetime.max

        async with session_manager.get_session(
            db=Database.ALPHAS,
            readonly=True,
        ) as session:
            alpha_ids: List[int] = await alpha_dal.find_ids_by(
                Alpha.date_created >= date_created_gt,
                Alpha.date_created <= date_created_lt,
                session=session,
            )
            if not alpha_ids:
                await self.log.ainfo(
                    event="没有符合条件的 alpha 数据",
                    message="无法构建 AlphaProfile，因为没有符合条件的 alpha 数据",
                    date_created_gt=date_created_gt,
                    date_created_lt=date_created_lt,
                    emoji=LoggingEmoji.INFO.value,
                )
                return

        await self.log.ainfo(
            event="开始构建 AlphaProfile",
            message=f"将处理 {len(alpha_ids)} 个 Alpha 数据",
            emoji=LoggingEmoji.INFO.value,
        )

        # 按 parallel 分组，分配到 parallel 个子列表
        alpha_groups: List[List[int]] = [
            alpha_ids[i::parallel] for i in range(parallel)
        ]

        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            tasks = [
                loop.run_in_executor(
                    executor,
                    process_async_runner,
                    build_alpha_profiles_process_runner,
                    group,
                    100,
                    fast_expression_manager_factory,
                    alpha_profile_manager_factory,
                )
                for group in alpha_groups
            ]
            await asyncio.gather(*tasks)

        await self.log.ainfo(
            event="AlphaProfile 构建完成",
            emoji=LoggingEmoji.SUCCESS.value,
        )
        await self.log.ainfo(
            event=f"退出 {self.build_alpha_profiles.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )


class AlphaProfilesServiceFactory(BaseProcessSafeFactory[AbstractAlphaProfilesService]):
    def __init__(
        self,
        alpha_manager_factory: AlphaManagerFactory,
        fast_expression_manager_factory: FastExpressionManagerFactory,
        alpha_profile_manager_factory: AlphaProfileManagerFactory,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._alpha_manager: Optional[AbstractAlphaManager] = None
        self._alpha_manager_factory: AlphaManagerFactory = alpha_manager_factory
        self._alpha_profile_manager: Optional[AbstractAlphaProfileManager] = None
        self._alpha_profile_manager_factory: AlphaProfileManagerFactory = (
            alpha_profile_manager_factory
        )
        self._fast_expression_manager: Optional[AbstractFastExpressionManager] = None
        self._fast_expression_manager_factory: FastExpressionManagerFactory = (
            fast_expression_manager_factory
        )

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
            "_alpha_manager": self._alpha_manager_factory,
            "_alpha_profile_manager": self._alpha_profile_manager_factory,
            "_fast_expression_manager": self._fast_expression_manager_factory,
        }
        await self.log.ainfo(
            event=f"退出 {self._dependency_factories.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return factories

    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaProfilesService:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        if self._alpha_manager is None:
            await self.log.aerror(
                event=f"{AbstractAlphaManager.__name__} 未初始化",
                message=(
                    f"{AbstractAlphaManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractAlphaProfilesService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractAlphaManager.__name__} 未初始化")

        if self._alpha_profile_manager is None:
            await self.log.aerror(
                event=f"{AbstractAlphaProfileManager.__name__} 未初始化",
                message=(
                    f"{AbstractAlphaProfileManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractAlphaProfilesService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractAlphaProfileManager.__name__} 未初始化")

        if self._fast_expression_manager is None:
            await self.log.aerror(
                event=f"{AbstractFastExpressionManager.__name__} 未初始化",
                message=(
                    f"{AbstractFastExpressionManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractAlphaProfilesService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractFastExpressionManager.__name__} 未初始化")

        service: AbstractAlphaProfilesService = AlphaProfilesService(
            alpha_manager=self._alpha_manager,
            alpha_profile_manager=self._alpha_profile_manager,
            fast_expression_manager=self._fast_expression_manager,
        )

        await self.log.ainfo(
            event=f"{AbstractAlphaProfilesService.__name__} 实例创建成功",
            message=(
                f"成功创建 {AbstractAlphaProfilesService.__name__} 实例，"
                f"使用的 {AbstractAlphaManager.__name__} 工厂为 {self._alpha_manager_factory.__class__.__name__}"
            ),
            emoji=LoggingEmoji.SUCCESS.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return service
