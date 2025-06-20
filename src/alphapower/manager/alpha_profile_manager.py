from typing import Any, Dict, List

from alphapower.constants import Database, LoggingEmoji
from alphapower.dal import alpha_profile_dal, alpha_profile_data_fields_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alpha_profiles import AlphaProfile, AlphaProfileDataFields
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)

from .alpha_profile_manager_abc import AbstractAlphaProfileManager


class AlphaProfileManager(BaseProcessSafeClass, AbstractAlphaProfileManager):
    def __init__(self) -> None:
        super().__init__()

    @async_exception_handler
    async def bulk_save_profile_to_db(
        self,
        profiles: List[AlphaProfile],
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event="批量保存 AlphaProfile",
            message=f"进入 {self.bulk_save_profile_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event="方法入参",
            message=f"{self.bulk_save_profile_to_db.__qualname__} 入参",
            profiles=[p.alpha_id for p in profiles],
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
        )
        async with (
            session_manager.get_session(db=Database.ALPHAS) as session,
            session.begin(),
        ):
            await alpha_profile_dal.bulk_upsert_by_unique_key(
                session=session,
                entities=profiles,
                unique_key="alpha_id",
            )
        await self.log.ainfo(
            event="批量保存 AlphaProfile 完成",
            message=f"退出 {self.bulk_save_profile_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def save_profile_to_db(
        self,
        profile: AlphaProfile,
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event="保存单个 AlphaProfile",
            message=f"进入 {self.save_profile_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.bulk_save_profile_to_db(
            profiles=[profile],
            **kwargs,
        )
        await self.log.ainfo(
            event="保存单个 AlphaProfile 完成",
            message=f"退出 {self.save_profile_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def bulk_save_profile_data_fields_to_db(
        self,
        profile_data_fields: Dict[AlphaProfile, List[AlphaProfileDataFields]],
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event="批量保存 AlphaProfileDataFields",
            message=f"进入 {self.bulk_save_profile_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event="方法入参",
            message=f"{self.bulk_save_profile_data_fields_to_db.__qualname__} 入参",
            profile_data_fields=[p.alpha_id for p in profile_data_fields],
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
        )

        inserted_fields: List[AlphaProfileDataFields] = []
        async with session_manager.get_session(
            db=Database.ALPHAS, readonly=True
        ) as session:
            for profile, fields in profile_data_fields.items():
                existed: List[AlphaProfileDataFields] = (
                    await alpha_profile_data_fields_dal.find_by(
                        AlphaProfileDataFields.alpha_id == profile.alpha_id,
                        session=session,
                    )
                )

                if len(existed) > 0:
                    await self.log.awarning(
                        event=f"发现已存在的 {AlphaProfileDataFields.__name__}",
                        message=f"{AlphaProfileDataFields.__name__} 已存在，跳过插入：{profile.alpha_id}",
                        emoji=LoggingEmoji.WARNING.value,
                    )
                    continue

                for field in fields:
                    field.alpha_id = profile.alpha_id
                    inserted_fields.append(field)

        async with (
            session_manager.get_session(db=Database.ALPHAS) as session,
            session.begin(),
        ):
            await alpha_profile_data_fields_dal.bulk_upsert(
                session=session,
                entities=inserted_fields,
            )
        await self.log.ainfo(
            event=f"批量保存 {AlphaProfileDataFields.__name__} 完成",
            message=f"退出 {self.bulk_save_profile_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def save_profile_data_fields_to_db(
        self,
        profile: AlphaProfile,
        profile_data_fields: AlphaProfileDataFields,
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event="保存单个 AlphaProfileDataFields",
            message=f"进入 {self.save_profile_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.bulk_save_profile_data_fields_to_db(
            profile_data_fields={profile: [profile_data_fields]},
            **kwargs,
        )
        await self.log.ainfo(
            event="保存单个 AlphaProfileDataFields 完成",
            message=f"退出 {self.save_profile_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )


class AlphaProfileManagerFactory(BaseProcessSafeFactory[AbstractAlphaProfileManager]):
    def __init__(self) -> None:
        super().__init__()

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        await self.log.adebug(
            event=f"{self._dependency_factories.__qualname__} 出参",
            message="依赖工厂列表生成成功",
            factories=[],
            emoji=LoggingEmoji.DEBUG.value,
        )
        return {}

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaProfileManager:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__} 方法",
            message="开始构建 AlphaProfileManager 实例",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self._build.__qualname__} 入参",
            message="构建方法参数",
            args=args,
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
        )
        manager: AbstractAlphaProfileManager = AlphaProfileManager()
        await self.log.adebug(
            event=f"{self._build.__qualname__} 出参",
            message="AlphaProfileManager 实例构建成功",
            manager_type=type(manager).__name__,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__} 方法",
            message="完成 AlphaProfileManager 实例构建",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return manager
