from typing import Any, Dict, List

from alphapower.constants import Database
from alphapower.dal import alpha_profile_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alpha_profiles import AlphaProfile
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)

from .alpha_profile_manager_abc import AbstractAlphaProfileManager


class AlphaProfileManager(BaseProcessSafeClass, AbstractAlphaProfileManager):
    def __init__(self) -> None:
        super().__init__()

    async def bulk_save_profile_to_db(
        self,
        profiles: List[AlphaProfile],
        **kwargs: Any,
    ) -> None:
        async with (
            session_manager.get_session(
                db=Database.ALPHAS,
            ) as session,
            session.begin(),
        ):
            await alpha_profile_dal.bulk_upsert_by_unique_key(
                session=session,
                entities=profiles,
                unique_key="alpha_id",
            )

    async def save_profile_to_db(
        self,
        profile: AlphaProfile,
        **kwargs: Any,
    ) -> None:
        await self.bulk_save_profile_to_db(
            profiles=[profile],
            **kwargs,
        )


class AlphaProfileManagerFactory(BaseProcessSafeFactory):
    """
    Factory for creating AlphaProfileManager instances.
    """

    def __init__(self) -> None:
        super().__init__()

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        return {}

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractAlphaProfileManager:
        """
        Build an instance of AlphaProfileManager.
        """
        return AlphaProfileManager()
