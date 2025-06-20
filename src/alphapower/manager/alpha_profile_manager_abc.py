from abc import ABC, abstractmethod
from typing import Any, Dict, List

from alphapower.entity.alpha_profiles import AlphaProfile, AlphaProfileDataFields


class AbstractAlphaProfileManager(ABC):
    @abstractmethod
    async def bulk_save_profile_to_db(
        self,
        profiles: List[AlphaProfile],
        **kwargs: Any,
    ) -> None:
        """
        Bulk save alpha profiles to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abstractmethod
    async def save_profile_to_db(
        self,
        profile: AlphaProfile,
        **kwargs: Any,
    ) -> None:
        """
        Save a single alpha profile to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abstractmethod
    async def bulk_save_profile_data_fields_to_db(
        self,
        profile_data_fields: Dict[AlphaProfile, List[AlphaProfileDataFields]],
        **kwargs: Any,
    ) -> None:
        """
        Bulk save alpha profile data fields to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abstractmethod
    async def save_profile_data_fields_to_db(
        self,
        profile: AlphaProfile,
        profile_data_fields: AlphaProfileDataFields,
        **kwargs: Any,
    ) -> None:
        """
        Save a single alpha profile data field to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )
