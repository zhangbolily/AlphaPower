import abc
from datetime import datetime
from typing import Any, List, Optional

from alphapower.constants import Status
from alphapower.entity.alphas import Alpha
from alphapower.view.alpha import AlphaView


class AbstractAlphaManager(abc.ABC):
    """
    Abstract base class for AlphaManager.

    competition: Optional[Any] = None
    date_created_gt: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("dateCreated>", "date_created_gt"),
        serialization_alias="dateCreated>",
    )
    date_created_lt: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("dateCreated<", "date_created_lt"),
        serialization_alias="dateCreated<",
    )
    hidden: Optional[bool] = None
    limit: Optional[int] = None
    name: Optional[str] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("status", "status_eq"),
        serialization_alias="status",
    )
    status_ne: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("status//!", "status_ne"),
        serialization_alias="status//!",
    )
    """

    @abc.abstractmethod
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
        """
        Fetch the count of alphas from the platform.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
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
        """
        Fetch alphas from the platform.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def fetch_first_alpha_from_platform(self) -> Optional[AlphaView]:
        """
        Fetch the first alpha from the platform.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def fetch_last_alpha_from_platform(self) -> Optional[AlphaView]:
        """
        Fetch the last alpha from the platform.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def fetch_first_alpha_from_db(self) -> Optional[Alpha]:
        """
        Fetch the first alpha from the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def fetch_last_alpha_from_db(self) -> Optional[Alpha]:
        """
        Fetch the last alpha from the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def save_alphas_to_db(
        self,
        alphas_view: List[AlphaView],
    ) -> None:
        """
        Save alphas to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def build_alpha_entity_from_view(
        self,
        alpha_view: AlphaView,
    ) -> Alpha:
        """
        Build an alpha entity from the view.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )
