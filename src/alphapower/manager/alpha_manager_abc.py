import abc
from datetime import datetime
from typing import Any, Dict, List, Optional

from alphapower.constants import Status
from alphapower.entity.alphas import AggregateData, Alpha
from alphapower.view.alpha import AggregateDataView, AlphaView


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
    async def bulk_save_alpha_to_db(
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

    @abc.abstractmethod
    async def build_aggregate_data_from_view(
        self,
        sample_data: AggregateDataView,
        primary_id: Optional[int] = None,
    ) -> AggregateData:
        """
        Build aggregate data from the view.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def save_aggregate_data_to_db(
        self,
        alpha_id: str,
        in_sample_view: Optional[AggregateDataView],
        out_sample_view: Optional[AggregateDataView],
        train_view: Optional[AggregateDataView],
        test_view: Optional[AggregateDataView],
        prod_view: Optional[AggregateDataView],
    ) -> Dict[str, AggregateData]:
        """
        Save aggregate data to the database.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )

    @abc.abstractmethod
    async def bulk_save_aggregate_data_to_db(
        self,
        alpha_ids: List[str],
        in_sample_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        out_sample_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        train_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        test_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
        prod_view_map: Optional[Dict[str, Optional[AggregateDataView]]],
    ) -> Dict[str, Dict[str, AggregateData]]:
        """
        Save aggregate data to the database in bulk.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )
