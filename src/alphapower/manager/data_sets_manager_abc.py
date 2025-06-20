from abc import ABC, abstractmethod
from typing import Any, List, Optional

from alphapower.constants import Delay, InstrumentType, Region, Universe
from alphapower.entity.data import DataField
from alphapower.view.data import DataCategoryView, DataFieldView, DatasetView


class AbstractDataSetsManager(ABC):
    @abstractmethod
    async def fetch_categories_from_platform(self) -> List[DataCategoryView]:
        """
        Fetch data categories from the platform.
        Returns a list of data categories.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_data_sets_from_platform(
        self,
        instrument_type: Optional[InstrumentType] = None,
        region: Optional[Region] = None,
        delay: Optional[Delay] = None,
        universe: Optional[Universe] = None,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        search: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> List[DatasetView]:
        """
        Fetch datasets from the platform.
        Returns a list of datasets.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_data_fields_from_platform(
        self,
        dataset_id: str,
        instrument_type: Optional[InstrumentType] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        delay: Optional[Delay] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> List[DataFieldView]:
        """
        Fetch data fields for a specific dataset from the platform.
        Returns a list of field names.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def build_data_field_entity_from_view(
        self,
        data_field_view: DataFieldView,
        **kwargs: Any,
    ) -> DataField:
        """
        Build a DataField entity from the provided DataFieldView.
        Returns a DataField entity.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def build_data_field_entities_from_views(
        self,
        data_field_views: List[DataFieldView],
        **kwargs: Any,
    ) -> List[DataField]:
        """
        Build a list of DataField entities from the provided list of DataFieldViews.
        Returns a list of DataField entities.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def bulk_save_data_fields_to_db(
        self,
        data_fields: List[DataField],
        **kwargs: Any,
    ) -> None:
        """
        Bulk save data fields to the database.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def save_data_field_to_db(
        self,
        data_field: DataField,
        **kwargs: Any,
    ) -> None:
        """
        Save a single data field to the database.
        """
        raise NotImplementedError("Subclasses must implement this method.")
