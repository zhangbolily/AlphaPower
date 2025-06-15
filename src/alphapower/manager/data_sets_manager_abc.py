from abc import ABC, abstractmethod
from typing import Any, List

from alphapower.view.data import DataCategoryView, DatasetView


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
        self, **kwargs: Any
    ) -> List[DatasetView]:
        """
        Fetch datasets from the platform.
        Returns a list of datasets.
        """
        raise NotImplementedError("Subclasses must implement this method.")
