import abc
from typing import Any, Optional

from alphapower.constants import Delay, InstrumentType, Region, Universe
from alphapower.manager.data_sets_manager import DataSetsManagerFactory


class AbstractDatasetsService(abc.ABC):
    """
    Abstract base class for Dataset Service.
    This class defines the interface for all Dataset Services.
    """

    @abc.abstractmethod
    async def sync_datasets(
        self,
        data_sets_manager_factory: DataSetsManagerFactory,
        category: Optional[str] = None,
        delay: Optional[Delay] = None,
        instrument_type: Optional[InstrumentType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        parallel: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize datasets from the platform.
        """
        raise NotImplementedError("sync_datasets_from_platform method not implemented")
