import abc
from typing import Any, Optional

from alphapower.constants import Delay, InstrumentType, Region, Universe


class AbstractDatasetsService(abc.ABC):
    """
    Abstract base class for Dataset Service.
    This class defines the interface for all Dataset Services.
    """

    @abc.abstractmethod
    async def sync_datasets(
        self,
        category: Optional[str] = None,
        delay: Optional[Delay] = None,
        instrumentType: Optional[InstrumentType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize datasets from the platform.
        """
        raise NotImplementedError("sync_datasets_from_platform method not implemented")
