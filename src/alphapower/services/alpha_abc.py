import abc
from datetime import datetime, tzinfo
from typing import Any, List, Optional, Tuple

from alphapower.constants import Status


class AbstractAlphaService(abc.ABC):
    """
    Abstract base class for Alpha Service.
    This class defines the interface for all Alpha Services.
    """

    @abc.abstractmethod
    async def sync_alphas(
        self,
        competition: Optional[str],
        date_created_gt: Optional[datetime],
        date_created_lt: Optional[datetime],
        tz: tzinfo,
        hidden: Optional[bool],
        name: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        concurrency: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize alphas from the platform.
        """
        raise NotImplementedError("sync_alphas_from_platform method not implemented")

    @abc.abstractmethod
    async def sync_alphas_in_ranges(
        self,
        competition: Optional[str],
        created_time_ranges: List[Tuple[datetime, datetime]],
        tz: tzinfo,
        hidden: Optional[bool],
        name: Optional[str],
        status_eq: Optional[Status],
        status_ne: Optional[Status],
        **kwargs: Any,
    ) -> None:
        """
        Synchronize alphas in ranges.
        """
        raise NotImplementedError("sync_alphas_in_ranges method not implemented")
