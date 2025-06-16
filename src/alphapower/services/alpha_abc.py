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
        tz: tzinfo,
        competition: Optional[str] = None,
        date_created_gt: Optional[datetime] = None,
        date_created_lt: Optional[datetime] = None,
        hidden: Optional[bool] = None,
        name: Optional[str] = None,
        status_eq: Optional[Status] = None,
        status_ne: Optional[Status] = None,
        concurrency: int = 1,
        aggregate_data_only: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize alphas from the platform.
        """
        raise NotImplementedError("sync_alphas_from_platform method not implemented")

    @abc.abstractmethod
    async def sync_alphas_in_ranges(
        self,
        tz: tzinfo,
        competition: Optional[str] = None,
        created_time_ranges: List[Tuple[datetime, datetime]] = [],
        hidden: Optional[bool] = None,
        name: Optional[str] = None,
        status_eq: Optional[Status] = None,
        status_ne: Optional[Status] = None,
        aggregate_data_only: bool = False,
        concurrency: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize alphas in ranges.
        """
        raise NotImplementedError("sync_alphas_in_ranges method not implemented")

    @abc.abstractmethod
    async def sync_alphas_incremental(
        self,
        tz: tzinfo,
        aggregate_data_only: bool = False,
        concurrency: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Synchronize alphas incrementally.
        """
        raise NotImplementedError("sync_alphas_incremental method not implemented")

    @abc.abstractmethod
    async def fix_alphas_properties(
        self,
        **kwargs: Any,
    ) -> None:
        """
        Fix alphas properties.
        """
        raise NotImplementedError("fix_alphas_properties method not implemented")

    @abc.abstractmethod
    async def build_alpha_profiles(
        self,
        date_created_gt: Optional[datetime] = None,
        date_created_lt: Optional[datetime] = None,
        parallel: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Build alpha profiles.
        """
        raise NotImplementedError("build_alpha_profiles method not implemented")
