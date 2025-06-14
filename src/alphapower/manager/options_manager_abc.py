from abc import ABC, abstractmethod
from typing import Any, List, Tuple

from alphapower.constants import Delay, InstrumentType, Region, Universe
from alphapower.view.options import (
    AlphasOptions,
    SimulationsOptions,
    SimulationsOptionsSettings,
)


class AbstractOptionsManager(ABC):
    @abstractmethod
    async def fetch_alphas_options_from_platform(
        self, user_id: str, **kwargs: Any
    ) -> AlphasOptions:
        """
        Fetch alphas options from the platform.
        Returns an AlphasOptions object.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_simulations_options_from_platform(
        self, **kwargs: Any
    ) -> SimulationsOptions:
        """
        Fetch simulations options from the platform.
        Returns an SimulationsOptions object.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
    @abstractmethod
    async def simulations_options_settings(
        self,
        simulations_options: SimulationsOptions,
        **kwargs: Any,
    ) -> SimulationsOptionsSettings:
        """
        Generate all combinations of settings based on the provided simulations options.
        Returns a list of tuples containing (setting_name, setting_value).
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def region_delays(
        self,
        simulations_options: SimulationsOptions,
        instrument_type: InstrumentType,
        region: Region,
        **kwargs: Any,
    ) -> List[Delay]:
        """
        Generate all combinations of regions and delays based on the provided simulations options.
        Returns a list of tuples containing (Region, Delay).
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def universe_combinations(
        self,
        simulations_options: SimulationsOptions,
        **kwargs: Any,
    ) -> List[Tuple[InstrumentType, Region, Delay, Universe]]:
        """
        Generate all combinations of regions, delays, and universes based on the provided simulations options.
        Returns a list of tuples containing (Region, Delay, Universe).
        """
        raise NotImplementedError("Subclasses must implement this method.")
    