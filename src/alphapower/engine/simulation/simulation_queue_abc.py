from abc import ABC, abstractmethod
from queue import Queue


class AbstractSimulationQueue(ABC, Queue):
    @abstractmethod
    async def add_to_queue(self, simulation_id: str) -> None:
        """
        Add a simulation to the queue.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def remove_from_queue(self, simulation_id: str) -> None:
        """
        Remove a simulation from the queue.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_next_simulation(self) -> str:
        """
        Get the next simulation ID from the queue.
        """
        raise NotImplementedError("Subclasses must implement this method.")