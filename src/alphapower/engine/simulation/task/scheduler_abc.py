"""
定义调度器的抽象类。
"""

from abc import ABC, abstractmethod
from typing import List

from alphapower.entity import SimulationTask

from .provider_abc import AbstractTaskProvider


class AbstractScheduler(ABC):
    """
    抽象类，用于定义调度器的通用接口。
    """

    @abstractmethod
    async def schedule(self, batch_size: int = 1) -> List[SimulationTask]:
        """
        调度任务，支持单个任务或批量任务。
        :param batch_size: 批量任务的大小，默认为 1
        :return: SimulationTask 对象的列表
        """

    @abstractmethod
    def add_tasks(self, tasks: List[SimulationTask]) -> None:
        """
        添加任务到调度器。
        :param tasks: SimulationTask 对象列表
        """

    @abstractmethod
    async def has_tasks(self) -> bool:
        """
        检查是否还有任务待调度。
        :return: 如果有任务返回 True，否则返回 False
        """

    @abstractmethod
    def set_task_provider(self, task_provider: AbstractTaskProvider) -> None:
        """
        设置任务提供者。
        :param task_provider: AbstractTaskProvider 对象
        """
