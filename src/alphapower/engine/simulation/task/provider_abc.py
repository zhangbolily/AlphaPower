# 用于定义任务提供者的接口。

from abc import ABC, abstractmethod
from typing import Optional

from alphapower.internal.entity import SimulationTask


class AbstractTaskProvider(ABC):
    """
    抽象类，用于定义任务提供者的接口。
    """

    @abstractmethod
    async def fetch_tasks(
        self,
        count: int,
        priority: Optional[int] = None,
    ) -> list[SimulationTask]:
        """
        从数据源获取任务。
        :return: SimulationTask 的列表
        """

    @abstractmethod
    async def acknowledge_scheduled_tasks(self, task_ids: list[int]) -> None:
        """
        确认调度的任务。
        :param task_ids: 任务的唯一标识符列表
        """
