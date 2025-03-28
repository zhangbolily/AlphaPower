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
        priotiry: Optional[int] = None,
    ) -> list[SimulationTask]:
        """
        从数据源获取任务。
        :return: SimulationTask 的列表
        """
        pass
