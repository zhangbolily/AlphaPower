from abc import ABC, abstractmethod

from worldquant.internal.entity import SimulationTask


class AbstractTaskProvider(ABC):
    """
    抽象类，用于定义任务提供者的接口。
    """

    @abstractmethod
    def fetch_tasks(self, count, priotiry) -> list[SimulationTask]:
        """
        从数据源获取任务。
        :return: SimulationTask 的列表
        """
        pass


class AbstractScheduler(ABC):
    """
    抽象类，用于定义调度器的通用接口。
    """

    @abstractmethod
    def get_next_task(self) -> SimulationTask:
        """
        获取下一个任务。
        :return: SimulationTask 对象
        """
        pass

    @abstractmethod
    def add_task(self, task: SimulationTask):
        """
        添加一个任务到调度器。
        :param task: SimulationTask 对象
        """
        pass

    @abstractmethod
    def has_tasks(self) -> bool:
        """
        检查是否还有任务待调度。
        :return: 如果有任务返回 True，否则返回 False
        """
        pass

    @abstractmethod
    def set_task_provider(self, task_provider: AbstractTaskProvider):
        """
        设置任务提供者。
        :param task_provider: AbstractTaskProvider 对象
        """
        pass

    @abstractmethod
    def get_next_batch(self, batch_size: int) -> list[SimulationTask]:
        """
        获取下一个批量任务。
        :param batch_size: 批量任务的大小
        :return: SimulationTask 对象的列表
        """
        pass
