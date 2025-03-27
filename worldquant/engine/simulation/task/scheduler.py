from typing import Optional

from worldquant.internal.entity import SimulationTask

from .scheduler_abc import AbstractScheduler, AbstractTaskProvider


class PriorityScheduler(AbstractScheduler):
    def __init__(
        self,
        tasks: Optional[list[SimulationTask]] = None,
        task_provider: Optional[AbstractTaskProvider] = None,
    ):
        """
        初始化调度器，接收任务列表或任务提供者。
        :param tasks: SimulationTask 的列表（可选）
        :param task_provider: 一个可调用对象，用于从数据库或其他数据源获取任务（可选）
        """
        self.tasks: list[SimulationTask] = tasks or []
        self.task_provider: Optional[AbstractTaskProvider] = task_provider

    def fetch_tasks_from_provider(self):
        """
        从任务提供者获取任务并添加到任务列表。
        """
        if self.task_provider:
            new_tasks = self.task_provider.fetch_tasks()
            if new_tasks:
                self.tasks.extend(new_tasks)

    def get_next_task(self) -> SimulationTask:
        """
        获取下一个优先级最高的任务。
        :return: SimulationTask 对象
        """
        if not self.tasks:
            self.fetch_tasks_from_provider()
            if not self.tasks:
                raise ValueError("任务列表为空，无法调度任务。")

        # 按优先级排序，假设 SimulationTask 有一个 `priority` 属性
        self.tasks.sort(key=lambda task: int(task.priority), reverse=True)
        return self.tasks.pop(0)  # 返回优先级最高的任务

    def add_task(self, task: SimulationTask):
        """
        添加一个新任务到调度器。
        :param task: SimulationTask 对象
        """
        self.tasks.append(task)

    def has_tasks(self) -> bool:
        """
        检查是否还有任务待调度。
        :return: 如果有任务返回 True，否则返回 False
        """
        if not self.tasks:
            self.fetch_tasks_from_provider()
        return len(self.tasks) > 0

    def set_task_provider(self, task_provider: AbstractTaskProvider):
        """
        设置任务提供者。
        :param task_provider: AbstractTaskProvider 对象
        """
        self.task_provider = task_provider

    def get_next_batch(self, batch_size: int) -> list[SimulationTask]:
        """
        获取下一个批量任务。
        :param batch_size: 批量任务的大小
        :return: SimulationTask 对象的列表
        """
        if not self.tasks:
            self.fetch_tasks_from_provider()
            if not self.tasks:
                raise ValueError("任务列表为空，无法调度任务。")

        # 按优先级排序，假设 SimulationTask 有一个 `priority` 属性
        self.tasks.sort(key=lambda task: int(task.priority), reverse=True)
        batch = self.tasks[:batch_size]  # 获取优先级最高的批量任务
        self.tasks = self.tasks[batch_size:]  # 移除已获取的任务
        return batch
