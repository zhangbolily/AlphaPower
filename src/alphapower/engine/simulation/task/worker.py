"""
@file   worker.py
@brief  工作者类
"""

from typing import Callable, Optional

from alphapower.client import WorldQuantClient

from .scheduler_abc import AbstractScheduler
from .worker_abc import AbstractWorker


class Worker(AbstractWorker):
    """
    工作者类，用于执行任务。
    """

    def __init__(self, client: WorldQuantClient) -> None:
        """
        初始化工作者实例。
        """
        self._scheduler: Optional[AbstractScheduler] = None
        self._client = client

    async def _do_work(self) -> None:
        """
        执行工作的方法。
        """
        async with self._client:
            if self._scheduler is None:
                raise ValueError("Scheduler is not set.")
            # 这里可以添加执行任务的逻辑
            pass

    def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """
        设置调度器。

        :param scheduler: 调度器实例
        """
        self._scheduler = scheduler

    async def run(self) -> None:
        """
        运行工作者，执行任务。
        """
        async with self._client:
            if self._scheduler is None:
                raise ValueError("Scheduler is not set.")

    async def stop(self, cancel_tasks: bool = False) -> None:
        """
        停止工作者，清理资源。
        """

    def add_task_complete_callback(self, callback: Callable) -> None:
        """
        添加任务完成回调函数。
        """
