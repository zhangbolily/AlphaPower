"""
@file: mock_task_worker.py
@brief: Mock工作者类
@details:
    该模块定义了一个Mock工作者类，用于模拟工作者的行为。
"""

import asyncio
from typing import Callable, List, Optional

from alphapower.engine import AbstractScheduler, AbstractWorker
from alphapower.entity import SimulationTask, SimulationTaskStatus


class MockWorker(AbstractWorker):
    """
    Mock工作者类，用于模拟工作者的行为。
    """

    def __init__(self, work_time: int = 1, job_slots: int = 1):
        """
        初始化Mock工作者。
        """
        self._scheduler: Optional[AbstractScheduler] = None
        self._tasks: List[SimulationTask] = []
        self._running: bool = False
        self._running_lock: asyncio.Lock = asyncio.Lock()
        self._shutdown: bool = False
        self._work_time: int = work_time
        self._job_slots: int = job_slots
        self._task_complete_callbacks: List[Callable] = []

    async def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """
        设置调度器。
        :param scheduler: 调度器实例
        """
        self._scheduler = scheduler

    async def run(self) -> None:
        """
        运行工作者，执行任务。
        """
        self._running = True

        async with self._running_lock:
            while not self._shutdown:
                if self._tasks:
                    raise RuntimeError(
                        "任务列表不为空，存在已完成任务未清理，无法运行工作者。"
                    )
                if self._scheduler is None or not isinstance(
                    self._scheduler, AbstractScheduler
                ):
                    raise RuntimeError("调度器未设置或类型不正确。")

                self._tasks = await self._scheduler.schedule(batch_size=self._job_slots)
                await asyncio.sleep(self._work_time)  # 模拟异步操作
                # 模拟任务完成
                for task in self._tasks:
                    task.status = SimulationTaskStatus.COMPLETE

                self._tasks.clear()

    async def stop(self, cancel_tasks: bool = False) -> None:
        """
        停止工作者，清理资源。
        :param cancel_tasks: 是否取消任务
        """
        self._shutdown = True

        async with self._running_lock:
            await asyncio.sleep(self._work_time)
            if cancel_tasks:
                self._tasks.clear()
            self._running = False

    async def add_task_complete_callback(self, callback: Callable) -> None:
        """
        添加任务完成回调函数。
        :param callback: 回调函数
        """
        self._task_complete_callbacks.append(callback)

    async def get_current_tasks(self) -> List[SimulationTask]:
        """
        获取当前任务的信息。
        :return: 当前任务列表
        """
        return self._tasks
