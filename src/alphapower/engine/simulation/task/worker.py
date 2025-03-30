"""
@file: worker.py
@brief: Mock工作者类
@details:
    该模块定义了一个Mock工作者类，用于模拟工作者的行为。
@note:
    该模块是 AlphaPower 引擎的一部分
"""

import asyncio
from typing import List, Optional

from alphapower.entity import SimulationTask, SimulationTaskStatus

from .scheduler_abc import AbstractScheduler
from .worker_abc import AbstractWorker


class MockWorker(AbstractWorker):
    """
    Mock工作者类，用于模拟工作者的行为。
    """

    def __init__(self, work_time: int = 1, job_slots: int = 1):
        """
        初始化Mock工作者。
        """
        self.scheduler: Optional[AbstractScheduler] = None
        self.tasks: List[SimulationTask] = []
        self.running: bool = False
        self.running_lock: asyncio.Lock = asyncio.Lock()
        self.shutdown: bool = False
        self.work_time: int = work_time
        self.job_slots: int = job_slots

    def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """
        设置调度器。
        :param scheduler: 调度器实例
        """
        self.scheduler = scheduler

    async def run(self):
        """
        运行工作者，执行任务。
        """
        self.running = True

        async with self.running_lock:
            while not self.shutdown:
                if self.tasks:
                    raise RuntimeError(
                        "任务列表不为空，存在已完成任务未清理，无法运行工作者。"
                    )
                if self.scheduler is None or not isinstance(
                    self.scheduler, AbstractScheduler
                ):
                    raise RuntimeError("调度器未设置或类型不正确。")

                self.tasks = await self.scheduler.schedule(batch_size=self.job_slots)
                await asyncio.sleep(self.work_time)  # 模拟异步操作
                # 模拟任务完成
                for task in self.tasks:
                    task.status = SimulationTaskStatus.COMPLETE

    async def stop(self, cancel_tasks=False):
        """
        停止工作者，清理资源。
        :param cancel_tasks: 是否取消任务
        """
        self.shutdown = True

        async with self.running_lock:
            await asyncio.sleep(self.work_time)
            if cancel_tasks:
                self.tasks.clear()
            self.running = False
