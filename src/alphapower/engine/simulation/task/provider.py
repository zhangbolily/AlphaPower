"""
This module provides task providers for simulation tasks, including database-backed providers.
"""

import asyncio
from datetime import datetime
from typing import List, Optional, Set

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.engine.simulation.task.core import (
    get_simulation_tasks_by,
    update_simulation_task_scheduled_info,
)
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.wraps import Transactional, Propagation

from .provider_abc import AbstractTaskProvider


class DatabaseTaskProvider(AbstractTaskProvider):
    """
    从数据库中获取任务的提供者。
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.cursor = 0
        self.committing_scheduled_task_ids: Set[int] = set()
        self._lock = asyncio.Lock()

    async def fetch_tasks(
        self,
        count: int = 10,  # 设置默认值
        priority: Optional[int] = None,
        sample_interval: int = 1,  # 新增参数，默认为1（不跳采样）
    ) -> List[SimulationTask]:
        """
        从数据库中获取任务，支持跳采样。
        """
        sampled_tasks: List[SimulationTask] = []
        offset = self.cursor

        while len(sampled_tasks) < count:
            # 分批获取任务
            tasks = await get_simulation_tasks_by(
                session=self.session,
                status=SimulationTaskStatus.PENDING,
                priority=priority,
                not_in_={
                    "id": list(self.committing_scheduled_task_ids),
                },
                limit=sample_interval,  # 每次获取 sample_interval 条记录
                offset=offset,
            )

            if not tasks:  # 如果没有更多任务，提前退出
                self.cursor = 0
                break

            # 取出当前批次的第一个任务作为采样
            sampled_tasks.append(tasks[0])
            offset += sample_interval  # 更新偏移量
            self.cursor += sample_interval

        return sampled_tasks[:count]  # 返回满足数量的任务

    async def acknowledge_scheduled_tasks(self, task_ids: List[int]) -> None:
        """
        确认调度的任务。
        """
        async with self._lock:
            # 如果任务已经在提交中，记录下来待确认
            self.committing_scheduled_task_ids.update(task_ids)

        @Transactional(propagation=Propagation.NESTED)
        async def _func_in_transaction(session: AsyncSession):
            for task_id in task_ids:
                await update_simulation_task_scheduled_info(
                    session=session,
                    task_id=task_id,
                    scheduled_at=datetime.now(),
                    status=SimulationTaskStatus.SCHEDULED,
                )

        await _func_in_transaction(session=self.session)

        async with self._lock:
            # 提交成功后，从待确认列表中移除
            self.committing_scheduled_task_ids.difference_update(task_ids)
