"""
This module provides task providers for simulation tasks, including database-backed providers.
"""

import asyncio
from typing import List, Optional, Set

from alphapower.constants import Database
from alphapower.dal.simulation import SimulationTaskDAL
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session

from .provider_abc import AbstractTaskProvider


class DatabaseTaskProvider(AbstractTaskProvider):
    """
    从数据库中获取任务的提供者。
    """

    def __init__(self) -> None:
        """
        初始化任务提供者。
        """
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

        async with get_db_session(Database.SIMULATION) as session:
            sampled_task_ids: List[int] = []
            while len(sampled_task_ids) < count:
                dal: SimulationTaskDAL = SimulationTaskDAL(session=session)
                task_ids: List[int] = await dal.find_task_ids_by_filters(
                    status=SimulationTaskStatus.PENDING,
                    priority=priority,
                    notin_={
                        "id": list(self.committing_scheduled_task_ids)
                        + sampled_task_ids,
                    },
                    limit=count * sample_interval,
                    offset=self.cursor,
                )

                if not task_ids:  # 如果没有更多任务，提前退出
                    pending_task_count: int = await dal.count(
                        status=SimulationTaskStatus.PENDING,
                        priority=priority,
                        notin_={
                            "id": list(self.committing_scheduled_task_ids)
                            + sampled_task_ids,
                        },
                    )

                    self.cursor = 0
                    if pending_task_count > 0:
                        # 如果还有待处理的任务，继续循环
                        continue
                    break

                sampled_task_ids.extend(task_ids[::sample_interval])
                self.cursor += len(task_ids)

            sampled_tasks = await dal.find_filtered(
                status=SimulationTaskStatus.PENDING,
                priority=priority,
                in_={"id": sampled_task_ids},
                not_in_={
                    "id": list(self.committing_scheduled_task_ids),
                },
                limit=count,
            )

        return sampled_tasks[:count]  # 返回满足数量的任务

    async def acknowledge_scheduled_tasks(self, task_ids: List[int]) -> None:
        """
        确认调度的任务。
        """
        async with self._lock:
            # 提交成功后，从待确认列表中移除
            self.committing_scheduled_task_ids.difference_update(task_ids)
