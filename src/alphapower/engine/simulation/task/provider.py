"""
此模块提供用于仿真任务的任务提供者，包括基于数据库的任务提供者。

模块功能：
- 定义任务提供者的抽象基类。
- 提供从数据库中获取任务的具体实现。
"""

import asyncio
from typing import List, Optional, Set

from structlog.stdlib import BoundLogger

from alphapower.constants import Database
from alphapower.dal.simulation import SimulationTaskDAL
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import get_logger

from .provider_abc import AbstractTaskProvider

logger: BoundLogger = get_logger(__name__)


class DatabaseTaskProvider(AbstractTaskProvider):
    """
    数据库任务提供者类。

    功能：
    - 从数据库中获取仿真任务。
    - 支持跳采样功能以优化任务获取效率。
    - 提供任务调度确认功能。
    """

    def __init__(self, sample_rate: int = 1) -> None:
        """
        初始化任务提供者。

        参数：
        - sample_rate (int): 采样率，用于跳采样任务，默认为 1（不跳采样）。
        """
        self.cursor = 0
        self.committing_scheduled_task_ids: Set[int] = set()
        self._lock = asyncio.Lock()
        self._sample_rate = sample_rate  # 新增采样率参数
        logger.info(
            event="初始化任务提供者",
            sample_rate=sample_rate,
            message="DatabaseTaskProvider 初始化完成",
            emoji="🚀",
        )

    async def fetch_tasks(
        self,
        count: int = 10,  # 设置默认值
        priority: Optional[int] = None,
    ) -> List[SimulationTask]:
        """
        从数据库中获取任务，支持跳采样。

        参数：
        - count (int): 需要获取的任务数量，默认为 10。
        - priority (Optional[int]): 任务优先级过滤条件，默认为 None。

        返回：
        - List[SimulationTask]: 获取到的任务列表。
        """
        await logger.adebug(
            event="开始获取任务",
            count=count,
            priority=priority,
            message="fetch_tasks 方法被调用",
            emoji="🔍",
        )
        sampled_tasks: List[SimulationTask] = []

        # TODO: 这里跳采样的逻辑还是有点复杂，可能需要进一步优化
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
                    limit=count * self._sample_rate,
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
                        await logger.adebug(
                            event="一轮跳采样未能获取到足够的任务，跳采样从头开始",
                            required_task_count=count,
                            sampled_task_count=len(sampled_task_ids),
                            pending_task_count=pending_task_count,
                            emoji="🔄",
                        )
                        continue
                    await logger.awarning(
                        event="无更多任务",
                        message="数据库中没有更多待处理任务",
                        required_task_count=count,
                        sampled_task_count=len(sampled_task_ids),
                        emoji="⚠️",
                    )
                    break

                sampled_task_ids.extend(task_ids[:: self._sample_rate])
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

            await logger.ainfo(
                event="获取任务完成",
                sampled_task_id_count=len(sampled_task_ids),
                sampled_task_count=len(sampled_tasks),
                required_task_count=count,
                message="成功获取到任务",
                emoji="✅",
            )

        return sampled_tasks[:count]  # 返回满足数量的任务

    async def acknowledge_scheduled_tasks(self, task_ids: List[int]) -> None:
        """
        确认调度的任务。

        参数：
        - task_ids (List[int]): 已调度任务的 ID 列表。
        """
        await logger.adebug(
            event="确认调度任务开始",
            task_ids=task_ids,
            message="acknowledge_scheduled_tasks 方法被调用",
            emoji="📋",
        )
        async with self._lock:
            # 提交成功后，从待确认列表中移除
            self.committing_scheduled_task_ids.difference_update(task_ids)
        await logger.ainfo(
            event="确认调度任务完成",
            task_ids=task_ids,
            message="成功确认调度的任务",
            emoji="✅",
        )
