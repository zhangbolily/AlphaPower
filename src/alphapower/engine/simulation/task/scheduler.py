"""
任务调度器模块，提供任务调度器的实现。
"""

import asyncio
from bisect import insort
from typing import Dict, List, Optional

from structlog.stdlib import BoundLogger

from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.logging import get_logger

from .provider_abc import AbstractTaskProvider
from .scheduler_abc import AbstractScheduler

logger: BoundLogger = get_logger(__name__)


class PriorityScheduler(AbstractScheduler):
    """
    优先级调度器，根据任务的优先级调度任务。
    """

    def __init__(
        self,
        tasks: Optional[List[SimulationTask]] = None,
        task_provider: Optional[AbstractTaskProvider] = None,
        task_fetch_size: int = 1,
        low_priority_threshold: int = 10,  # 新增低优先级任务阈值
    ):
        """
        初始化调度器，接收任务列表或任务提供者。
        :param tasks: SimulationTask 的列表（可选）
        :param task_provider: 一个可调用对象，用于从数据库或其他数据源获取任务（可选）
        """
        self.tasks: List[SimulationTask] = sorted(
            tasks or [], key=lambda task: -int(task.priority)
        )
        self.task_provider: Optional[AbstractTaskProvider] = task_provider
        self.settings_group_map: Dict[str, List[SimulationTask]] = {}  # 新增映射关系
        self.task_fetch_size: int = task_fetch_size
        self.low_priority_threshold: int = low_priority_threshold  # 保存阈值
        self.low_priority_counter: Dict[str, int] = {}  # 记录低优先级任务的调度次数

        self._post_async_tasks: List[asyncio.Task] = []  # 保存后续异步任务
        self._post_async_tasks_lock: asyncio.Lock = asyncio.Lock()

        # 初始化 settings_group_map 映射关系
        for task in self.tasks:
            group_key: str = str(task.settings_group_key)
            if group_key not in self.settings_group_map:
                self.settings_group_map[group_key] = []
            self.settings_group_map[group_key].append(task)

    def __del__(self) -> None:
        """
        析构函数，用于清理后续异步任务。
        """
        for task in self._post_async_tasks:
            if not task.done():
                task.cancel()
        self._post_async_tasks.clear()

    async def fetch_tasks_from_provider(self) -> None:
        """
        从任务提供者获取任务并添加到任务列表，同时更新映射关系。
        """
        if self.task_provider:
            new_tasks: List[SimulationTask] = await self.task_provider.fetch_tasks(
                count=self.task_fetch_size
            )
            if new_tasks:
                await logger.adebug(
                    event="从任务提供者获取新任务",
                    new_task_count=len(new_tasks),
                    message="成功从任务提供者获取新任务",
                    emoji="📥",
                )
                self.add_tasks(new_tasks)  # 使用 add_tasks 确保映射关系更新

    def add_tasks(self, tasks: List[SimulationTask]) -> None:
        """
        批量添加新任务到调度器，并保持任务列表和分组任务的有序性。
        :param tasks: SimulationTask 对象的列表
        """
        for task in tasks:
            insort(self.tasks, task, key=lambda t: -int(t.priority))  # 插入时保持有序
            group_key: str = str(task.settings_group_key)
            if group_key not in self.settings_group_map:
                self.settings_group_map[group_key] = []
            insort(
                self.settings_group_map[group_key], task, key=lambda t: -int(t.priority)
            )  # 插入时保持有序

    def remove_task(self, task: SimulationTask) -> None:
        """
        从调度器中移除一个任务，并更新 settings_group_key 映射关系。
        """
        group_key: str = str(task.settings_group_key)
        if group_key in self.settings_group_map:
            self.settings_group_map[group_key].remove(task)
            if not self.settings_group_map[group_key]:
                del self.settings_group_map[group_key]
        self.tasks.remove(task)

    async def has_tasks(self) -> bool:
        """
        检查是否还有任务待调度。
        :return: 如果有任务返回 True，否则返回 False
        """
        if not self.tasks:
            await self.fetch_tasks_from_provider()
        return len(self.tasks) > 0

    def set_task_provider(self, task_provider: AbstractTaskProvider) -> None:
        """
        设置任务提供者。
        :param task_provider: AbstractTaskProvider 对象
        """
        self.task_provider = task_provider

    def promote_low_priority_tasks(self) -> None:
        """
        提升低优先级任务的优先级，防止饥饿。
        """
        for group_key, tasks in self.settings_group_map.items():
            if group_key not in self.low_priority_counter:
                self.low_priority_counter[group_key] = 0

            # 如果低优先级任务的调度次数超过阈值，提升其优先级
            if self.low_priority_counter[group_key] >= self.low_priority_threshold:
                for task in tasks:
                    task.priority += 1  # 提升优先级
                self.settings_group_map[group_key] = sorted(
                    tasks, key=lambda t: -int(t.priority)
                )  # 重新排序
                self.low_priority_counter[group_key] = 0  # 重置计数器
                logger.info(
                    event="提升低优先级任务",
                    group_key=group_key,
                    promoted_task_count=len(tasks),
                    message="低优先级任务已提升优先级",
                    emoji="⬆️",
                )

    async def _do_schedule(self, batch_size: int) -> List[SimulationTask]:
        """
        调度任务，支持单个任务或批量任务。
        :param batch_size: 批量任务的大小
        :return: SimulationTask 对象的列表
        """
        if not self.tasks:
            return []

        if batch_size == 1:
            # 返回单个任务
            task: SimulationTask = self.tasks[0]
            self.remove_task(task)  # 确保映射关系更新
            return [task]

        # 获取第一个任务的 settings_group_key
        first_task: SimulationTask = self.tasks[0]
        target_group_key: str = str(first_task.settings_group_key)

        # 从映射关系中获取属于同一 settings_group_key 的任务
        batch: List[SimulationTask] = self.settings_group_map[target_group_key][
            :batch_size
        ]
        for task in batch:
            self.remove_task(task)  # 使用 remove_task 确保映射关系更新

        # 更新低优先级任务的调度计数
        self.low_priority_counter[target_group_key] += 1

        return batch

    async def _before_schedule(self) -> None:
        """
        调度前的处理，用于检查是否有任务待调度。
        """
        if not self.tasks:
            await self.fetch_tasks_from_provider()

        # 调度前检查并提升低优先级任务
        self.promote_low_priority_tasks()

    async def _post_schedule(
        self, scheduled_tasks: Optional[List[SimulationTask]]
    ) -> None:
        """
        调度后的处理，用于确认调度的任务。
        :param scheduled_tasks: SimulationTask 对象的列表
        """
        if scheduled_tasks:
            for task in scheduled_tasks:
                if task.status != SimulationTaskStatus.PENDING:
                    raise ValueError(f"任务状态 {task.status} 错误，无法调度。")
                task.status = SimulationTaskStatus.SCHEDULED

        if self.task_provider and scheduled_tasks:
            task_ids: List[int] = [task.id for task in scheduled_tasks]

            async with self._post_async_tasks_lock:
                for post_async_task in self._post_async_tasks:
                    if post_async_task.done():
                        self._post_async_tasks.remove(post_async_task)

                self._post_async_tasks.append(
                    asyncio.create_task(
                        self.task_provider.acknowledge_scheduled_tasks(task_ids)
                    )
                )
        await logger.ainfo(
            event="调度任务完成",
            scheduled_task_count=len(scheduled_tasks) if scheduled_tasks else 0,
            message="任务调度完成",
            emoji="✅",
        )

    async def wait_for_post_async_tasks(self) -> None:
        """
        等待后续异步任务完成。
        """
        async with self._post_async_tasks_lock:
            result = await asyncio.gather(
                *self._post_async_tasks, return_exceptions=True
            )
            for task in result:
                if isinstance(task, Exception):
                    raise task
            self._post_async_tasks.clear()

    async def schedule(self, batch_size: int = 1) -> List[SimulationTask]:
        """
        调度任务，支持单个任务或批量任务。
        :param batch_size: 批量任务的大小，默认为 1
        :return: SimulationTask 对象的列表
        """
        await self._before_schedule()
        scheduled_tasks: List[SimulationTask] = await self._do_schedule(batch_size)
        await self._post_schedule(scheduled_tasks)

        return scheduled_tasks
