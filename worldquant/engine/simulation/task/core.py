from datetime import datetime
from typing import List, Union

from sqlalchemy import Column
from sqlalchemy.ext.asyncio import AsyncSession
from worldquant.internal.entity import (
    SimulationTask,
    SimulationTaskStatus,
    SimulationTaskType,
)
from worldquant.internal.http_api.model import SimulationSettings
from worldquant.internal.wraps import transactional


@transactional(nested_transaction=False)
async def create_simulation_task(
    session: AsyncSession,
    regular: str,
    settings: SimulationSettings,
    priority: int = 0,
) -> SimulationTask:
    task = SimulationTask(
        type=SimulationTaskType.REGULAR,
        settings=settings.__dict__,
        regular=regular,
        status=SimulationTaskStatus.PENDING,
        priority=priority,
    )
    session.add(task)
    return task


@transactional(nested_transaction=False)
async def create_simulation_tasks(
    session: AsyncSession,
    regular: List[str],
    settings: List[SimulationSettings],
    priority: List[int],
) -> List[SimulationTask]:
    if len(regular) != len(settings) or len(regular) != len(priority):
        raise ValueError("regular、settings 和 priority 的长度必须相同")

    tasks = []
    for i in range(len(regular)):
        task = SimulationTask(
            type=SimulationTaskType.REGULAR,
            settings=settings[i].__dict__,
            regular=regular[i],
            status=SimulationTaskStatus.PENDING,
            priority=priority[i],
        )
        tasks.append(task)
    session.add_all(tasks)
    return tasks


@transactional(nested_transaction=False)
async def update_simulation_task_status(
    session: AsyncSession, task_id: int, status: SimulationTaskStatus
) -> SimulationTask:
    task: Union[SimulationTask, None] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到 ID 为 {task_id} 的 task")
    elif isinstance(task, SimulationTask):
        task.status = Column[str](status.value)
        await session.merge(task)
    return task


@transactional(nested_transaction=False)
async def update_simulation_task_scheduled_time(
    session: AsyncSession, task_id: int, scheduled_at: datetime
) -> SimulationTask:
    task: Union[SimulationTask, None] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到 ID 为 {task_id} 的 task")
    elif isinstance(task, SimulationTask):
        task.scheduled_at = scheduled_at
        await session.merge(task)
    return task
