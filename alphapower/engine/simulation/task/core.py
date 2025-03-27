import hashlib
import json
from datetime import datetime
from typing import List, Optional, Union

from alphapower.internal.entity import (
    SimulationTask,
    SimulationTaskStatus,
    SimulationTaskType,
)
from alphapower.internal.http_api.model import SimulationSettings
from alphapower.internal.wraps import transactional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select


def get_settings_group_key(settings: SimulationSettings) -> str:
    """
    根据 SimulationSettings 生成唯一的设置组键。
    """
    return f"{settings.region}_{settings.delay}_{settings.language}_{settings.instrumentType}"


def get_task_signature(regular: str, settings: SimulationSettings) -> str:
    """
    生成任务签名，用于唯一标识任务。
    """
    settings_dict = {
        "region": settings.region or "None",
        "delay": settings.delay or "None",
        "language": settings.language or "None",
        "instrumentType": settings.instrumentType or "None",
        "nanHandling": settings.nanHandling or "None",
        "universe": settings.universe or "None",
        "truncation": settings.truncation or "None",
        "unitHandling": settings.unitHandling or "None",
        "testPeriod": settings.testPeriod or "None",
        "pasteurization": settings.pasteurization or "None",
        "decay": settings.decay or "None",
        "neutralization": settings.neutralization or "None",
        "visualization": settings.visualization or "None",
        "maxTrade": settings.maxTrade or "None",
    }
    settings_str = json.dumps(settings_dict, sort_keys=True)
    return hashlib.md5(f"{regular}_{settings_str}".encode("utf-8")).hexdigest()


def _create_task(
    regular: str,
    settings: SimulationSettings,
    settings_group_key: str,
    signature: str,
    status: SimulationTaskStatus,
    priority: int,
) -> SimulationTask:
    """
    辅助函数：创建单个 SimulationTask 对象。
    """
    return SimulationTask(
        type=SimulationTaskType.REGULAR,
        settings=settings.__dict__,
        settings_group_key=settings_group_key,
        signature=signature,
        regular=regular,
        status=status,
        priority=priority,
    )


@transactional(nested_transaction=False)
async def create_simulation_task(
    session: AsyncSession,
    regular: str,
    settings: SimulationSettings,
    priority: int = 0,
) -> SimulationTask:
    """
    创建单个 SimulationTask 并保存到数据库。
    """
    settings_group_key = get_settings_group_key(settings)
    signature = get_task_signature(regular, settings)
    task = _create_task(
        regular=regular,
        settings=settings,
        settings_group_key=settings_group_key,
        signature=signature,
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
    """
    批量创建 SimulationTask 并保存到数据库。
    """
    if len(regular) != len(settings) or len(regular) != len(priority):
        raise ValueError("regular、settings 和 priority 的长度必须相同")

    tasks = [
        _create_task(
            regular=regular[i],
            settings=settings[i],
            settings_group_key=get_settings_group_key(settings[i]),
            signature=get_task_signature(regular[i], settings[i]),
            status=SimulationTaskStatus.PENDING,
            priority=priority[i],
        )
        for i in range(len(regular))
    ]
    session.add_all(tasks)
    return tasks


@transactional(nested_transaction=False)
async def update_simulation_task_status(
    session: AsyncSession, task_id: int, status: SimulationTaskStatus
) -> SimulationTask:
    """
    更新指定任务的状态。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到 ID 为 {task_id} 的任务")
    task.status = status
    await session.merge(task)
    return task


@transactional(nested_transaction=False)
async def update_simulation_task_scheduled_time(
    session: AsyncSession, task_id: int, scheduled_at: datetime
) -> SimulationTask:
    """
    更新指定任务的调度时间。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到 ID 为 {task_id} 的任务")
    task.scheduled_at = scheduled_at
    await session.merge(task)
    return task


async def get_simulation_task_by_id(
    session: AsyncSession, task_id: int
) -> SimulationTask:
    """
    根据任务 ID 获取任务。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到 ID 为 {task_id} 的任务")
    return task


async def get_simulation_tasks_by(
    session: AsyncSession,
    status: Optional[SimulationTaskStatus] = None,
    priority: Optional[int] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[SimulationTask]:
    """
    根据条件从数据库中获取 SimulationTask 列表。
    """
    filters = []
    if status is not None:
        filters.append(SimulationTask.status == status)
    if priority is not None:
        filters.append(SimulationTask.priority == priority)

    query = select(SimulationTask).filter(*filters)
    if limit is not None:
        query = query.limit(limit)
    if offset is not None:
        query = query.offset(offset)
    result = await session.execute(query)
    return list(result.scalars().all())
