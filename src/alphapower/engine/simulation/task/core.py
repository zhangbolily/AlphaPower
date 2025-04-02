"""模拟任务管理模块。

该模块提供了创建、查询和更新模拟任务的功能。它包括任务签名生成、设置组键创建以及
数据库交互等核心功能，用于管理模拟任务的完整生命周期。

典型用法：
  async with AsyncSession(engine) as session:
      task = await create_simulation_task(
          session=session,
          regular="my_regular",
          settings=my_settings,
          priority=1
      )
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from alphapower.client import SimulationSettingsView
from alphapower.entity import (
    SimulationTask,
    SimulationTaskStatus,
    SimulationTaskType,
)


def get_settings_group_key(settings: SimulationSettingsView) -> str:
    """生成唯一的设置组键。

    基于模拟设置中的区域、延迟、语言和工具类型生成一个唯一标识符。

    Args:
        settings: 包含模拟配置的设置对象。

    Returns:
        一个字符串，作为设置组的唯一键。
    """
    return f"{settings.region}_{settings.delay}_{settings.language}_{settings.instrument_type}"


def get_task_signature(regular: str, settings: SimulationSettingsView) -> str:
    """生成任务签名，用于唯一标识任务。

    基于regular字符串和模拟设置生成MD5哈希值，用作任务的唯一标识符。

    Args:
        regular: 任务的常规标识符。
        settings: 包含模拟配置的设置对象。

    Returns:
        一个MD5哈希字符串，作为任务的唯一签名。
    """
    settings_dict = {
        "region": settings.region or "None",
        "delay": settings.delay or "None",
        "language": settings.language or "None",
        "instrument_type": settings.instrument_type or "None",
        "nan_handling": settings.nan_handling or "None",
        "universe": settings.universe or "None",
        "truncation": settings.truncation or "None",
        "unit_handling": settings.unit_handling or "None",
        "test_period": settings.test_period or "None",
        "pasteurization": settings.pasteurization or "None",
        "decay": settings.decay or "None",
        "neutralization": settings.neutralization or "None",
        "visualization": settings.visualization or "None",
        "max_trade": settings.max_trade or "None",
    }
    settings_str = json.dumps(settings_dict, sort_keys=True)
    return hashlib.md5(f"{regular}_{settings_str}".encode("utf-8")).hexdigest()


def _create_task(
    regular: str,
    settings: SimulationSettingsView,
    settings_group_key: str,
    signature: str,
    status: SimulationTaskStatus,
    priority: int,
) -> SimulationTask:
    """创建单个SimulationTask对象。

    Args:
        regular: 任务的常规标识符。
        settings: 包含模拟配置的设置对象。
        settings_group_key: 设置组的唯一键。
        signature: 任务的唯一签名。
        status: 任务的初始状态。
        priority: 任务的优先级。

    Returns:
        一个新创建的SimulationTask实例。
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


async def create_simulation_task(
    session: AsyncSession,
    regular: str,
    settings: SimulationSettingsView,
    priority: int = 0,
) -> SimulationTask:
    """创建单个SimulationTask并保存到数据库。

    Args:
        session: 数据库会话对象。
        regular: 任务的常规标识符。
        settings: 包含模拟配置的设置对象。
        priority: 任务的优先级，默认为0。

    Returns:
        一个新创建的SimulationTask实例。
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


async def create_simulation_tasks(
    session: AsyncSession,
    regular: List[str],
    settings: List[SimulationSettingsView],
    priority: List[int],
) -> List[SimulationTask]:
    """批量创建SimulationTask并保存到数据库。

    Args:
        session: 数据库会话对象。
        regular: 任务的常规标识符列表。
        settings: 包含模拟配置的设置对象列表。
        priority: 任务的优先级列表。

    Returns:
        一个包含新创建的SimulationTask实例的列表。

    Raises:
        ValueError: 如果regular、settings和priority的长度不相同。
    """
    if len(regular) != len(settings) or len(regular) != len(priority):
        raise ValueError("regular、settings和priority的长度必须相同")

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


async def update_simulation_task_status(
    session: AsyncSession, task_id: int, status: SimulationTaskStatus
) -> SimulationTask:
    """更新指定任务的状态。

    Args:
        session: 数据库会话对象。
        task_id: 任务的唯一标识符。
        status: 任务的新状态。

    Returns:
        更新后的SimulationTask实例。

    Raises:
        ValueError: 如果找不到指定ID的任务。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到ID为{task_id}的任务")
    task.status = status
    await session.merge(task)
    return task


async def update_simulation_task_scheduled_info(
    session: AsyncSession,
    task_id: int,
    scheduled_at: datetime,
    status: SimulationTaskStatus,
) -> SimulationTask:
    """更新指定任务的调度信息。

    Args:
        session: 数据库会话对象。
        task_id: 任务的唯一标识符。
        scheduled_at: 任务的调度时间。
        status: 任务的新状态。

    Returns:
        更新后的SimulationTask实例。

    Raises:
        ValueError: 如果找不到指定ID的任务。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到ID为{task_id}的任务")
    task.scheduled_at = scheduled_at
    task.status = status
    await session.merge(task)
    return task


async def get_simulation_task_by_id(
    session: AsyncSession, task_id: int
) -> SimulationTask:
    """根据任务ID获取任务。

    Args:
        session: 数据库会话对象。
        task_id: 任务的唯一标识符。

    Returns:
        对应的SimulationTask实例。

    Raises:
        ValueError: 如果找不到指定ID的任务。
    """
    task: Optional[SimulationTask] = await session.get(SimulationTask, task_id)
    if task is None:
        raise ValueError(f"找不到ID为{task_id}的任务")
    return task


async def get_simulation_tasks_by(
    session: AsyncSession,
    status: Optional[SimulationTaskStatus] = None,
    priority: Optional[int] = None,
    not_in_: Optional[Dict[str, List[int]]] = None,
    in_: Optional[Dict[str, List[int]]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[SimulationTask]:
    """根据条件从数据库中获取SimulationTask列表。

    Args:
        session: 数据库会话对象。
        status: 任务的状态过滤条件。
        priority: 任务的优先级过滤条件。
        not_in_: 不包含的字段值过滤条件。
        in_: 包含的字段值过滤条件。
        limit: 返回结果的最大数量。
        offset: 返回结果的偏移量。

    Returns:
        一个包含符合条件的SimulationTask实例的列表。

    Raises:
        ValueError: 如果没有提供任何过滤条件或字段名无效。
    """
    filters = []
    if status is not None:
        filters.append(SimulationTask.status == status)
    if priority is not None:
        filters.append(SimulationTask.priority == priority)
    if not_in_ is not None:
        for key, values in not_in_.items():
            if hasattr(SimulationTask, key):
                column = getattr(SimulationTask, key)
                filters.append(column.notin_(values))
            else:
                raise ValueError(f"无效的字段名: {key}")
    if in_ is not None:
        for key, values in in_.items():
            if hasattr(SimulationTask, key):
                column = getattr(SimulationTask, key)
                filters.append(column.in_(values))
            else:
                raise ValueError(f"无效的字段名: {key}")

    if not filters:
        raise ValueError("至少需要一个过滤条件")

    query = select(SimulationTask).filter(*filters)
    if limit is not None:
        query = query.limit(limit)
    if offset is not None:
        query = query.offset(offset)
    result = await session.execute(query)
    return list(result.scalars().all())
