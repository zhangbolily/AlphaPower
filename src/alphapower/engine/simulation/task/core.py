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

from alphapower.client import SimulationSettingsView
from alphapower.constants import AlphaType
from alphapower.dal.simulation import SimulationTaskDAL
from alphapower.entity import SimulationTask, SimulationTaskStatus

from sqlalchemy.ext.asyncio import AsyncSession


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
        "region": str(settings.region) or "None",
        "delay": str(settings.delay) if settings.delay is not None else "None",
        "language": str(settings.language) or "None",
        "instrument_type": str(settings.instrument_type) or "None",
        "universe": str(settings.universe) or "None",
        "truncation": (
            str(settings.truncation) if settings.truncation is not None else "None"
        ),
        "unit_handling": str(settings.unit_handling) or "None",
        "test_period": str(settings.test_period) or "None",
        "pasteurization": str(settings.pasteurization) or "None",
        "decay": str(settings.decay) if settings.decay is not None else "None",
        "neutralization": str(settings.neutralization) or "None",
        "visualization": (
            str(settings.visualization)
            if settings.visualization is not None
            else "False"
        ),
        "max_trade": str(settings.max_trade) or "None",
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
    tags: Optional[List[str]] = None,
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
        type=AlphaType.REGULAR,
        settings_group_key=settings_group_key,
        signature=signature,
        regular=regular,
        status=status,
        priority=priority,
        region=settings.region,
        delay=settings.delay,
        language=settings.language,
        instrument_type=settings.instrument_type,
        universe=settings.universe,
        truncation=settings.truncation,
        unit_handling=settings.unit_handling,
        test_period=settings.test_period,
        pasteurization=settings.pasteurization,
        decay=settings.decay,
        neutralization=settings.neutralization,
        visualization=(
            settings.visualization if settings.visualization is not None else False
        ),
        max_trade=settings.max_trade,
        tags=tags or [],
    )


async def create_simulation_task(
    session: AsyncSession,
    regular: str,
    settings: SimulationSettingsView,
    priority: int = 0,
    tags: Optional[List[str]] = None,
) -> SimulationTask:
    """
    创建单个SimulationTask并保存到数据库，使用SimulationTaskDAL。
    """
    dal = SimulationTaskDAL(session)
    signature = get_task_signature(regular, settings)
    task = _create_task(
        regular=regular,
        settings=settings,
        settings_group_key="",
        signature=signature,
        status=SimulationTaskStatus.PENDING,
        priority=priority,
        tags=tags,
    )
    task = await dal.create(task)
    return task


async def create_simulation_tasks(
    session: AsyncSession,
    regular: List[str],
    settings: List[SimulationSettingsView],
    priority: List[int],
    tags_list: List[Optional[List[str]]],
) -> List[SimulationTask]:
    """
    批量创建SimulationTask，并使用SimulationTaskDAL将其保存到数据库。
    """
    dal = SimulationTaskDAL()
    if len(regular) != len(settings) or len(regular) != len(priority):
        raise ValueError("regular、settings和priority的长度必须相同")

    tasks = [
        _create_task(
            regular=regular[i],
            settings=settings[i],
            settings_group_key="",
            signature=get_task_signature(regular[i], settings[i]),
            status=SimulationTaskStatus.PENDING,
            priority=priority[i],
            tags=tags_list[i],
        )
        for i in range(len(regular))
    ]
    tasks = await dal.bulk_create(tasks, session=session)
    return tasks


async def update_simulation_task_status(
    session: AsyncSession, task_id: int, status: SimulationTaskStatus
) -> SimulationTask:
    """
    更新指定任务的状态，使用SimulationTaskDAL。
    """
    dal = SimulationTaskDAL(session)
    task = await dal.find_one_by(id=task_id)
    if task is None:
        raise ValueError(f"找不到ID为{task_id}的任务")
    task.status = status
    task = await dal.update(task)
    return task


async def update_simulation_task_scheduled_info(
    session: AsyncSession,
    task_id: int,
    scheduled_at: datetime,
    status: SimulationTaskStatus,
) -> SimulationTask:
    """
    更新指定任务的调度信息，使用SimulationTaskDAL。
    """
    dal = SimulationTaskDAL(session)
    task = await dal.find_one_by(id=task_id)
    if task is None:
        raise ValueError(f"找不到ID为{task_id}的任务")
    task.scheduled_at = scheduled_at
    task.status = status
    task = await dal.update(task)
    return task


async def get_simulation_task_by_id(
    session: AsyncSession, task_id: int
) -> SimulationTask:
    """
    通过DAL查询指定ID的任务。
    """
    dal = SimulationTaskDAL(session)
    task = await dal.find_one_by(id=task_id)
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
    """
    通过DAL多条件查询模拟任务列表。
    """
    dal = SimulationTaskDAL(session)
    return await dal.find_filtered(
        status=status,
        priority=priority,
        not_in_=not_in_,
        in_=in_,
        limit=limit,
        offset=offset,
    )
