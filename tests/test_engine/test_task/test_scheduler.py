import asyncio
from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.engine.simulation.task import (
    DatabaseTaskProvider,
    PriorityScheduler,
    create_simulation_tasks,
)
from alphapower.internal.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.http_api.model import SimulationSettings
from alphapower.internal.wraps import with_session


@pytest.mark.asyncio
async def test_add_task():
    scheduler = PriorityScheduler()
    task = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)

    scheduler.add_tasks([task])

    assert len(scheduler.tasks) == 1
    assert scheduler.settings_group_map["group_1"] == [task]


@pytest.mark.asyncio
async def test_fetch_tasks_from_provider():
    task_provider = AsyncMock()
    task_provider.fetch_tasks.return_value = [
        MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    ]
    scheduler = PriorityScheduler(task_provider=task_provider)

    await scheduler.fetch_tasks_from_provider()

    assert len(scheduler.tasks) == 1
    task_provider.fetch_tasks.assert_called_once_with(count=1)


@pytest.mark.asyncio
async def test_has_tasks():
    scheduler = PriorityScheduler()
    assert not await scheduler.has_tasks()

    task = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    scheduler.add_tasks([task])

    assert await scheduler.has_tasks()


@pytest.mark.asyncio
async def test_schedule_single_task():
    task1 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_1",
        priority=10,
        status=SimulationTaskStatus.PENDING,
    )
    task2 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_1",
        priority=20,
        status=SimulationTaskStatus.PENDING,
    )
    scheduler = PriorityScheduler(tasks=[task1, task2])

    scheduled_tasks = await scheduler.schedule(batch_size=1)

    assert len(scheduled_tasks) == 1
    assert scheduled_tasks[0] == task2  # 优先级最高的任务（数字越大优先级越高）
    assert len(scheduler.tasks) == 1


@pytest.mark.asyncio
async def test_schedule_batch_tasks():
    task1 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_1",
        priority=10,
        status=SimulationTaskStatus.PENDING,
    )
    task2 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_1",
        priority=20,
        status=SimulationTaskStatus.PENDING,
    )
    task3 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_2",
        priority=30,
        status=SimulationTaskStatus.PENDING,
    )
    task4 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_2",
        priority=40,
        status=SimulationTaskStatus.PENDING,
    )
    task5 = MagicMock(
        spec=SimulationTask,
        settings_group_key="group_3",
        priority=50,
        status=SimulationTaskStatus.PENDING,
    )
    scheduler: PriorityScheduler = PriorityScheduler(
        tasks=[task1, task2, task3, task4, task5]
    )

    prev_priority: int = int(50)
    while await scheduler.has_tasks():
        scheduled_tasks = await scheduler.schedule(batch_size=3)
        assert len(scheduled_tasks) > 0
        assert len(scheduled_tasks) <= 3
        assert scheduled_tasks[0].priority <= prev_priority
        prev_priority = int(scheduled_tasks[0].priority)
        assert all(
            task.priority <= scheduled_tasks[0].priority for task in scheduled_tasks
        )
        assert all(
            task.settings_group_key == scheduled_tasks[0].settings_group_key
            for task in scheduled_tasks
        )


@pytest.mark.asyncio
@with_session("simulation_test")
async def test_schedule_with_database_task_provider(session: AsyncSession):
    # 准备测试数据
    regular = ["task1", "task2"]
    settings: List[SimulationSettings] = [
        SimulationSettings(
            region="USA", delay=10, language="FASTEXPRESS", instrumentType="type1"
        ),
        SimulationSettings(
            region="CN", delay=20, language="FASTEXPRESS", instrumentType="type2"
        ),
    ]
    priority = [1, 1]

    # 向数据库插入测试数据
    await create_simulation_tasks(session, regular, settings, priority)
    await session.commit()

    # 初始化 DatabaseTaskProvider
    provider = DatabaseTaskProvider(session=session)
    scheduler = PriorityScheduler(task_provider=provider, task_fetch_size=10)

    # 调用 schedule 方法
    scheduled_tasks = await scheduler.schedule(batch_size=2)
    # 验证返回值
    await scheduler.wait_for_post_async_tasks()

    # 验证任务状态和其他属性
    for _, task in enumerate(scheduled_tasks):
        assert task.status == SimulationTaskStatus.SCHEDULED
        assert task.priority == 1

    await session.execute(text(f"DELETE FROM {SimulationTask.__tablename__}"))
    await session.commit()
