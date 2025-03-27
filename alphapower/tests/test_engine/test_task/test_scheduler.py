from unittest.mock import AsyncMock, MagicMock

import pytest
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.internal.entity import SimulationTask


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
    task1 = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    task2 = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=20)
    scheduler = PriorityScheduler(tasks=[task1, task2])

    scheduled_tasks = await scheduler.schedule(batch_size=1)

    assert len(scheduled_tasks) == 1
    assert scheduled_tasks[0] == task2  # 优先级最高的任务（数字越大优先级越高）
    assert len(scheduler.tasks) == 1


@pytest.mark.asyncio
async def test_schedule_batch_tasks():
    task1 = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    task2 = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=20)
    task3 = MagicMock(spec=SimulationTask, settings_group_key="group_2", priority=30)
    task4 = MagicMock(spec=SimulationTask, settings_group_key="group_2", priority=40)
    task5 = MagicMock(spec=SimulationTask, settings_group_key="group_3", priority=50)
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
