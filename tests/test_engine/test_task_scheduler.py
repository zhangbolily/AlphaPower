"""
测试调度器的功能。
"""

import asyncio
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text

from alphapower.client import SimulationSettingsView
from alphapower.constants import (
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.engine.simulation.task import (
    DatabaseTaskProvider,
    PriorityScheduler,
    create_simulation_tasks,
)
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session
from tests.mocks.mock_task_worker import MockWorker


@pytest.mark.asyncio
async def test_add_task() -> None:
    scheduler = PriorityScheduler()
    task = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)

    scheduler.add_tasks([task])

    assert len(scheduler.tasks) == 1
    assert scheduler.settings_group_map["group_1"] == [task]


@pytest.mark.asyncio
async def test_fetch_tasks_from_provider() -> None:
    task_provider = AsyncMock()
    task_provider.fetch_tasks.return_value = [
        MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    ]
    scheduler = PriorityScheduler(task_provider=task_provider)

    await scheduler.fetch_tasks_from_provider()

    assert len(scheduler.tasks) == 1
    task_provider.fetch_tasks.assert_called_once_with(count=1)


@pytest.mark.asyncio
async def test_has_tasks() -> None:
    scheduler = PriorityScheduler()
    assert not await scheduler.has_tasks()

    task = MagicMock(spec=SimulationTask, settings_group_key="group_1", priority=10)
    scheduler.add_tasks([task])

    assert await scheduler.has_tasks()


@pytest.mark.asyncio
async def test_schedule_single_task() -> None:
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
async def test_schedule_batch_tasks() -> None:
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
async def test_schedule_with_database_task_provider() -> None:
    """
    测试使用数据库任务提供者的调度功能。
    """
    async with get_db_session(Database.SIMULATION) as session:
        # 准备测试数据
        regular = ["task1", "task2"]
        settings: List[SimulationSettingsView] = [
            SimulationSettingsView.model_construct(
                region=Region.USA.name,
                delay=Delay.ONE.value,
                language=RegularLanguage.FASTEXPR.value,
                instrument_type=InstrumentType.EQUITY.value,
                universe=Universe.TOP1000.value,
                neutralization=Neutralization.INDUSTRY.value,
                pasteurization=Switch.ON.value,
                unit_handling=UnitHandling.VERIFY.value,
                max_trade=Switch.OFF.value,
                decay=10,
                truncation=0.5,
                visualization=False,
                test_period="2020-01-01:2021-01-01",
            ),
            SimulationSettingsView.model_construct(
                region=Region.CHN.name,
                delay=Delay.ONE.value,
                language=RegularLanguage.FASTEXPR.value,
                instrument_type=InstrumentType.EQUITY.value,
                universe=Universe.TOP2000U.value,
                neutralization=Neutralization.INDUSTRY.value,
                pasteurization=Switch.ON.value,
                unit_handling=UnitHandling.VERIFY.value,
                max_trade=Switch.OFF.value,
                decay=10,
                truncation=0.5,
                visualization=False,
                test_period="2020-01-01:2021-01-01",
            ),
        ]
        priority = [1, 1]
        tags_list: List[Optional[List[str]]] = [
            ["tag1", "tag2"],
            ["tag3", "tag4"],
        ]

        # 向数据库插入测试数据
        await create_simulation_tasks(session, regular, settings, priority, tags_list)
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


async def test_scheduler_with_mock_task_worker() -> None:
    """
    测试调度器与Mock工作者的集成。
    """

    scheduler = PriorityScheduler()
    worker = MockWorker(work_time=1, job_slots=2)
    await worker.set_scheduler(scheduler)

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
    scheduler.add_tasks([task1, task2])

    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(3)  # 等待工作者开始运行
    await worker.stop(cancel_tasks=False)  # 停止工作者
    await worker_task

    assert task1.status == SimulationTaskStatus.COMPLETE
    assert task2.status == SimulationTaskStatus.COMPLETE
