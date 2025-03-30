from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text  # 添加导入
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.engine.simulation.task.core import create_simulation_tasks
from alphapower.engine.simulation.task.provider import DatabaseTaskProvider
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.client import SimulationSettingsView
from alphapower.internal.wraps import with_session


@pytest.mark.asyncio  # 确保每个异步测试函数都使用了 pytest.mark.asyncio
@with_session("simulation_test")
async def test_fetch_tasks(session: AsyncSession) -> None:
    """
    测试从数据库中获取任务的功能。
    """
    # 准备测试数据
    regular = ["task1", "task2"]
    settings = [
        SimulationSettingsView.model_construct(
            region="region1", delay=10, language="en", instrumentType="type1"
        ),
        SimulationSettingsView.model_construct(
            region="region2", delay=20, language="cn", instrumentType="type2"
        ),
    ]
    priority = [1, 1]

    # 向数据库插入测试数据
    await create_simulation_tasks(session, regular, settings, priority)
    await session.commit()

    # 初始化 DatabaseTaskProvider
    provider = DatabaseTaskProvider(session=session)

    # 调用 fetch_tasks 方法
    tasks = await provider.fetch_tasks(count=2, priority=1)

    # 验证返回值
    assert len(tasks) == 2
    assert tasks[0].regular == "task1"
    assert tasks[1].regular == "task2"

    # 验证任务状态和其他属性
    for i, task in enumerate(tasks):
        assert task.status == SimulationTaskStatus.PENDING
        assert task.priority == 1
        assert task.settings["region"] == settings[i].region
        assert task.settings["delay"] == settings[i].delay
        assert task.settings["language"] == settings[i].language
        assert task.settings["instrument_type"] == settings[i].instrument_type

    # 清理测试数据
    await session.execute(text(f"DELETE FROM {SimulationTask.__tablename__}"))
    await session.commit()


@pytest.mark.asyncio
@with_session("simulation_test")
async def test_fetch_tasks_no_results(session: AsyncSession) -> None:
    """
    测试从数据库中获取任务的功能，当没有符合条件的任务时。
    """
    # 初始化 DatabaseTaskProvider
    provider = DatabaseTaskProvider(session=session)

    # 调用 fetch_tasks 方法，期望无结果
    tasks = await provider.fetch_tasks(count=2, priority=10000000)

    # 验证返回值为空
    assert len(tasks) == 0


@pytest.mark.asyncio
@with_session("simulation_test")
async def test_fetch_tasks_invalid_priority(session: AsyncSession) -> None:
    """
    测试从数据库中获取任务的功能，当使用不存在的优先级时。
    """
    # 准备测试数据
    regular = ["task1"]
    settings = [
        SimulationSettingsView.model_construct(
            region="region1", delay=10, language="en", instrumentType="type1"
        )
    ]
    priority = [1]

    # 向数据库插入测试数据
    await create_simulation_tasks(session, regular, settings, priority)
    await session.commit()

    # 初始化 DatabaseTaskProvider
    provider = DatabaseTaskProvider(session=session)

    # 调用 fetch_tasks 方法，使用不存在的优先级
    tasks = await provider.fetch_tasks(count=1, priority=99)

    # 验证返回值为空
    assert len(tasks) == 0

    # 清理测试数据
    await session.execute(text(f"DELETE FROM {SimulationTask.__tablename__}"))
    await session.commit()


@pytest.mark.asyncio
@patch(
    "alphapower.engine.simulation.task.provider.DatabaseTaskProvider.fetch_tasks",
    side_effect=Exception("Database error"),
)
async def test_fetch_tasks_exception(mock_fetch_tasks: AsyncMock) -> None:
    """
    测试从数据库中获取任务的功能，当发生异常时。
    """
    # 模拟异常情况
    provider = DatabaseTaskProvider(session=AsyncMock())

    # 验证异常抛出
    with pytest.raises(Exception, match="Database error"):
        await provider.fetch_tasks(count=1, priority=1)
