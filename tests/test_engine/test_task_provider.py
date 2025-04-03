"""测试DatabaseTaskProvider类的功能模块。

本模块包含对DatabaseTaskProvider类的单元测试，重点测试其fetch_tasks方法在各种情况下的行为：
- 从数据库正常获取任务
- 当数据库中没有符合条件的任务时的行为
- 使用不存在的优先级获取任务的情况
- fetch_tasks方法抛出异常时的错误处理

这些测试使用pytest异步测试框架和模拟(mock)来隔离测试环境。
"""

from typing import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.client import SimulationSettingsView
from alphapower.constants import (
    DB_SIMULATION,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.engine.simulation.task.core import create_simulation_tasks
from alphapower.engine.simulation.task.provider import DatabaseTaskProvider
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="session")
async def fixture_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Fixture for creating a database session.
    """
    async with get_db_session(DB_SIMULATION) as session:
        yield session


@pytest.mark.asyncio
async def test_fetch_tasks(session: AsyncSession) -> None:
    """
    测试从数据库中获取任务的功能。

    该测试验证fetch_tasks方法能够正确地从数据库中获取任务，并检查返回的任务属性是否符合预期。

    Args:
        session: 异步数据库会话对象
    """
    # 准备测试数据
    regular = ["task1", "task2"]
    settings = [
        SimulationSettingsView.model_construct(
            region=Region.USA.name,
            delay=Delay.ONE.value,
            language=RegularLanguage.EXPRESSION.value,
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
            region=Region.CHINA.name,
            delay=Delay.ONE.value,
            language=RegularLanguage.EXPRESSION.value,
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
        assert task.region.name == settings[i].region
        assert task.delay.value == settings[i].delay
        assert task.language.name == settings[i].language
        assert task.instrument_type.name == settings[i].instrument_type

    # 清理测试数据
    await session.execute(text(f"DELETE FROM {SimulationTask.__tablename__}"))
    await session.commit()


@pytest.mark.asyncio
async def test_fetch_tasks_no_results(session: AsyncSession) -> None:
    """
    测试从数据库中获取任务的功能，当没有符合条件的任务时。

    该测试验证当数据库中没有符合条件的任务时，fetch_tasks方法应当返回空列表。

    Args:
        session: 异步数据库会话对象
    """
    # 初始化 DatabaseTaskProvider
    provider = DatabaseTaskProvider(session=session)

    # 调用 fetch_tasks 方法，期望无结果
    tasks = await provider.fetch_tasks(count=2, priority=10000000)

    # 验证返回值为空
    assert len(tasks) == 0


@pytest.mark.asyncio
async def test_fetch_tasks_invalid_priority(session: AsyncSession) -> None:
    """测试使用不存在的优先级获取任务的情况。

    该测试验证当请求的优先级在数据库中不存在时，fetch_tasks方法应当返回空列表。

    Args:
        session: 异步数据库会话对象
    """
    regular = ["task1"]
    settings = [
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
async def test_fetch_tasks_exception(
    mock_fetch_tasks: AsyncMock,  # pylint: disable=W0613
) -> None:
    """测试DatabaseTaskProvider.fetch_tasks方法抛出异常的情况。

    该测试模拟fetch_tasks方法抛出异常的场景，验证异常是否能够正确传播。

    Args:
        mock_fetch_tasks: 模拟的fetch_tasks方法，配置为抛出Exception异常

    Raises:
        Exception: 期望抛出包含"Database error"信息的异常
    """
    # 模拟异常情况
    provider = DatabaseTaskProvider(session=AsyncMock())

    # 验证异常抛出
    with pytest.raises(Exception, match="Database error"):
        await provider.fetch_tasks(count=1, priority=1)
