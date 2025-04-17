"""
测试 SimulationTaskDAL 类的各项功能。
该模块包含对 SimulationTaskDAL 类的单元测试，主要测试通过不同条件查询模拟任务的方法。
"""

from datetime import datetime, timedelta
from typing import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import (
    AlphaType,
    Database,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.dal.simulation import (
    SimulationTaskDAL,
)
from alphapower.entity.simulation import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="simulation_session")
async def fixture_simulation_session() -> AsyncGenerator[AsyncSession, None]:
    """创建Simulation数据库会话用于测试。

    创建与真实数据库的连接会话，用于测试模拟任务相关的数据访问层的操作。
    测试完成后会自动清理会话。

    Yields:
        AsyncSession: SQLAlchemy 异步会话对象。
    """
    async with get_db_session(Database.SIMULATION) as simulation_session:
        yield simulation_session
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


class TestSimulationTaskDAL:
    """测试 SimulationTaskDAL 类的各项功能。"""

    async def test_find_by_status(self, simulation_session: AsyncSession) -> None:
        """测试通过状态查询模拟任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据 - 不同状态的任务
        statuses = [
            SimulationTaskStatus.PENDING,
            SimulationTaskStatus.RUNNING,
            SimulationTaskStatus.COMPLETE,
        ]
        for status in statuses:
            tasks = [
                SimulationTask(
                    type=AlphaType.REGULAR,
                    alpha_id=f"{status.name}_ALPHA_{i}",
                    signature=f"{status.name}_SIG_{i}",
                    settings_group_key="TEST_GROUP",
                    status=status,
                    priority=i,
                    created_at=datetime.now(),
                    regular="test_regular",
                    # 添加缺失的必填字段
                    instrument_type=InstrumentType.EQUITY,
                    region=Region.GLB,
                    universe=Universe.TOP3000,
                    delay=Delay.ONE,
                    neutralization=Neutralization.MARKET,
                    pasteurization=Switch.OFF,
                    unit_handling=UnitHandling.VERIFY,
                    max_trade=Switch.OFF,
                    language=RegularLanguage.PYTHON,
                    visualization=False,
                )
                for i in range(1, 3)
            ]
            simulation_session.add_all(tasks)
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_by_status(SimulationTaskStatus.PENDING)

        # 验证查询结果
        assert len(results) >= 2
        assert all(t.status == SimulationTaskStatus.PENDING for t in results)

    async def test_find_by_alpha_id(self, simulation_session: AsyncSession) -> None:
        """测试通过 alpha_id 查询模拟任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据
        alpha_id = "TEST_ALPHA_ID"
        tasks = [
            SimulationTask(
                alpha_id=alpha_id,
                signature=f"ALPHA_SIG_{i}",
                settings_group_key="TEST_GROUP",
                status=SimulationTaskStatus.PENDING,
                type=AlphaType.REGULAR,
                priority=i,
                created_at=datetime.now(),
                regular="test_regular",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            for i in range(1, 4)
        ]
        simulation_session.add_all(tasks)
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_by_alpha_id(alpha_id)

        # 验证查询结果
        assert len(results) >= 3
        assert all(t.alpha_id == alpha_id for t in results)

    async def test_find_by_signature(self, simulation_session: AsyncSession) -> None:
        """测试通过签名查询模拟任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据
        signature = "UNIQUE_SIGNATURE"
        task = SimulationTask(
            alpha_id="SIG_ALPHA",
            signature=signature,
            settings_group_key="TEST_GROUP",
            status=SimulationTaskStatus.PENDING,
            type=AlphaType.REGULAR,
            priority=1,
            created_at=datetime.now(),
            regular="test_regular",
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )
        simulation_session.add(task)
        await simulation_session.flush()

        # 使用特定方法查询
        result = await task_dal.find_by_signature(signature)

        # 验证查询结果
        assert result is not None
        assert result.signature == signature

    async def test_find_pending_tasks(self, simulation_session: AsyncSession) -> None:
        """测试查询待处理任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据 - 不同状态的任务
        pending_tasks = [
            SimulationTask(
                alpha_id=f"PENDING_ALPHA_{i}",
                signature=f"PENDING_SIG_{i}",
                settings_group_key="TEST_GROUP",
                status=SimulationTaskStatus.PENDING,
                type=AlphaType.REGULAR,
                priority=i,
                created_at=datetime.now(),
                regular="test_regular",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            for i in range(1, 4)
        ]
        running_task = SimulationTask(
            alpha_id="RUNNING_ALPHA",
            signature="RUNNING_SIG",
            settings_group_key="TEST_GROUP",
            status=SimulationTaskStatus.RUNNING,
            type=AlphaType.REGULAR,
            priority=1,
            created_at=datetime.now(),
            regular="test_regular",
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )

        simulation_session.add_all(pending_tasks + [running_task])
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_pending_tasks()

        # 验证查询结果
        assert len(results) >= 3
        assert all(t.status == SimulationTaskStatus.PENDING for t in results)

    async def test_find_running_tasks(self, simulation_session: AsyncSession) -> None:
        """测试查询正在运行任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据 - 不同状态的任务
        running_tasks = [
            SimulationTask(
                alpha_id=f"RUNNING_ALPHA_{i}",
                signature=f"RUNNING_SIG_{i}",
                settings_group_key="TEST_GROUP",
                status=SimulationTaskStatus.RUNNING,
                type=AlphaType.REGULAR,
                priority=i,
                created_at=datetime.now(),
                regular="test_regular",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            for i in range(1, 4)
        ]
        pending_task = SimulationTask(
            alpha_id="PENDING_ALPHA",
            signature="PENDING_SIG",
            settings_group_key="TEST_GROUP",
            status=SimulationTaskStatus.PENDING,
            priority=1,
            created_at=datetime.now(),
            regular="test_regular",
            type=AlphaType.REGULAR,
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )

        simulation_session.add_all(running_tasks + [pending_task])
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_running_tasks()

        # 验证查询结果
        assert len(results) >= 3
        assert all(t.status == SimulationTaskStatus.RUNNING for t in results)

    async def test_find_high_priority_tasks(
        self, simulation_session: AsyncSession
    ) -> None:
        """测试查询高优先级任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据 - 不同优先级的任务
        priorities = [1, 3, 5, 7, 10]
        for priority in priorities:
            task = SimulationTask(
                alpha_id=f"PRIO_{priority}_ALPHA",
                signature=f"PRIO_{priority}_SIG",
                status=SimulationTaskStatus.PENDING,
                type=AlphaType.REGULAR,
                priority=priority,
                created_at=datetime.now(),
                settings_group_key="TEST_GROUP",
                regular="test_regular",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            simulation_session.add(task)
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_high_priority_tasks(5)

        # 验证查询结果
        assert len(results) >= 3  # 优先级 >= 5 的任务有3个
        assert all(t.priority >= 5 for t in results)

    async def test_find_by_settings_group(
        self, simulation_session: AsyncSession
    ) -> None:
        """测试通过设置组查询任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据
        group_key = (
            f"{Region.GLB.value}_{Delay.ONE.value}_"
            + f"{RegularLanguage.PYTHON.value}_{InstrumentType.EQUITY.value}"
        )
        tasks = [
            SimulationTask(
                alpha_id=f"GROUP_ALPHA_{i}",
                signature=f"GROUP_SIG_{i}",
                status=SimulationTaskStatus.PENDING,
                type=AlphaType.REGULAR,
                priority=i,
                settings_group_key=group_key,
                created_at=datetime.now(),
                regular="test_regular",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            for i in range(1, 4)
        ]
        # 不同组的任务
        other_task = SimulationTask(
            alpha_id="OTHER_GROUP_ALPHA",
            signature="OTHER_GROUP_SIG",
            status=SimulationTaskStatus.PENDING,
            type=AlphaType.REGULAR,
            priority=1,
            settings_group_key="OTHER_GROUP",
            created_at=datetime.now(),
            regular="test_regular",
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )

        simulation_session.add_all(tasks + [other_task])
        await simulation_session.flush()

        # 使用特定方法查询
        results = await task_dal.find_by_settings_group(group_key)

        # 验证查询结果
        assert len(results) >= 3
        assert all(t.settings_group_key == group_key for t in results)

    async def test_find_tasks_by_date_range(
        self, simulation_session: AsyncSession
    ) -> None:
        """测试通过日期范围查询任务的方法。

        Args:
            simulation_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        task_dal = SimulationTaskDAL(simulation_session)

        # 创建测试数据 - 不同日期的任务
        # 假设今天是基准日期
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        # 昨天的任务
        yesterday_task = SimulationTask(
            alpha_id="YESTERDAY_ALPHA",
            signature="YESTERDAY_SIG",
            status=SimulationTaskStatus.COMPLETE,
            type=AlphaType.REGULAR,
            settings_group_key="TEST_GROUP",
            created_at=datetime.strptime(yesterday, "%Y-%m-%d"),
            regular="test_regular",
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )

        # 今天的任务
        today_tasks = [
            SimulationTask(
                alpha_id=f"TODAY_ALPHA_{i}",
                signature=f"TODAY_SIG_{i}",
                status=SimulationTaskStatus.PENDING,
                type=AlphaType.REGULAR,
                created_at=datetime.strptime(today, "%Y-%m-%d"),
                regular="test_regular",
                settings_group_key="TEST_GROUP",
                # 添加缺失的必填字段
                instrument_type=InstrumentType.EQUITY,
                region=Region.GLB,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.OFF,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
                visualization=False,
            )
            for i in range(1, 3)
        ]

        # 明天的任务
        tomorrow_task = SimulationTask(
            alpha_id="TOMORROW_ALPHA",
            signature="TOMORROW_SIG",
            settings_group_key="TEST_GROUP",
            status=SimulationTaskStatus.PENDING,
            type=AlphaType.REGULAR,
            created_at=datetime.strptime(tomorrow, "%Y-%m-%d"),
            regular="test_regular",
            # 添加缺失的必填字段
            instrument_type=InstrumentType.EQUITY,
            region=Region.GLB,
            universe=Universe.TOP3000,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
            visualization=False,
        )

        simulation_session.add_all([yesterday_task] + today_tasks + [tomorrow_task])
        await simulation_session.flush()

        # 使用特定方法查询 - 查询今天和明天的任务
        results = await task_dal.find_tasks_by_date_range(today, tomorrow)

        # 验证查询结果
        assert len(results) >= 3  # 今天的2个 + 明天的1个
        # 验证日期范围
        for task in results:
            task_date = task.created_at.strftime("%Y-%m-%d")
            assert yesterday < task_date <= tomorrow
