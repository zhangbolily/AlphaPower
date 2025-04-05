"""测试 alphapower.entity.simulation 模块中定义的实体类。

本模块包含一系列测试，用于验证 alphapower.entity.simulation 模块中定义的所有数据库实体类
是否正确实现。测试使用真实数据库连接进行，验证实体类的创建、查询和关系映射。

测试覆盖以下实体类:
- Base: 基础映射类
- SimulationTaskStatus: 任务状态枚举
- AlphaType: 任务类型枚举
- SimulationTask: 主要模拟任务实体
"""

import datetime
from typing import AsyncGenerator, Optional

import pytest
from sqlalchemy import Result, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import (
    AlphaType,
    Database,
    Decay,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Switch,
    Truncation,
    UnitHandling,
    Universe,
)
from alphapower.entity.simulation import (
    Base,
    SimulationTask,
    SimulationTaskStatus,
)
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="session")
async def fixture_session() -> AsyncGenerator[AsyncSession, None]:
    """创建数据库会话用于测试。

    创建与真实数据库的连接会话，用于测试实体类的数据库操作。
    测试完成后会自动清理会话。

    Yields:
        AsyncSession: SQLAlchemy 异步会话对象。
    """
    async with get_db_session(Database.SIMULATION) as session:
        yield session
        # 注意：在生产环境测试中可能需要更复杂的数据清理策略
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


@pytest.fixture(autouse=True)
async def clean_data(session: AsyncSession) -> AsyncGenerator[None, None]:
    """在每个测试开始前清理数据。"""
    await session.execute(delete(SimulationTask))
    yield


class TestBase:
    """测试 Base 基础映射类的基本属性。"""

    def test_base_class_exists(self) -> None:
        """验证 Base 类是否存在且具有必要的元数据属性。

        测试 Base 类是否正确定义，并具有 SQLAlchemy ORM 所需的
        metadata 和 registry 属性。
        """
        assert hasattr(Base, "metadata"), "Base 应该有 metadata 属性"
        assert hasattr(Base, "registry"), "Base 应该有 registry 属性"


class TestSimulationTaskEnums:
    """测试模拟任务相关枚举类型的值和行为。"""

    def test_simulation_task_status_enum(self) -> None:
        """验证 SimulationTaskStatus 枚举类是否包含所有预期的状态值。"""
        assert SimulationTaskStatus.DEFAULT.value == "DEFAULT"
        assert SimulationTaskStatus.PENDING.value == "PENDING"
        assert SimulationTaskStatus.NOT_SCHEDULABLE.value == "NOT_SCHEDULABLE"
        assert SimulationTaskStatus.SCHEDULED.value == "SCHEDULED"
        assert SimulationTaskStatus.RUNNING.value == "RUNNING"
        assert SimulationTaskStatus.COMPLETE.value == "COMPLETE"
        assert SimulationTaskStatus.ERROR.value == "ERROR"
        assert SimulationTaskStatus.CANCELLED.value == "CANCELLED"

        # 验证枚举数量
        assert len(SimulationTaskStatus) == 8, "SimulationTaskStatus 应该有 8 个状态值"

    def test_simulation_task_type_enum(self) -> None:
        """验证 AlphaType 枚举类是否包含所有预期的类型值。"""
        assert AlphaType.REGULAR.value == "REGULAR"
        assert AlphaType.SUPER.value == "SUPER"

        # 验证枚举数量
        assert len(AlphaType) == 2, "AlphaType 应该有 2 个类型值"


class TestSimulationTask:
    """测试 SimulationTask 实体类的各项功能。"""

    async def test_create_simulation_task(self, session: AsyncSession) -> None:
        """测试创建和查询 SimulationTask 实例。

        验证是否可以创建 SimulationTask 对象，将其保存到数据库，然后再次查询出来，
        并确保所有字段的值都正确保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=AlphaType.REGULAR,
            regular="test_regular",
            status=SimulationTaskStatus.PENDING,
            alpha_id="ALPHA123",
            priority=5,
            signature="unique_signature_1",
            description="测试模拟任务",
            tags=["test", "simulation", "unit"],
            # 设置参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            decay=10,
            neutralization=Neutralization.MARKET,
            truncation=0.5,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            language=RegularLanguage.PYTHON,
            visualization=True,
            test_period="2020-01-01/2021-01-01",
            max_trade=Switch.OFF,
        )
        session.add(task)
        await session.flush()  # 使用flush而非commit，让fixture管理事务

        # 查询任务
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        db_task: Optional[SimulationTask] = result.scalars().first()

        # 验证查询结果包含所有原始字段
        assert db_task is not None
        assert db_task.type == AlphaType.REGULAR
        expected_key = (
            f"{Region.CHN.value}_{Delay.ONE.value}_"
            + f"{RegularLanguage.PYTHON.value}_{InstrumentType.EQUITY.value}"
        )
        assert db_task.settings_group_key == expected_key
        assert db_task.regular == "test_regular"
        assert db_task.status == SimulationTaskStatus.PENDING
        assert db_task.alpha_id == "ALPHA123"
        assert db_task.priority == 5
        assert db_task.signature == "unique_signature_1"
        assert db_task.created_at is not None
        assert db_task.updated_at is not None
        assert db_task.description == "测试模拟任务"
        assert set(db_task.tags) == {"test", "simulation", "unit"}
        assert db_task.parent_progress_id is None
        assert db_task.child_progress_id is None
        assert db_task.result is None
        assert db_task.scheduled_at is None
        assert db_task.deleted_at is None
        assert db_task.dependencies is None
        assert db_task.completed_at is None

        # 验证设置参数字段
        assert db_task.instrument_type == InstrumentType.EQUITY
        assert db_task.region == Region.CHN
        assert db_task.universe == Universe.TOP2000U
        assert db_task.delay == Delay.ONE
        assert db_task.decay == 10
        assert db_task.neutralization == Neutralization.MARKET
        assert db_task.truncation == 0.5
        assert db_task.pasteurization == Switch.ON
        assert db_task.unit_handling == UnitHandling.VERIFY
        assert db_task.language == RegularLanguage.PYTHON
        assert db_task.visualization is True
        assert db_task.test_period == "2020-01-01/2021-01-01"
        assert db_task.max_trade == Switch.OFF

    async def test_update_simulation_task(self, session: AsyncSession) -> None:
        """测试更新 SimulationTask 实例。

        验证是否可以正确更新现有的 SimulationTask 对象，并确保所有更新的字段
        都被正确保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=AlphaType.REGULAR,
            regular="regular_value",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_2",
            # 设置参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOP1000,
            delay=Delay.ZERO,
            neutralization=Neutralization.NONE,
            pasteurization=Switch.OFF,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )
        session.add(task)
        await session.flush()

        # 查询并更新任务
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        db_task: SimulationTask = result.scalars().one()

        # 更新任务状态和其他字段
        db_task.status = SimulationTaskStatus.RUNNING
        db_task.parent_progress_id = "parent_1"
        db_task.child_progress_id = "child_1"
        db_task.priority = 10
        db_task.result = {"status": "in_progress", "percentage": 50}
        db_task.scheduled_at = datetime.datetime.now()
        # 更新枚举字段
        db_task.delay = Delay.ONE
        db_task.universe = Universe.TOP3000
        db_task.region = Region.GLB
        db_task.neutralization = Neutralization.INDUSTRY
        db_task.visualization = True

        await session.flush()

        # 重新查询以验证更新
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        updated_task: SimulationTask = result.scalars().one()

        # 验证更新后的字段
        assert updated_task.status == SimulationTaskStatus.RUNNING
        assert updated_task.parent_progress_id == "parent_1"
        assert updated_task.child_progress_id == "child_1"
        assert updated_task.priority == 10
        assert updated_task.result == {"status": "in_progress", "percentage": 50}
        assert updated_task.scheduled_at is not None
        # 验证更新后的枚举字段
        assert updated_task.region == Region.GLB
        assert updated_task.universe == Universe.TOP3000
        assert updated_task.neutralization == Neutralization.INDUSTRY
        assert updated_task.visualization is True

    async def test_complete_simulation_task(self, session: AsyncSession) -> None:
        """测试完成 SimulationTask 任务的流程。

        验证 SimulationTask 从创建到完成的整个生命周期，包括状态变更和结果保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=AlphaType.SUPER,
            regular="lifecycle_regular",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_3",
            # 设置参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.JPN,
            universe=Universe.TOP1200,
            delay=Delay.ONE,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.ON,
            language=RegularLanguage.PYTHON,
        )
        session.add(task)
        await session.flush()

        # 更新为运行状态
        task.status = SimulationTaskStatus.RUNNING
        await session.flush()

        # 再次更新为完成状态
        task.status = SimulationTaskStatus.COMPLETE
        task.result = {"outcome": "success", "metrics": {"score": 0.95}}
        task.completed_at = datetime.datetime.now()
        await session.flush()

        # 查询验证完整生命周期
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        completed_task: SimulationTask = result.scalars().one()

        # 验证最终状态
        assert completed_task.status == SimulationTaskStatus.COMPLETE
        assert completed_task.result is not None
        assert completed_task.result["outcome"] == "success"
        assert completed_task.result["metrics"]["score"] == 0.95
        assert completed_task.completed_at is not None
        # 验证枚举字段保持不变
        assert completed_task.instrument_type == InstrumentType.EQUITY
        assert completed_task.region == Region.JPN
        assert completed_task.universe == Universe.TOP1200
        assert completed_task.delay == Delay.ONE
        assert completed_task.neutralization == Neutralization.SECTOR

    async def test_simulation_task_with_error(self, session: AsyncSession) -> None:
        """测试带有错误信息的 SimulationTask 状态处理。

        验证当 SimulationTask 遇到错误时，能否正确保存错误信息和状态。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=AlphaType.REGULAR,
            regular="error_regular",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_4",
            # 设置参数
            instrument_type=InstrumentType.CRYPTO,
            region=Region.GLB,
            universe=Universe.TOP20,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )
        session.add(task)
        await session.flush()

        # 设置为错误状态
        task.status = SimulationTaskStatus.ERROR
        task.result = {"error": "计算超时", "details": "执行时间超过预设限制"}
        await session.flush()

        # 查询验证错误状态
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        error_task: SimulationTask = result.scalars().one()

        # 验证错误状态和信息
        assert error_task.status == SimulationTaskStatus.ERROR
        assert error_task.result is not None
        assert "error" in error_task.result
        assert error_task.result["error"] == "计算超时"
        # 验证加密货币特有设置
        assert error_task.instrument_type == InstrumentType.CRYPTO
        assert error_task.region == Region.GLB
        assert error_task.universe == Universe.TOP20
        assert error_task.neutralization == Neutralization.MARKET

    async def test_query_simulation_tasks_by_group(self, session: AsyncSession) -> None:
        """测试通过分组键查询 SimulationTask。

        验证是否可以使用 settings_group_key 字段查询一组相关的任务。

        Args:
            session: 数据库会话对象。
        """
        # 使用相同的region、delay、language和instrument_type创建任务，以生成相同的settings_group_key
        group_region = Region.CHN
        group_delay = Delay.ONE
        group_language = RegularLanguage.PYTHON
        group_instrument = InstrumentType.EQUITY

        tasks = []
        for i in range(3):
            task = SimulationTask(
                type=AlphaType.REGULAR,
                regular=f"regular_{i}",
                status=SimulationTaskStatus.PENDING,
                signature=f"group_signature_{i}",
                # 设置参数以生成相同的settings_group_key
                region=group_region,
                delay=group_delay,
                language=group_language,
                instrument_type=group_instrument,
                universe=Universe.TOP2000U,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
            )
            tasks.append(task)

        # 额外创建一个不同分组的任务
        different_task = SimulationTask(
            type=AlphaType.REGULAR,
            regular="different_regular",
            status=SimulationTaskStatus.PENDING,
            signature="different_signature",
            # 设置不同的参数以生成不同的settings_group_key
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,  # 不同的区域
            universe=Universe.TOP3000,  # 适合USA的universe
            delay=Delay.ZERO,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )

        session.add_all(tasks + [different_task])
        await session.flush()

        # 获取预期的 settings_group_key
        group_key = (
            f"{group_region.value}_{group_delay.value}_"
            + f"{group_language.value}_{group_instrument.value}"
        )

        # 查询特定分组的任务
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.settings_group_key == group_key)
        )
        group_tasks = result.scalars().all()

        # 验证查询结果
        assert len(group_tasks) == 3
        for task in group_tasks:
            assert task.settings_group_key == group_key

    async def test_validation_methods(self, session: AsyncSession) -> None:
        """测试参数验证方法的边界情况和异常处理。

        验证 decay 和 truncation 参数的验证方法是否正确工作，包括成功和失败的情况。

        Args:
            session: 数据库会话对象。
        """
        # 测试有效参数值
        valid_task = SimulationTask(
            type=AlphaType.REGULAR,
            regular="valid_params",
            signature="validation_signature",
            decay=10,  # 有效的 decay 值
            truncation=0.5,  # 有效的 truncation 值
            # 设置必需的枚举参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOP3000,
            delay=Delay.ZERO,
            neutralization=Neutralization.NONE,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )
        session.add(valid_task)
        await session.flush()

        # 验证有效值被正确保存
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == valid_task.id)
        )
        db_valid_task = result.scalars().one()
        assert db_valid_task.decay == 10
        assert db_valid_task.truncation == 0.5

        # 测试无效的 decay 值（应该触发验证异常）
        with pytest.raises(
            ValueError,
            match=f"decay 必须在 {Decay.MIN.value} 到 {Decay.MAX.value} 之间",
        ):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="invalid_params",
                signature="invalid_decay_signature",
                decay=513,  # 超出有效范围
                # 设置必需的枚举参数
                instrument_type=InstrumentType.EQUITY,
                region=Region.USA,
                universe=Universe.TOP1000,
                delay=Delay.ZERO,
                neutralization=Neutralization.NONE,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        # 测试无效的 truncation 值（应该触发验证异常）
        with pytest.raises(
            ValueError,
            match=f"truncation 必须在 {Truncation.MIN.value} 到 {Truncation.MAX.value} 之间",
        ):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="invalid_params",
                signature="invalid_truncation_signature",
                truncation=1.5,  # 超出有效范围
                # 设置必需的枚举参数
                instrument_type=InstrumentType.EQUITY,
                region=Region.USA,
                universe=Universe.TOP2000,
                delay=Delay.ZERO,
                neutralization=Neutralization.NONE,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

    async def test_tag_methods(self, session: AsyncSession) -> None:
        """测试标签管理方法的功能。

        验证标签的添加、移除以及获取功能是否正常工作。

        Args:
            session: 数据库会话对象。
        """
        # 创建带有初始标签的任务
        task = SimulationTask(
            type=AlphaType.REGULAR,
            regular="tag_methods",
            signature="tag_signature",
            tags=["initial", "tags"],
            # 设置必需的枚举参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOP1000,
            delay=Delay.ZERO,
            neutralization=Neutralization.NONE,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )
        session.add(task)
        await session.flush()

        # 验证初始标签
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        db_task = result.scalars().one()
        assert db_task.tags == ["initial", "tags"]

        # 测试添加标签
        db_task.add_tag("new_tag")
        await session.flush()

        # 重新查询以验证
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        updated_task = result.scalars().one()
        assert "new_tag" in updated_task.tags
        assert len(updated_task.tags) == 3

        # 测试移除标签
        updated_task.remove_tag("initial")
        await session.flush()

        # 再次查询验证
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        final_task = result.scalars().one()
        assert "initial" not in final_task.tags
        assert len(final_task.tags) == 2
        assert set(final_task.tags) == {"tags", "new_tag"}

        # 测试设置全新的标签列表
        final_task.tags = ["completely", "new", "list"]  # type: ignore[method-assign]
        await session.flush()

        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        reset_task = result.scalars().one()
        assert set(reset_task.tags) == {"completely", "new", "list"}
        assert len(reset_task.tags) == 3

        # 测试空标签处理
        reset_task.add_tag("")
        reset_task.add_tag("   ")
        await session.flush()

        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        empty_tag_task = result.scalars().one()
        assert set(empty_tag_task.tags) == {"completely", "new", "list"}

        # 测试移除不存在的标签
        empty_tag_task.remove_tag("nonexistent")
        await session.flush()
        assert set(empty_tag_task.tags) == {"completely", "new", "list"}

        # 测试设置 None 到标签
        empty_tag_task.tags = None  # type: ignore[method-assign]
        await session.flush()

        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        none_tag_task = result.scalars().one()
        assert none_tag_task.tags is None

    async def test_settings_group_key_generation(self, session: AsyncSession) -> None:
        """测试 settings_group_key 的自动生成逻辑。

        验证 settings_group_key 是否根据 region、delay、language 和 instrument_type 正确生成。

        Args:
            session: 数据库会话对象。
        """
        # 创建基本任务
        task = SimulationTask(
            type=AlphaType.REGULAR,
            regular="test_settings_key",
            signature="settings_key_signature",
            # 设置参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            neutralization=Neutralization.MARKET,
            language=RegularLanguage.PYTHON,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
        )
        session.add(task)
        await session.flush()

        # 验证初始 settings_group_key
        expected_key = (
            f"{Region.CHN.value}_{Delay.ONE.value}_"
            + f"{RegularLanguage.PYTHON.value}_{InstrumentType.EQUITY.value}"
        )
        assert task.settings_group_key == expected_key

        # 修改各个影响字段并验证 settings_group_key 是否更新
        task.universe = Universe.TOP3000
        task.region = Region.USA
        await session.flush()
        expected_key = (
            f"{Region.USA.value}_{Delay.ONE.value}_"
            + f"{RegularLanguage.PYTHON.value}_{InstrumentType.EQUITY.value}"
        )
        assert task.settings_group_key == expected_key

        task.delay = Delay.ZERO
        await session.flush()
        expected_key = (
            f"{Region.USA.value}_{Delay.ZERO.value}_"
            + f"{RegularLanguage.PYTHON.value}_{InstrumentType.EQUITY.value}"
        )
        assert task.settings_group_key == expected_key

        task.language = RegularLanguage.EXPRESSION
        await session.flush()
        expected_key = (
            f"{Region.USA.value}_{Delay.ZERO.value}_"
            + f"{RegularLanguage.EXPRESSION.value}_{InstrumentType.EQUITY.value}"
        )
        assert task.settings_group_key == expected_key

        task.instrument_type = InstrumentType.EQUITY
        await session.flush()
        expected_key = (
            f"{Region.USA.value}_{Delay.ZERO.value}_"
            + f"{RegularLanguage.EXPRESSION.value}_{InstrumentType.EQUITY.value}"
        )
        assert task.settings_group_key == expected_key

    async def test_field_validators(self) -> None:
        """测试各个字段的验证器功能。

        验证所有字段验证方法是否正确工作，包括值域验证和值的合法性检查。

        Args:
            session: 数据库会话对象。
        """
        # 测试各个枚举字段的 DEFAULT 值验证
        with pytest.raises(ValueError, match="region 不能使用 DEFAULT 值"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="validator_test",
                signature="validator_signature",
                region=Region.DEFAULT,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP500,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        with pytest.raises(ValueError, match="instrument_type 不能使用 DEFAULT 值"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="validator_test",
                signature="validator_signature",
                region=Region.USA,
                instrument_type=InstrumentType.DEFAULT,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        with pytest.raises(ValueError, match="universe 不能使用 DEFAULT 值"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="validator_test",
                signature="validator_signature",
                region=Region.USA,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.DEFAULT,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        with pytest.raises(ValueError, match="delay 不能使用 DEFAULT 值"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="validator_test",
                signature="validator_signature",
                region=Region.USA,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP3000,
                delay=Delay.DEFAULT,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        with pytest.raises(ValueError, match="neutralization 不能使用 DEFAULT 值"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="validator_test",
                signature="validator_signature",
                region=Region.USA,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.DEFAULT,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

    async def test_field_relationship_validation(self) -> None:
        """测试字段间关系的验证逻辑。

        验证字段间的关系约束是否正确实施，如 region 和 instrument_type 的兼容性等。

        Args:
            session: 数据库会话对象。
        """
        # 测试区域和证券类型兼容性验证
        with pytest.raises(ValueError, match="区域.*不支持证券类型"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="relationship_test",
                signature="relationship_signature",
                region=Region.TWN,
                instrument_type=InstrumentType.CRYPTO,
                universe=Universe.TOP500,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        # 测试选股范围与区域、证券类型兼容性验证
        with pytest.raises(ValueError, match="选股范围.*对证券类型.*和区域.*无效"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="universe_test",
                signature="universe_signature",
                region=Region.USA,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP20,
                delay=Delay.ONE,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        # 测试延迟设置与区域兼容性验证
        with pytest.raises(ValueError, match="延迟设置.*对区域.*无效"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="delay_test",
                signature="delay_signature",
                region=Region.GLB,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP3000,
                delay=Delay.ZERO,
                neutralization=Neutralization.MARKET,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

        # 测试中性化策略与区域和证券类型兼容性验证
        with pytest.raises(ValueError, match="中性化策略.*对证券类型.*和区域.*无效"):
            SimulationTask(
                type=AlphaType.REGULAR,
                regular="neutralization_test",
                signature="neutralization_signature",
                region=Region.USA,
                instrument_type=InstrumentType.EQUITY,
                universe=Universe.TOP3000,
                delay=Delay.ONE,
                neutralization=Neutralization.COUNTRY,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                max_trade=Switch.OFF,
                language=RegularLanguage.PYTHON,
            )

    async def test_event_listeners(self, session: AsyncSession) -> None:
        """测试 SQLAlchemy 事件监听器的功能。

        验证对象更新前后的自动事件处理，包括 settings_group_key 自动更新和关系验证。

        Args:
            session: 数据库会话对象。
        """
        # 创建基本任务
        task = SimulationTask(
            type=AlphaType.REGULAR,
            regular="event_listener_test",
            signature="listener_signature",
            # 设置参数
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOP3000,
            delay=Delay.ZERO,
            neutralization=Neutralization.MARKET,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            max_trade=Switch.OFF,
            language=RegularLanguage.PYTHON,
        )
        session.add(task)
        await session.flush()

        original_key = task.settings_group_key

        # 修改影响 settings_group_key 的字段，验证监听器是否触发更新
        task.universe = Universe.TOP2000U
        task.region = Region.CHN
        await session.flush()

        # 重新查询验证自动更新
        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        updated_task = result.scalars().one()

        assert updated_task.settings_group_key != original_key
        assert Region.CHN.value in updated_task.settings_group_key

        # 测试多字段同时变更
        task.delay = Delay.ONE
        task.language = RegularLanguage.EXPRESSION
        await session.flush()

        result = await session.execute(
            select(SimulationTask).where(SimulationTask.id == task.id)
        )
        multi_updated_task = result.scalars().one()

        assert str(Delay.ONE.value) in multi_updated_task.settings_group_key
        assert RegularLanguage.EXPRESSION.value in multi_updated_task.settings_group_key
