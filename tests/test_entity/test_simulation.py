"""测试 alphapower.entity.simulation 模块中定义的实体类。

本模块包含一系列测试，用于验证 alphapower.entity.simulation 模块中定义的所有数据库实体类
是否正确实现。测试使用真实数据库连接进行，验证实体类的创建、查询和关系映射。

测试覆盖以下实体类:
- Base: 基础映射类
- SimulationTaskStatus: 任务状态枚举
- SimulationTaskType: 任务类型枚举
- SimulationTask: 主要模拟任务实体
"""

import datetime
from typing import AsyncGenerator, Dict, Optional

import pytest
from sqlalchemy import Result, select
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import DB_SIMULATION
from alphapower.entity.simulation import (
    Base,
    SimulationTask,
    SimulationTaskStatus,
    SimulationTaskType,
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
    async with get_db_session(DB_SIMULATION) as session:
        yield session
        # 注意：在生产环境测试中可能需要更复杂的数据清理策略
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


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
        """验证 SimulationTaskType 枚举类是否包含所有预期的类型值。"""
        assert SimulationTaskType.REGULAR.value == "REGULAR"
        assert SimulationTaskType.SUPER.value == "SUPER"

        # 验证枚举数量
        assert len(SimulationTaskType) == 2, "SimulationTaskType 应该有 2 个类型值"


class TestSimulationTask:
    """测试 SimulationTask 实体类的各项功能。"""

    async def test_create_simulation_task(self, session: AsyncSession) -> None:
        """测试创建和查询 SimulationTask 实例。

        验证是否可以创建 SimulationTask 对象，将其保存到数据库，然后再次查询出来，
        并确保所有字段的值都正确保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建任务设置
        settings: Dict = {"param1": "value1", "param2": 42, "nested": {"key": "value"}}

        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=SimulationTaskType.REGULAR,
            settings=settings,
            settings_group_key="test_group",
            regular="test_regular",
            status=SimulationTaskStatus.PENDING,
            alpha_id="ALPHA123",
            priority=5,
            signature="unique_signature_1",
            description="测试模拟任务",
            tags="test,simulation,unit",
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
        assert db_task.type == SimulationTaskType.REGULAR
        assert db_task.settings == settings
        assert db_task.settings_group_key == "test_group"
        assert db_task.regular == "test_regular"
        assert db_task.status == SimulationTaskStatus.PENDING
        assert db_task.alpha_id == "ALPHA123"
        assert db_task.priority == 5
        assert db_task.signature == "unique_signature_1"
        assert db_task.created_at is not None
        assert db_task.updated_at is not None
        assert db_task.description == "测试模拟任务"
        assert db_task.tags == "test,simulation,unit"
        assert db_task.parent_progress_id is None
        assert db_task.child_progress_id is None
        assert db_task.result is None
        assert db_task.scheduled_at is None
        assert db_task.deleted_at is None
        assert db_task.dependencies is None
        assert db_task.completed_at is None

    async def test_update_simulation_task(self, session: AsyncSession) -> None:
        """测试更新 SimulationTask 实例。

        验证是否可以正确更新现有的 SimulationTask 对象，并确保所有更新的字段
        都被正确保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=SimulationTaskType.REGULAR,
            settings={"initial": "config"},
            settings_group_key="update_group",
            regular="regular_value",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_2",
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

    async def test_complete_simulation_task(self, session: AsyncSession) -> None:
        """测试完成 SimulationTask 任务的流程。

        验证 SimulationTask 从创建到完成的整个生命周期，包括状态变更和结果保存。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=SimulationTaskType.SUPER,
            settings={"super": "config"},
            settings_group_key="lifecycle_group",
            regular="lifecycle_regular",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_3",
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

    async def test_simulation_task_with_error(self, session: AsyncSession) -> None:
        """测试带有错误信息的 SimulationTask 状态处理。

        验证当 SimulationTask 遇到错误时，能否正确保存错误信息和状态。

        Args:
            session: 数据库会话对象。
        """
        # 创建模拟任务
        task: SimulationTask = SimulationTask(
            type=SimulationTaskType.REGULAR,
            settings={"error": "test"},
            settings_group_key="error_group",
            regular="error_regular",
            status=SimulationTaskStatus.PENDING,
            signature="unique_signature_4",
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

    async def test_query_simulation_tasks_by_group(self, session: AsyncSession) -> None:
        """测试通过分组键查询 SimulationTask。

        验证是否可以使用 settings_group_key 字段查询一组相关的任务。

        Args:
            session: 数据库会话对象。
        """
        # 创建多个具有相同分组键的任务
        group_key = "group_query_test"
        tasks = []

        for i in range(3):
            task = SimulationTask(
                type=SimulationTaskType.REGULAR,
                settings={"test_index": i},
                settings_group_key=group_key,
                regular=f"regular_{i}",
                status=SimulationTaskStatus.PENDING,
                signature=f"group_signature_{i}",
            )
            tasks.append(task)

        # 额外创建一个不同分组的任务
        different_task = SimulationTask(
            type=SimulationTaskType.REGULAR,
            settings={"different": "true"},
            settings_group_key="different_group",
            regular="different_regular",
            status=SimulationTaskStatus.PENDING,
            signature="different_signature",
        )

        session.add_all(tasks + [different_task])
        await session.flush()

        # 查询特定分组的任务
        result: Result = await session.execute(
            select(SimulationTask).where(SimulationTask.settings_group_key == group_key)
        )
        group_tasks = result.scalars().all()

        # 验证查询结果
        assert len(group_tasks) == 3
        for task in group_tasks:
            assert task.settings_group_key == group_key
            assert "test_index" in task.settings
