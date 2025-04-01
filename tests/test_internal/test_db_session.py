"""数据库会话管理模块测试。

本模块包含对 alphapower.internal.db_session 模块的单元测试，
主要测试数据库连接、会话管理和资源清理等功能。

典型用法:
    pytest tests/test_internal/test_db_session.py
"""

import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, AsyncGenerator, List

import pytest
from sqlalchemy import Integer, Result, String, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from alphapower.constants import DB_ALPHAS, DB_DATA
from alphapower.internal.db_session import (
    async_session_factories,
    db_engines,
    get_db_session,
    register_db,
    release_all_db_engines,
)
from alphapower.settings import DatabaseConfig, settings


# 创建测试模型基类
class TestBase(DeclarativeBase):
    """测试用 SQLAlchemy 模型基类。"""


# 创建一个简单的测试模型
class TestModel(TestBase):
    """测试用 SQLAlchemy 模型。

    用于验证数据库创建和会话管理功能的简单模型。

    Attributes:
        id: 主键，自增整数
        name: 名称，非空字符串
        value: 值，整数，默认为0
    """

    __tablename__: str = "test_model"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    value: Mapped[int] = mapped_column(Integer, default=0)


@pytest.fixture(scope="function", name="db_config")
def fixture_db_config() -> DatabaseConfig:
    """提供测试数据库配置。

    Returns:
        DatabaseConfig: 从全局配置中获取的数据库配置对象
    """
    return settings.databases[DB_ALPHAS]


@pytest.fixture(scope="function", name="setup_test_db")
@asynccontextmanager
async def fixture_setup_test_db(db_config: DatabaseConfig) -> AsyncGenerator[str, None]:
    """设置测试数据库并在测试结束后清理资源。

    此 fixture 创建一个临时测试数据库环境，用于测试数据库相关功能，
    并在测试完成后自动清理所有资源，确保测试环境的隔离性。

    Args:
        db_config: 数据库配置对象

    Yields:
        str: 数据库标识符
    """
    try:
        # 注册测试数据库
        await register_db(TestBase, DB_ALPHAS, db_config, force_recreate=True)
        yield DB_ALPHAS
    finally:
        # 确保清理测试数据库资源，即使测试失败
        await release_all_db_engines()


@pytest.mark.asyncio
async def test_register_db(db_config: DatabaseConfig) -> None:
    """测试数据库注册功能。

    验证 register_db 函数能正确注册数据库引擎和会话工厂，
    并确保注册的引擎类型正确。

    Args:
        db_config: 数据库配置对象
    """
    try:
        # 注册数据库
        await register_db(TestBase, DB_ALPHAS, db_config, force_recreate=True)

        # 验证注册是否成功
        assert DB_ALPHAS in db_engines, "数据库引擎未注册成功"
        assert DB_ALPHAS in async_session_factories, "数据库会话工厂未注册成功"
        assert isinstance(db_engines[DB_ALPHAS], AsyncEngine), "注册的引擎类型不正确"
    finally:
        # 确保资源清理
        await release_all_db_engines()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "db_name,expected_in_engines",
    [
        (DB_ALPHAS, True),
        (DB_DATA, True),  # 可以替换为你的另一个数据库名称
    ],
)
async def test_register_multiple_dbs(db_name: str, expected_in_engines: bool) -> None:
    """使用参数化测试注册多个数据库。

    验证多个数据库能否正确注册，并确保指定数据库的注册状态符合预期。

    Args:
        db_name: 数据库名称
        expected_in_engines: 预期的注册状态
    """
    try:
        # 注册所有配置的数据库
        for name, db_config in settings.databases.items():
            if (
                name == db_name or db_name == DB_ALPHAS
            ):  # 确保至少注册了测试需要的数据库
                await register_db(TestBase, name, db_config, force_recreate=True)

        # 验证指定的数据库已正确注册
        assert (
            db_name in db_engines
        ) == expected_in_engines, f"数据库 {db_name} 注册状态不符合预期"
        if expected_in_engines:
            assert (
                db_name in async_session_factories
            ), f"数据库 {db_name} 的会话工厂未注册成功"
    finally:
        # 清理
        await release_all_db_engines()


@pytest.mark.asyncio
async def test_get_db_session(
    setup_test_db: AbstractAsyncContextManager[str, None],
) -> None:
    """测试获取数据库会话。

    验证 get_db_session 函数能正确返回会话对象，并确保会话能正常执行数据库操作。

    Args:
        setup_test_db: 测试数据库环境上下文管理器
    """
    async with setup_test_db as db_name:
        # 使用会话执行基本操作
        async with get_db_session(db_name) as session:
            assert isinstance(session, AsyncSession), "会话对象类型不正确"

            # 创建测试数据
            test_item: TestModel = TestModel(name="测试项", value=42)
            session.add(test_item)
            await session.commit()

            # 查询数据验证
            result: Result[Any] = await session.execute(
                select(TestModel).where(TestModel.name == "测试项")
            )
            item: TestModel = result.scalar_one()

            assert item.name == "测试项", "创建的测试项名称不正确"
            assert item.value == 42, "创建的测试项值不正确"


@pytest.mark.asyncio
async def test_session_rollback(
    setup_test_db: AbstractAsyncContextManager[str, None],
) -> None:
    """测试会话回滚功能。

    验证会话在发生异常时能正确回滚未提交的事务。

    Args:
        setup_test_db: 测试数据库环境上下文管理器
    """
    async with setup_test_db as db_name:
        # 先插入测试数据
        async with get_db_session(db_name) as session:
            test_item: TestModel = TestModel(name="回滚测试", value=100)
            session.add(test_item)

        # 尝试执行会引发异常的操作，使用上下文管理器处理异常
        with pytest.raises(ValueError, match="测试异常，触发回滚"):
            async with get_db_session(db_name) as session:
                # 查询确认数据存在
                result: Result[Any] = await session.execute(
                    select(TestModel).where(TestModel.name == "回滚测试")
                )
                item: TestModel = result.scalar_one()
                assert item.value == 100, "原始数据值不正确"

                # 修改数据
                item.value = 200

                # 制造异常
                raise ValueError("测试异常，触发回滚")

        # 验证数据是否回滚
        async with get_db_session(db_name) as session:
            result = await session.execute(
                select(TestModel).where(TestModel.name == "回滚测试")
            )
            item = result.scalar_one()
            assert item.value == 100, "数据未正确回滚，值被修改为 200"


@pytest.mark.asyncio
async def test_invalid_db_name() -> None:
    """测试获取不存在的数据库会话。

    验证 get_db_session 函数在传入无效数据库名称时能正确引发 KeyError 异常。
    """
    with pytest.raises(KeyError, match=".*不存在的数据库.*"):
        async with get_db_session("不存在的数据库"):
            pytest.fail("不应该执行到这里，因为应该引发 KeyError")


@pytest.mark.asyncio
async def test_reregister_db(db_config: DatabaseConfig) -> None:
    """测试重新注册同名数据库。

    验证 register_db 函数在重新注册同名数据库时能正确替换旧的数据库引擎。

    Args:
        db_config: 数据库配置对象
    """
    try:
        # 首次注册
        await register_db(TestBase, DB_ALPHAS, db_config, force_recreate=True)
        first_engine: AsyncEngine = db_engines[DB_ALPHAS]

        # 再次注册同名数据库
        await register_db(TestBase, DB_ALPHAS, db_config, force_recreate=True)
        second_engine: AsyncEngine = db_engines[DB_ALPHAS]

        # 验证引擎已被替换
        assert first_engine is not second_engine, "重新注册后引擎实例应该不同"
        assert id(first_engine) != id(second_engine), "引擎对象ID应该不同"
    finally:
        # 清理
        await release_all_db_engines()


@pytest.mark.asyncio
async def test_concurrent_db_operations(
    setup_test_db: AbstractAsyncContextManager[str, None],
) -> None:
    """测试并发数据库操作。

    验证多个并发任务能正确执行数据库操作，并确保数据一致性。

    Args:
        setup_test_db: 测试数据库环境上下文管理器
    """
    async with setup_test_db as db_name:

        async def add_item(name: str, value: int) -> None:
            """向数据库添加测试项。

            Args:
                name: 测试项名称
                value: 测试项值
            """
            async with get_db_session(db_name) as session:
                test_item: TestModel = TestModel(name=name, value=value)
                session.add(test_item)

        # 并发添加10个项目
        tasks: List[asyncio.Task[None]] = []
        for i in range(10):
            tasks.append(asyncio.create_task(add_item(f"并发项{i}", i * 10)))

        await asyncio.gather(*tasks)

        # 验证所有项目都已添加
        async with get_db_session(db_name) as session:
            result: Result[Any] = await session.execute(
                select(TestModel).where(TestModel.name.like("并发项%"))
            )
            items: List[TestModel] = list(result.scalars())
            assert len(items) == 10, "并发添加的项目数量不符合预期"

            # 确认每个值都正确添加
            values = sorted([item.value for item in items])
            assert values == [i * 10 for i in range(10)], "并发添加的项目值不正确"


@pytest.mark.asyncio
async def test_session_isolation(
    setup_test_db: AbstractAsyncContextManager[str, None],
) -> None:
    """测试会话隔离性。

    验证未提交的事务在其他会话中不可见，并确保异常发生时能正确回滚。

    Args:
        setup_test_db: 测试数据库环境上下文管理器
    """
    async with setup_test_db as db_name:
        try:
            # 会话1添加数据但不提交
            async with get_db_session(db_name) as session1:
                test_item1: TestModel = TestModel(name="隔离测试1", value=101)
                session1.add(test_item1)
                # 故意不提交

                # 在同一时间，会话2尝试查询该数据
                async with get_db_session(db_name) as session2:
                    result = await session2.execute(
                        select(TestModel).where(TestModel.name == "隔离测试1")
                    )
                    # 由于会话1未提交，会话2应该查不到数据
                    assert result.first() is None, "未提交的数据不应在其他会话中可见"

                raise ValueError("故意触发异常以测试回滚")
        except ValueError:
            # 捕获异常以确保会话1回滚
            pass

        # 验证会话结束后数据仍未提交
        async with get_db_session(db_name) as session:
            result = await session.execute(
                select(TestModel).where(TestModel.name == "隔离测试1")
            )
            assert result.first() is None, "自动回滚应生效，数据不应存在"
