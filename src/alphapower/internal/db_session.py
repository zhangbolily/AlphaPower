"""
数据库会话管理模块。

该模块提供异步数据库引擎和会话工厂的注册和创建功能。可用于管理多个数据库连接，
并提供异步上下文管理器以确保数据库会话的正确使用和资源释放。

主要功能:
    - 注册和管理多个数据库引擎
    - 提供异步会话上下文管理器
    - 自动处理事务提交和回滚
    - 资源释放功能

典型用法:
    async def init_db():
        await register_db(BaseModel, db_config)

    async with get_db_session("main_db") as session:
        result = await session.execute(query)
        data = result.scalars().all()
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Tuple, Type

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from alphapower.constants import Database
from alphapower.settings import DatabaseConfig, settings

from .logging import setup_logging

logger = setup_logging(__name__)

db_engines: Dict[Database, AsyncEngine] = {}
async_session_factories: Dict[Database, async_sessionmaker[AsyncSession]] = {}

# 添加锁来保护全局字典的访问
_db_lock: asyncio.Lock = asyncio.Lock()


async def register_db(
    base: Type[DeclarativeBase],
    db: Database,
    config: DatabaseConfig,
    force_recreate: bool = False,
) -> None:
    """
    注册数据库引擎和会话工厂，并创建表结构。

    此函数现在是线程安全的，使用异步锁保护全局字典的修改。

    Args:
        base: SQLAlchemy 基类，用于定义模型，必须是 DeclarativeBase 或其子类。
        name: 数据库名称，用于后续引用。
        config: 数据库配置对象，包含数据库连接信息。
        force_recreate: 是否强制重新创建表结构，默认为False。

    Returns:
        None
    """
    await logger.adebug(
        f"进入 register_db 函数，参数: base={base}, db={db}, config={config}, "
        f"force_recreate={force_recreate}",
        emoji="🔧",
    )
    async with _db_lock:
        if db in db_engines:
            await logger.awarning(
                f"数据库 {db} 已注册，重新注册会覆盖现有配置。",
                emoji="⚠️",
            )

        # 创建数据库引擎，配置连接参数
        connect_args: Dict[str, Any] = {}
        execution_options: Dict[str, Any] = {}

        if "sqlite" in config.dsn.scheme:
            # SQLite连接参数，根据使用情况决定是否允许跨线程访问
            connect_args["check_same_thread"] = False
            # 设置超时时间(秒)，防止"database is locked"错误
            connect_args["timeout"] = 30.0
            execution_options["isolation_level"] = "SERIALIZABLE"

        db_engine: AsyncEngine = create_async_engine(
            config.dsn.encoded_string(),
            echo=settings.sql_echo,
            connect_args=connect_args,
            execution_options=execution_options,
        )

        # 注册引擎和会话工厂到全局字典
        db_engines[db] = db_engine
        session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=db_engine,
            class_=AsyncSession,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        async_session_factories[db] = session_factory

        # 如果是SQLite，配置WAL模式
        if "sqlite" in config.dsn.scheme:
            async with db_engine.begin() as conn:
                # 配置WAL模式，提高并发性能
                await conn.execute(text("PRAGMA journal_mode=WAL;"))
                # 设置同步模式，提高性能
                await conn.execute(text("PRAGMA synchronous=NORMAL;"))
                await logger.ainfo(f"数据库 {db} 已配置为WAL模式")

        await logger.ainfo(
            f"数据库 {db} 已注册，连接字符串: {config.dsn}",
            emoji="✅",
        )
        await logger.adebug(
            f"数据库 {db} 注册信息，DSN: {config.dsn}，描述: {config.description}，别名: {config.alias}，"
            + f"基类: {base}，引擎: {db_engine}，会话工厂: {session_factory}"
            + f"，表结构: {base.metadata.tables}",
            emoji="📋",
        )

        # 创建数据库表结构
        async with db_engine.begin() as conn:
            if force_recreate:
                await conn.run_sync(base.metadata.drop_all)
                await logger.ainfo(f"数据库 {db} 已删除现有表结构。")
            await conn.run_sync(base.metadata.create_all)
            await logger.ainfo(
                f"数据库 {db} 已注册并创建表结构。",
                emoji="🏗️",
            )


def sync_register_db(
    base: Type[DeclarativeBase],
    db: Database,
    config: DatabaseConfig,
    force_recreate: bool = False,
) -> None:
    """
    同步注册数据库引擎和会话工厂，并创建表结构。

    此函数是线程安全的，使用锁保护全局字典的修改。

    Args:
        base: SQLAlchemy 基类，用于定义模型，必须是 DeclarativeBase 或其子类。
        db: 数据库名称，用于后续引用。
        config: 数据库配置对象，包含数据库连接信息。
        force_recreate: 是否强制重新创建表结构，默认为False。

    Returns:
        None
    """
    asyncio.run(register_db(base, db, config, force_recreate))


@asynccontextmanager
async def get_db_session(db: Database) -> AsyncGenerator[AsyncSession, None]:
    """
    获取指定数据库的异步会话。

    此函数线程安全地获取数据库会话工厂。

    使用异步上下文管理器提供数据库会话，确保会话在使用后正确关闭。
    会在退出上下文时自动提交或回滚事务。

    Args:
        db_name: 数据库名称，必须是已通过register_db注册的数据库

    Yields:
        AsyncSession: 用于执行数据库操作的异步会话对象

    Raises:
        KeyError: 当指定的数据库名称不存在时

    示例:
        ```python
        async with get_db_session("main_db") as session:
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalar_one_or_none()
        ```
    """
    await logger.adebug(f"进入 get_db_session 函数，参数: db={db}", emoji="🔧")
    db_name: str = db.value

    # 使用锁保护对全局字典的读取操作
    async with _db_lock:
        if db not in async_session_factories:
            raise KeyError(
                f"数据库 {db_name} 未注册，无法获取会话。请先调用 register_db 进行注册。"
            )
        # 获取会话工厂的本地引用
        session_factory: async_sessionmaker[AsyncSession] = async_session_factories[db]

    # 创建会话 - 在锁外创建以避免长时间持有锁
    async_session: AsyncSession = session_factory()
    try:
        # 提供会话给调用者
        yield async_session
        # 提交事务
        await async_session.commit()
        await logger.adebug(f"数据库会话 {db_name} 提交成功。", emoji="✅")
    except Exception as e:
        # 发生异常时回滚事务
        await async_session.rollback()
        await logger.aerror(
            f"数据库会话 {db_name} 回滚，发生异常: {e}",
            exc_info=True,
            emoji="❌",
        )
        raise e
    finally:
        # 确保会话始终被关闭
        await async_session.close()
        await logger.ainfo(f"数据库会话 {db_name} 已关闭。", emoji="🔒")


async def release_all_db_engines() -> None:
    """
    线程安全地释放所有注册的数据库引擎。

    Returns:
        None
    """
    await logger.ainfo("开始释放所有数据库引擎。", emoji="🛠️")
    async with _db_lock:
        engines_to_dispose: List[Tuple[Database, AsyncEngine]] = list(
            db_engines.items()
        )
        db_engines.clear()
        async_session_factories.clear()

    # 在锁外释放引擎，避免长时间持有锁
    for db_name, engine in engines_to_dispose:
        await engine.dispose()
        await logger.ainfo(f"数据库 {db_name} 引擎已释放。", emoji="✅")
    await logger.ainfo("所有数据库引擎已释放完成。", emoji="🎉")
