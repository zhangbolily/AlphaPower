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
from typing import AsyncGenerator, Dict

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from alphapower.settings import DatabaseConfig, settings

from .logging import setup_logging

logger = setup_logging(__name__)

db_engines: Dict[str, AsyncEngine] = {}
async_session_factories: Dict[str, async_sessionmaker[AsyncSession]] = {}

# 添加锁来保护全局字典的访问
_db_lock: asyncio.Lock = asyncio.Lock()


async def register_db(
    base: type[DeclarativeBase],
    name: str,
    config: DatabaseConfig,
    force_recreate: bool = False,
) -> None:
    """
    注册数据库引擎和会话工厂，并创建表结构。

    此函数现在是线程安全的，使用异步锁保护全局字典的修改。

    Args:
        base: SQLAlchemy 基类，用于定义模型，必须是 DeclarativeBase 或其子类。
        config: 数据库配置对象，包含数据库连接信息。

    Returns:
        None
    """
    async with _db_lock:
        if name in db_engines:
            await logger.awarning(f"数据库 {name} 已注册，重新注册会覆盖现有配置。")

        # 创建数据库引擎，配置连接参数
        connect_args = {}
        if "sqlite" in config.dsn.scheme:
            # SQLite连接参数，根据使用情况决定是否允许跨线程访问
            connect_args["check_same_thread"] = False

        db_engine: AsyncEngine = create_async_engine(
            config.dsn.encoded_string(),
            echo=settings.sql_echo,
            pool_size=5,
            max_overflow=10,  # 增加最大溢出连接数
            pool_timeout=30,  # 设置获取连接的超时时间
            pool_recycle=1800,  # 连接回收时间（秒）
            connect_args=connect_args,
        )

        # 注册引擎和会话工厂到全局字典
        db_engines[name] = db_engine
        async_session_factories[name] = async_sessionmaker(
            bind=db_engine,
            class_=AsyncSession,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        await logger.ainfo(f"数据库 {name} 已注册，连接字符串: {config.dsn}")
        await logger.adebug(
            f"数据库 {name} 注册信息，DSN: {config.dsn}，描述: {config.description}，别名: {config.alias}，"
            + f"基类: {base}，引擎: {db_engine}，会话工厂: {async_session_factories[name]}"
            + f"，表结构: {base.metadata.tables}"
        )

        # 创建数据库表结构
        async with db_engine.begin() as conn:
            if force_recreate:
                await conn.run_sync(base.metadata.drop_all)
                await logger.ainfo(f"数据库 {name} 已删除现有表结构。")
            await conn.run_sync(base.metadata.create_all)
            await logger.ainfo(f"数据库 {name} 已注册并创建表结构。")


@asynccontextmanager
async def get_db_session(db_name: str) -> AsyncGenerator[AsyncSession, None]:
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
    # 使用锁保护对全局字典的读取操作
    async with _db_lock:
        if db_name not in async_session_factories:
            raise KeyError(
                f"数据库 {db_name} 未注册，无法获取会话。请先调用 register_db 进行注册。"
            )
        # 获取会话工厂的本地引用
        session_factory = async_session_factories[db_name]

    # 创建会话 - 在锁外创建以避免长时间持有锁
    async_session: AsyncSession = session_factory()
    try:
        # 提供会话给调用者
        yield async_session
        # 提交事务
        await async_session.commit()
    except Exception as e:
        # 发生异常时回滚事务
        await async_session.rollback()
        raise e
    finally:
        # 确保会话始终被关闭
        await async_session.close()


async def release_all_db_engines() -> None:
    """
    线程安全地释放所有注册的数据库引擎。

    Returns:
        None
    """
    async with _db_lock:
        engines_to_dispose = list(db_engines.items())
        db_engines.clear()
        async_session_factories.clear()

    # 在锁外释放引擎，避免长时间持有锁
    for db_name, engine in engines_to_dispose:
        await engine.dispose()
        await logger.ainfo(f"数据库 {db_name} 引擎已释放。")
