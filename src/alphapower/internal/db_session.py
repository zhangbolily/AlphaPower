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
        db: 数据库名称，用于后续引用。
        config: 数据库配置对象，包含数据库连接信息。
        force_recreate: 是否强制重新创建表结构，默认为False。

    Returns:
        None
    """
    # 使用 structlog 风格记录函数入口和参数
    await logger.adebug(
        "进入 register_db 函数",
        base=str(base),  # 记录类型信息，避免直接引用复杂对象
        db=db.value,
        config=config.model_dump(exclude={"dsn"}),  # 排除敏感信息或过长信息
        dsn=config.dsn.encoded_string(),
        force_recreate=force_recreate,
        emoji="🏁",
    )
    async with _db_lock:
        if db in db_engines:
            await logger.awarning(
                "数据库已注册，重新注册将覆盖现有配置",
                db=db.value,
                emoji="⚠️",
            )

        # 创建数据库引擎，配置连接参数
        connect_args: Dict[str, Any] = {}
        execution_options: Dict[str, Any] = {}

        if "sqlite" in config.dsn.scheme:
            connect_args["check_same_thread"] = False
            connect_args["timeout"] = 30.0
            execution_options["isolation_level"] = "SERIALIZABLE"
            await logger.adebug(
                "配置 SQLite 特定连接参数",
                db=db.value,
                connect_args=connect_args,
                execution_options=execution_options,
                emoji="⚙️",
            )

        try:
            db_engine: AsyncEngine = create_async_engine(
                config.dsn.encoded_string(),
                echo=settings.sql_echo,
                connect_args=connect_args,
                execution_options=execution_options,
            )
            await logger.adebug(
                "异步引擎已创建",
                db=db.value,
                engine_repr=repr(db_engine),  # 使用 repr 获取引擎信息
                emoji="🛠️",
            )
        except Exception as e:
            await logger.aerror(
                "创建异步引擎失败",
                db=db.value,
                dsn=config.dsn.encoded_string(),
                error=str(e),
                exc_info=True,
                emoji="💥",
            )
            raise  # 重新抛出异常，以便上层处理

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
        await logger.adebug(
            "引擎和会话工厂已注册",
            db=db.value,
            engine_repr=repr(db_engine),
            session_factory_repr=repr(session_factory),
            emoji="💾",
        )

        # 如果是SQLite，配置WAL模式
        if "sqlite" in config.dsn.scheme:
            try:
                async with db_engine.begin() as conn:
                    await conn.execute(text("PRAGMA journal_mode=WAL;"))
                    await conn.execute(text("PRAGMA synchronous=NORMAL;"))
                    await logger.ainfo(
                        "数据库已配置为 WAL 模式",
                        db=db.value,
                        emoji="💡",
                    )
            except Exception as e:
                await logger.aerror(
                    "为 SQLite 配置 WAL 模式失败",
                    db=db.value,
                    error=str(e),
                    exc_info=True,
                    emoji="💥",
                )
                # 根据策略决定是否继续或抛出异常

        await logger.ainfo(
            "数据库已注册",
            db=db.value,
            dsn=config.dsn.encoded_string(),
            emoji="✅",
        )

        # 创建数据库表结构
        try:
            async with db_engine.begin() as conn:
                if force_recreate:
                    await logger.ainfo(
                        "正在删除现有表",
                        db=db.value,
                        emoji="🗑️",
                    )
                    await conn.run_sync(base.metadata.drop_all)
                    await logger.ainfo(
                        "现有表已删除",
                        db=db.value,
                        emoji="✅",
                    )
                await logger.adebug(
                    "正在创建数据库表",
                    db=db.value,
                    tables=list(base.metadata.tables.keys()),
                    emoji="🏗️",
                )
                await conn.run_sync(base.metadata.create_all)
                await logger.ainfo(
                    "数据库表已创建/验证",
                    db=db.value,
                    emoji="👍",
                )
        except Exception as e:
            await logger.aerror(
                "创建数据库表失败",
                db=db.value,
                error=str(e),
                exc_info=True,
                emoji="💥",
            )
            raise  # 重新抛出异常

    await logger.adebug("退出 register_db 函数", db=db.value, emoji="🚪")


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
    # 同步函数中调用异步注册，日志在异步函数内部处理
    asyncio.run(register_db(base, db, config, force_recreate))


@asynccontextmanager
async def get_db_session(db: Database) -> AsyncGenerator[AsyncSession, None]:
    """
    获取指定数据库的异步会话。

    此函数线程安全地获取数据库会话工厂。

    使用异步上下文管理器提供数据库会话，确保会话在使用后正确关闭。
    会在退出上下文时自动提交或回滚事务。

    Args:
        db: 数据库名称，必须是已通过register_db注册的数据库

    Yields:
        AsyncSession: 用于执行数据库操作的异步会话对象

    Raises:
        KeyError: 当指定的数据库名称不存在时

    示例:
        ```python
        async with get_db_session(Database.MAIN) as session:
            # ... use session ...
        ```
    """
    db_name: str = db.value
    await logger.adebug("进入 get_db_session 函数", db=db_name, emoji="🚪")

    session_factory: async_sessionmaker[AsyncSession] | None = None
    async with _db_lock:
        if db not in async_session_factories:
            # 错误日志应在抛出异常前记录
            await logger.aerror(
                "数据库未注册，无法获取会话",
                db=db_name,
                available_dbs=[
                    d.value for d in async_session_factories.keys()
                ],  # 显示中文枚举值
                emoji="❌",
            )
            raise KeyError(
                f"数据库 '{db_name}' 未注册，无法获取会话。请先调用 register_db 进行注册。"
            )
        # 获取会话工厂的本地引用
        session_factory = async_session_factories[db]
        await logger.adebug(
            "已获取会话工厂",
            db=db_name,
            factory_repr=repr(session_factory),
            emoji="🔧",
        )

    # 在锁外创建会话
    if session_factory is None:
        # 理论上不应发生，但作为防御性编程添加检查
        await logger.aerror("获取锁后会话工厂仍为 None", db=db_name, emoji="🤯")
        raise RuntimeError(f"无法为数据库 '{db_name}' 获取会话工厂。")

    async_session: AsyncSession = session_factory()
    session_id = id(async_session)  # 获取会话ID用于跟踪
    await logger.adebug(
        "已创建新的数据库会话",
        db=db_name,
        session_id=session_id,
        emoji="✨",
    )

    try:
        yield async_session
        # 提交事务
        await async_session.commit()
        await logger.adebug(
            "数据库会话提交成功",
            db=db_name,
            session_id=session_id,
            emoji="✅",
        )
    except Exception as e:
        # 发生异常时回滚事务
        await async_session.rollback()
        # 使用 aerror 记录异常信息和堆栈
        await logger.aerror(
            "数据库会话因异常回滚",
            db=db_name,
            session_id=session_id,
            error=str(e),
            exc_info=True,  # 包含堆栈信息
            emoji="⏪",
        )
        raise  # 重新抛出异常，让上层处理
    finally:
        # 确保会话始终被关闭
        await async_session.close()
        await logger.ainfo(
            "数据库会话已关闭",
            db=db_name,
            session_id=session_id,
            emoji="🔒",
        )
        await logger.adebug("退出 get_db_session 函数", db=db_name, emoji="🚪")


async def release_all_db_engines() -> None:
    """
    线程安全地释放所有注册的数据库引擎。

    Returns:
        None
    """
    await logger.ainfo("开始释放所有数据库引擎", emoji="🏁")
    engines_to_dispose: List[Tuple[Database, AsyncEngine]] = []
    async with _db_lock:
        # 复制列表以在锁外操作
        engines_to_dispose = list(db_engines.items())
        db_names_to_clear = [d.value for d in db_engines.keys()]  # 显示中文枚举值
        await logger.adebug(
            "已获取锁，准备清理引擎和工厂",
            engines_count=len(engines_to_dispose),
            factory_count=len(async_session_factories),
            emoji="🔒",
        )
        db_engines.clear()
        async_session_factories.clear()
        await logger.adebug(
            "已清理内部引擎和工厂字典",
            cleared_dbs=db_names_to_clear,
            emoji="🧹",
        )

    # 在锁外释放引擎
    dispose_tasks = []
    for db, engine in engines_to_dispose:
        db_name = db.value
        await logger.adebug(
            "开始释放引擎",
            db=db_name,
            engine_repr=repr(engine),
            emoji="💨",
        )
        dispose_tasks.append(engine.dispose())
        # 记录每个引擎的释放启动
        await logger.ainfo("正在释放数据库引擎", db=db_name, emoji="🔧")

    # 并发执行所有 dispose 操作
    results = await asyncio.gather(*dispose_tasks, return_exceptions=True)

    # 检查释放结果
    all_successful = True
    for (db, _), result in zip(engines_to_dispose, results):
        db_name = db.value
        if isinstance(result, Exception):
            all_successful = False
            await logger.aerror(
                "释放引擎失败",
                db=db_name,
                error=str(result),
                exc_info=result,  # 传递异常对象以记录堆栈
                emoji="💥",
            )
        else:
            await logger.ainfo(
                "数据库引擎已成功释放",
                db=db_name,
                emoji="✅",
            )

    if all_successful:
        await logger.ainfo("所有数据库引擎已成功释放", emoji="🎉")
    else:
        await logger.awarning("部分数据库引擎未能正确释放", emoji="⚠️")
    await logger.ainfo("完成释放所有数据库引擎", emoji="🏁")
