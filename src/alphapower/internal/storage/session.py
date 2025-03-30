import importlib
from asyncio import Lock
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from alphapower.config.settings import DATABASES
from alphapower.entity import AlphaBase, DataBase, SimulationBase
from alphapower.internal.utils import setup_logging

# 配置日志
logger = setup_logging(__name__)

# 创建异步数据库引擎
engines = {
    db_name: create_async_engine(
        db_config["url"],
        echo=True,
    )
    for db_name, db_config in DATABASES.items()
}

all_entity_bases = (AlphaBase, DataBase, SimulationBase)


SessionFactories: dict[str, async_sessionmaker] = {
    db_name: async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )
    for db_name, engine in engines.items()
}

# 创建一个全局异步锁
lock = Lock()


@lru_cache
def load_module(db_name: str) -> object:
    """
    动态加载模块并返回模块对象。

    参数:
    db_name (str): 数据库名称

    返回:
    module: 动态加载的模块对象

    异常:
    ValueError: 如果模块未定义 Base 或加载失败。
    """
    module_name = f"alphapower.internal.{db_name}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        raise ValueError(f"无法加载模块 {module_name}: {e}") from e

    if not hasattr(module, "Base"):
        raise ValueError(f"模块 {module_name} 中未定义 Base")

    return module


async def create_tables_if_needed(db_name: str) -> None:
    """
    创建数据库表，仅在首次访问时执行。

    参数:
    db_name (str): 数据库名称
    """
    db_config = DATABASES[db_name]
    base_class_name = db_config.get("base_class")
    if not base_class_name:
        raise ValueError(f"数据库配置 '{db_name}' 中未定义 base_class")

    # 动态获取 base_class
    base_class = globals().get(base_class_name)
    if not base_class:
        raise ValueError(f"无法解析 base_class '{base_class_name}'，请确保其已正确导入")
    elif base_class not in all_entity_bases:
        raise ValueError(
            f"base_class '{base_class_name}' 必须继承自 {all_entity_bases}"
        )

    engine = engines[db_name]
    async with engine.begin() as conn:
        await conn.run_sync(base_class.metadata.create_all)
    logger.info("数据库 '%s' 的表已初始化。", db_name)


@asynccontextmanager
async def get_db_session(db_name: str) -> AsyncGenerator[AsyncSession, None]:
    """
    获取指定数据库的异步会话。

    参数:
    db_name (str): 数据库名称

    返回:
    AsyncGenerator[AsyncSession, None]: 异步数据库会话生成器。
    """
    if db_name not in SessionFactories:
        raise ValueError(f"未知的数据库名称: {db_name}")
    try:
        # 使用异步锁保护对共享资源的访问
        async with lock:
            await create_tables_if_needed(db_name)
            session: AsyncSession = SessionFactories[db_name]()  # 显式声明 session 类型
        try:
            yield session
            # 显式提交事务，确保所有事务都已完成
            await session.commit()
        except Exception:
            # 如果发生异常，回滚事务
            await session.rollback()
            raise
        finally:
            # 确保会话关闭之前没有进行中的事务
            await session.close()
    except Exception as e:
        logger.exception("获取数据库会话时出错: %s", e)
        raise


async def close_resources() -> None:
    """
    异步关闭所有数据库连接和释放资源。
    """
    for db_name, engine in engines.items():
        await engine.dispose()  # 异步释放数据库引擎资源
        logger.info("数据库引擎 '%s' 已释放。", db_name)
