import importlib
import logging

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from alphapower.config.settings import DATABASES
from alphapower.internal.utils import setup_logging

# 配置日志
logger = logging.getLogger(__name__)

# 创建异步数据库引擎
engines = {
    db_name: create_async_engine(
        db_config["url"],
        echo=False,
    )
    for db_name, db_config in DATABASES.items()
}
# 创建异步会话工厂
SessionFactories = {
    db_name: sessionmaker(
        bind=engine, class_=AsyncSession, autocommit=False, autoflush=False
    )
    for db_name, engine in engines.items()
}


async def get_db(db_name):
    """
    获取指定数据库的异步会话。

    参数:
    db_name (str): 数据库名称

    返回:
    sqlalchemy.ext.asyncio.AsyncSession: 异步数据库会话。
    """
    if db_name not in SessionFactories:
        raise ValueError(f"未知的数据库名称: {db_name}")
    try:
        # 动态加载对应库的 Base
        module_name = f"worldquant.entity.{db_name}"
        module = importlib.import_module(module_name)
        if not hasattr(module, "Base"):
            raise ValueError(f"模块 {module_name} 中未定义 Base")

        engine = engines[db_name]
        # 异步创建所有表
        async with engine.begin() as conn:
            await conn.run_sync(module.Base.metadata.create_all)

        async with SessionFactories[db_name]() as session:
            yield session
    except Exception as e:
        logger.error(f"获取数据库会话时出错: {e}")
        raise


async def close_resources():
    """
    异步关闭所有数据库连接和释放资源。
    """
    for db_name, engine in engines.items():
        await engine.dispose()  # 异步释放数据库引擎资源
        logger.info(f"数据库引擎 '{db_name}' 已释放。")
