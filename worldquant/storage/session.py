import importlib
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from worldquant.config.settings import DATABASES

# 配置日志
logger = logging.getLogger(__name__)

# 创建数据库引擎
engines = {
    db_name: create_engine(config["url"], echo=False)
    for db_name, config in DATABASES.items()
}

# 创建线程安全的会话工厂
SessionFactories = {
    db_name: scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    for db_name, engine in engines.items()
}


def get_db(db_name):
    """
    获取指定数据库的会话。

    参数:
    db_name (str): 数据库名称

    返回:
    sqlalchemy.orm.Session: 数据库会话。
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
        # 创建所有表
        module.Base.metadata.create_all(bind=engine)
        session = SessionFactories[db_name]()
        yield session
    except Exception as e:
        logger.error(f"获取数据库会话时出错: {e}")
        raise
    finally:
        session.close()


def close_resources():
    """
    关闭所有数据库连接和释放资源。
    """
    for db_name, session_factory in SessionFactories.items():
        session_factory.remove()  # 移除线程本地的会话
        logger.info(f"数据库会话工厂 '{db_name}' 已关闭。")
    for db_name, engine in engines.items():
        engine.dispose()  # 释放数据库引擎资源
        logger.info(f"数据库引擎 '{db_name}' 已释放。")
