"""
应用程序配置模块
该模块定义了应用程序的配置类，包括数据库配置、日志配置和凭据配置。
"""

import multiprocessing
import os
import pathlib
from typing import Dict

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .constants import Database, Environment


class DatabaseConfig(BaseSettings):
    """
    数据库配置类，用于定义数据库的 URL、描述和基类。
    """

    alias: str
    dsn: AnyUrl = Field(default_factory=lambda url: AnyUrl(url=url))
    description: str = ""


class CredentialConfig(BaseSettings):
    """
    凭据配置类，用于定义用户名和密码。
    """

    username: str = ""
    password: str = ""


class AppConfig(BaseSettings):
    """
    应用程序配置类，用于定义数据库、日志和凭据的相关配置。
    """

    databases: Dict[Database, DatabaseConfig] = {
        Database.ALPHAS: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///file:db/alphas.db?uri=true"),
            description="Alpha 数据库",
            alias=Database.ALPHAS.value,
        ),
        Database.DATA: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///file:db/data.db?uri=true"),
            description="数据集数据库",
            alias=Database.DATA.value,
        ),
        Database.SIMULATION: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///file:db/simulation.db?uri=true"),
            description="模拟回测任务数据库",
            alias=Database.SIMULATION.value,
        ),
        Database.EVALUATE: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///file:db/evaluate.db?uri=true"),
            description="检查数据库",
            alias=Database.EVALUATE.value,
        ),
    }

    asyncio_max_workers: int = 64
    credential: CredentialConfig = CredentialConfig()
    environment: str = Environment.PROD.value
    log_dir: str = "./logs"
    log_file_backup_count: int = 3
    log_file_max_bytes: int = 32 * 1024 * 1024  # 32 MB
    log_level: str = "INFO"
    sql_log_level: str = "WARNING"

    root_dir: pathlib.Path = Field(
        default_factory=lambda: (
            pathlib.Path().home().joinpath(".alphapower").absolute()
        )
    )

    model_config = SettingsConfigDict(
        env_file=f".env.{os.getenv('ENVIRONMENT', 'default')}",
        env_nested_delimiter="__",
        env_ignore_empty=True,
    )


# 加载配置
settings = AppConfig()

pathlib.Path(settings.root_dir).mkdir(parents=True, exist_ok=True)
os.chdir(settings.root_dir)
print(f"当前工作目录: {os.getcwd()}")


def setup_multiprocessing_context() -> None:
    """
    设置多进程上下文为 spawn 模式。

    相关性计算的场景需要使用 spawn 模式来避免 fork 可能导致的死锁问题
    此外，需要提高并行度，还需提高默认的线程池大小
    """
    multiprocessing.set_start_method("spawn", force=True)

    # 设置 asyncio 的最大工作线程数
    if settings.asyncio_max_workers > 0:
        # 设置 asyncio 最大工作线程数，PYTHONASYNCIO_MAX_WORKERS 仅在 Python 3.11+ 生效
        # 注意：此环境变量必须在 asyncio 事件循环创建前设置，否则不会生效
        os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(settings.asyncio_max_workers)
