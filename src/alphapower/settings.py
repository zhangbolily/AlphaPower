"""
应用程序配置模块
该模块定义了应用程序的配置类，包括数据库配置、日志配置和凭据配置。
"""

import os
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
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/alphas.db"),
            description="Alpha 数据库",
            alias=Database.ALPHAS.value,
        ),
        Database.DATA: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/data.db"),
            description="数据集数据库",
            alias=Database.DATA.value,
        ),
        Database.SIMULATION: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/simulation.db"),
            description="模拟回测任务数据库",
            alias=Database.SIMULATION.value,
        ),
        Database.EVALUATE: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/evaluate.db"),
            description="检查数据库",
            alias=Database.EVALUATE.value,
        ),
    }

    log_level: str = "INFO"
    log_dir: str = "./logs"
    log_file_max_bytes: int = 32 * 1024 * 1024  # 5 MB
    log_file_backup_count: int = 3
    sql_echo: bool = True
    environment: str = Environment.PROD.value
    credential: CredentialConfig = CredentialConfig()

    model_config = SettingsConfigDict(
        env_file=f".env.{os.getenv('ENVIRONMENT', 'default')}",
        env_nested_delimiter="__",
        env_ignore_empty=True,
    )


# 加载配置
settings = AppConfig()
