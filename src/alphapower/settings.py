"""
应用程序配置模块
该模块定义了应用程序的配置类，包括数据库配置、日志配置和凭据配置。
"""

import os
from typing import Dict

from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .constants import DB_ALPHAS, DB_DATA, DB_SIMULATION, ENV_PROD


class DatabaseConfig(BaseSettings):
    """
    数据库配置类，用于定义数据库的 URL、描述和基类。
    """

    alias: str
    dsn: AnyUrl = Field(default_factory=lambda url: AnyUrl(url=url))
    description: str = Field(default="")


class CredentialsConfig(BaseSettings):
    """
    凭据配置类，用于定义用户名和密码。
    """

    username: str = Field(default="")
    password: str = Field(default="")


class AppConfig(BaseSettings):
    """
    应用程序配置类，用于定义数据库、日志和凭据的相关配置。
    """

    databases: Dict[str, DatabaseConfig] = {
        DB_ALPHAS: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/alphas.db"),
            description="Alpha 数据库",
            alias=DB_ALPHAS,
        ),
        DB_DATA: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/data.db"),
            description="数据集数据库",
            alias=DB_DATA,
        ),
        DB_SIMULATION: DatabaseConfig(
            dsn=AnyUrl(url="sqlite+aiosqlite:///db/simulation.db"),
            description="模拟回测任务数据库",
            alias=DB_SIMULATION,
        ),
    }

    log_level: str = Field(default="INFO")
    log_dir: str = Field(default="./logs")
    sql_echo: bool = Field(default=False)
    environment: str = Field(default=ENV_PROD)

    credentials: Dict[str, CredentialsConfig] = {
        "0": CredentialsConfig(),
        "1": CredentialsConfig(),
    }

    model_config = SettingsConfigDict(
        env_file=f".env.{os.getenv('ENVIRONMENT', 'default')}",
        env_nested_delimiter="__",
        env_ignore_empty=True,
    )


# 加载配置
settings = AppConfig()
