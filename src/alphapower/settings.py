"""
应用程序配置模块
该模块定义了应用程序的配置类，包括数据库配置、日志配置和凭据配置。
"""

import os
from typing import Dict

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .constants import DB_ALPHAS, DB_DATA, DB_SIMULATION


class DatabaseConfig(BaseSettings):
    """
    数据库配置类，用于定义数据库的 URL、描述和基类。
    """

    url: str
    description: str
    base_class: str


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
            url="sqlite+aiosqlite:///db/alphas.db",
            description="用户回测因子的信息",
            base_class="AlphaBase",
        ),
        DB_DATA: DatabaseConfig(
            url="sqlite+aiosqlite:///db/data.db",
            description="数据集和数据字段的信息",
            base_class="DataBase",
        ),
        DB_SIMULATION: DatabaseConfig(
            url="sqlite+aiosqlite:///db/simulation.db",
            description="用户回测任务的信息",
            base_class="SimulationBase",
        ),
    }

    log_level: str = Field(default="INFO")
    log_dir: str = Field(default="./logs")

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
