"""
日志模块
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import structlog

from alphapower.settings import settings


def setup_logging(
    module_name: str, enable_console: bool = True
) -> structlog.stdlib.BoundLogger:
    """
    配置日志记录器，支持控制台和文件输出。

    参数:
    module_name (str): 模块名称，用于区分日志文件。
    enable_console (bool): 是否启用控制台日志输出，默认为 True。
    """
    if not os.path.exists(settings.log_dir):
        os.makedirs(settings.log_dir)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 配置文件日志处理器
    file_handler = RotatingFileHandler(
        os.path.join(settings.log_dir, f"{module_name}.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(settings.log_level)
    file_handler.setFormatter(logging.Formatter(log_format))

    # 配置标准日志记录器
    handlers: list[logging.Handler] = [file_handler]
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(settings.log_level)
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)

    logging.basicConfig(level=settings.log_level, encoding="utf-8", handlers=handlers)

    # 配置 structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(module_name)
