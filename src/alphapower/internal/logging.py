"""
日志模块
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import structlog

from alphapower.config.settings import LOG_DIR, LOG_LEVEL


def setup_logging(
    module_name: str, enable_console: bool = True
) -> structlog.BoundLogger:
    """
    配置日志记录器，支持控制台和文件输出。

    参数:
    module_name (str): 模块名称，用于区分日志文件。
    enable_console (bool): 是否启用控制台日志输出，默认为 True。
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 配置文件日志处理器
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, f"{module_name}.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(logging.Formatter(log_format))

    # 配置标准日志记录器
    handlers: list[logging.Handler] = [file_handler]
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL)
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)

    logging.basicConfig(level=LOG_LEVEL, handlers=handlers)

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
