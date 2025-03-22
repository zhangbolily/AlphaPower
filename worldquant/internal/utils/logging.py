import logging
import os
from logging.handlers import RotatingFileHandler

from worldquant.config.settings import LOG_DIR, LOG_LEVEL


def setup_logging(module_name: str, enable_console: bool = True):
    """
    配置日志记录器，支持控制台和文件输出。

    参数:
    module_name (str): 模块名称，用于区分日志文件。
    enable_console (bool): 是否启用控制台日志输出，默认为 True。
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger = logging.getLogger(module_name)
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False

    # 禁用默认的控制台日志处理器
    logger.handlers = []

    # 文件日志处理器
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, f"{module_name}.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setLevel(LOG_LEVEL)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # 控制台日志处理器（可选）
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger
