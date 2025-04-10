"""
日志模块
"""

import asyncio
import logging
import os
import threading
from logging.handlers import RotatingFileHandler
from typing import Any, Mapping, MutableMapping

import structlog

from alphapower.settings import settings


def unicode_decoder(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> Mapping[str, Any]:
    """
    确保日志内容支持中文输出。
    """
    for key, value in event_dict.items():
        if isinstance(value, bytes):
            event_dict[key] = value.decode("utf-8")
    return event_dict


def add_coroutine_id(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> Mapping[str, Any]:
    """
    添加当前协程ID到日志事件中。

    如果当前是在协程中运行，则添加协程ID；否则添加线程ID。
    """
    try:
        # 尝试获取当前协程的任务
        task = asyncio.current_task()
        if task:
            # 如果是协程，添加协程ID
            event_dict["coroutine_id"] = id(task)
        else:
            # 如果不是协程，添加线程ID
            event_dict["thread_id"] = threading.get_ident()
    except RuntimeError:
        # 如果不在事件循环中运行，添加线程ID
        event_dict["thread_id"] = threading.get_ident()

    return event_dict


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

    # 修改标准日志格式
    log_format = (
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(module)s.%(funcName)s:%(lineno)d - %(message)s"
    )
    formatter = logging.Formatter(log_format)

    # 为特定模块创建日志记录器
    logger = logging.getLogger(module_name)
    # 清除之前的处理器，避免重复输出
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    # 重置日志级别
    logger.setLevel(settings.log_level)
    # 阻止传播到根日志记录器，确保日志只发送到我们配置的处理器
    logger.propagate = False

    # 配置文件日志处理器
    file_handler = RotatingFileHandler(
        os.path.join(settings.log_dir, f"{module_name}.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(settings.log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 配置控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(settings.log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 更新 structlog 配置
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.CallsiteParameterAdder(
                parameters={
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.LINENO,
                    structlog.processors.CallsiteParameter.PATHNAME,
                }
            ),
            add_coroutine_id,  # 添加协程ID处理器
            unicode_decoder,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(module_name)
