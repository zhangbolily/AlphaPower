"""
日志模块
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Any, List, Mapping, MutableMapping

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


def get_logger(
    module_name: str, enable_console: bool = True
) -> structlog.stdlib.BoundLogger:
    """
    配置日志记录器，支持控制台和文件输出。

    使用 structlog 进行完全配置，为控制台提供彩色输出，为文件提供 JSON 输出。

    参数:
        module_name (str): 模块名称，用于区分日志文件和日志记录器名称。
        enable_console (bool): 是否启用控制台日志输出，默认为 True。

    返回:
        structlog.stdlib.BoundLogger: 配置好的 structlog 日志记录器实例。
    """
    log_dir: str = settings.log_dir
    log_level: str = settings.log_level.upper()

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # structlog 共享处理器
    shared_processors: list[Any] = [
        structlog.stdlib.add_logger_name,  # 添加日志记录器名称
        structlog.stdlib.add_log_level,  # 添加日志级别
        structlog.stdlib.add_log_level_number,  # 添加日志级别数字
        structlog.stdlib.PositionalArgumentsFormatter(),  # 格式化位置参数
        structlog.processors.TimeStamper(
            fmt="iso", utc=False, key="datetime"
        ),  # 添加 ISO 格式时间戳
        structlog.processors.StackInfoRenderer(),  # 添加堆栈信息
        structlog.processors.format_exc_info,  # 格式化异常信息
        structlog.processors.CallsiteParameterAdder(  # 添加调用点信息
            parameters={
                structlog.processors.CallsiteParameter.THREAD,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.LINENO,
                # structlog.processors.CallsiteParameter.PATHNAME, # 路径通常较长，暂不添加
            }
        ),
        unicode_decoder,  # 解码字节字符串为 UTF-8
    ]

    # 配置 structlog
    structlog.configure(
        processors=shared_processors
        + [
            # 这个处理器必须放在最后，它会将事件传递给标准 logging
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,  # 标准库绑定的 Logger
        cache_logger_on_first_use=True,  # 缓存 Logger 实例以提高性能
    )

    # --- 配置标准 logging ---

    # 1. 文件处理器 (JSON 格式)
    log_file = os.path.join(log_dir, f"{module_name}.log")
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=settings.log_file_max_bytes,
        backupCount=settings.log_file_backup_count,
        encoding="utf-8",
    )
    # 文件处理器的格式化器
    file_formatter = structlog.stdlib.ProcessorFormatter(
        # 使用标准的 ProcessorFormatter
        processor=structlog.processors.KeyValueRenderer(
            key_order=[
                "datetime",
                "level",
                "level_number",
                "logger",
                "thread",
                "coroutine_id",
                "module",
                "func_name",
                "lineno",
                "emoji",
                "event",
            ],
        ),
        foreign_pre_chain=shared_processors,
    )
    file_handler.setFormatter(file_formatter)

    handlers: List[logging.Handler] = [file_handler]

    # 2. 控制台处理器 (美化彩色输出)
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)  # 输出到标准输出
        console_handler.setFormatter(file_formatter)
        handlers.append(console_handler)

    # 获取并配置标准日志记录器
    logger = logging.getLogger(module_name)
    # 清除之前的处理器，避免重复添加
    if logger.hasHandlers():
        logger.handlers.clear()
    # 添加配置好的处理器
    for handler in handlers:
        logger.addHandler(handler)
    # 设置日志级别
    logger.setLevel(log_level)
    # 阻止传播到根日志记录器
    logger.propagate = False

    # 返回 structlog 包装的日志记录器
    return structlog.get_logger(module_name)
