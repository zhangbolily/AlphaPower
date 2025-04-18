"""
异常处理模块。

该模块提供了一个通用的异常处理装饰器，用于捕获异步函数中的异常并记录日志。
支持对特定的 HTTP 429 错误（请求过多）进行自动重试，最大重试次数和等待时间可配置。
日志记录遵循项目规范，包含函数名、入参、异常信息等详细内容，便于调试和排查问题。

主要功能:
- 捕获异步函数中的异常并记录日志。
- 对 HTTP 429 错误进行自动重试。
- 提供详细的日志记录，包括异常堆栈信息。

模块依赖:
- asyncio: 用于异步操作。
- aiohttp.ClientResponseError: 捕获 HTTP 请求相关的异常。
- alphapower.internal.logging: 用于日志记录，遵循 structlog 风格。

使用方法:
- 使用 `@exception_handler` 装饰器装饰需要捕获异常的异步函数。
"""

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar

from aiohttp import ClientResponseError

from alphapower.internal.logging import get_logger

# 配置日志
logger = get_logger(__name__)

T = TypeVar("T", bound=Callable[..., Awaitable])


def exception_handler(func: T) -> T:
    """
    异常处理装饰器。

    该装饰器用于捕获被装饰的异步函数中的异常，并记录错误日志。
    如果发生异常，会将其重新抛出。
    对于 429 错误（请求过多），会自动重试，最多重试 6 次，每次等待 5 分钟。

    参数:
        func (Callable[..., Awaitable]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Awaitable[Any]:
        retry_count: int = 0
        max_retries: int = 6
        wait_time: int = 300  # 5分钟，单位为秒
        func_name: str = func.__name__

        # 记录函数调用的 DEBUG 日志
        await log_function_entry(func_name, args, kwargs)

        while True:
            try:
                result = await func(*args, **kwargs)
                await log_function_success(func_name, result)
                return result
            except ClientResponseError as e:
                if e.status == 429 and retry_count < max_retries:
                    retry_count += 1
                    await log_retry_warning(
                        func_name, e, retry_count, max_retries, wait_time
                    )
                    await asyncio.sleep(wait_time)
                    continue
                elif e.status == 429:
                    await log_max_retry_error(func_name, e, max_retries)
                    raise
                else:
                    await log_request_error(func_name, e)
                    raise
            except Exception as e:
                await log_generic_error(func_name, e)
                raise

    return wrapper  # type: ignore


async def log_function_entry(func_name: str, args: Any, kwargs: Any) -> None:
    """记录函数进入的 DEBUG 日志。"""
    await logger.adebug(
        "进入函数",
        wrapped_func_name=func_name,
        args=args,
        kwargs=kwargs,
        module_name=__name__,
        emoji="🚀",
    )


async def log_function_success(func_name: str, result: Any) -> None:
    """记录函数成功返回的 DEBUG 日志。"""
    await logger.adebug(
        "函数执行成功",
        wrapped_func_name=func_name,
        result=str(result)[:500],
        module_name=__name__,
        emoji="✅",
    )


async def log_retry_warning(
    func_name: str,
    error: ClientResponseError,
    retry_count: int,
    max_retries: int,
    wait_time: int,
) -> None:
    """记录请求过于频繁的 WARNING 日志。"""
    await logger.awarning(
        "请求过于频繁",
        wrapped_func_name=func_name,
        status_code=error.status,
        error_message=str(error),
        retry_count=retry_count,
        max_retries=max_retries,
        wait_time=wait_time,
        module_name=__name__,
        emoji="⏳",
    )


async def log_max_retry_error(
    func_name: str, error: ClientResponseError, max_retries: int
) -> None:
    """记录达到最大重试次数的 ERROR 日志。"""
    await logger.aerror(
        "请求过于频繁，达到最大重试次数",
        wrapped_func_name=func_name,
        status_code=error.status,
        error_message=str(error),
        max_retries=max_retries,
        module_name=__name__,
        stack_info=True,
        emoji="❌",
    )


async def log_request_error(func_name: str, error: ClientResponseError) -> None:
    """记录请求失败的 ERROR 日志。"""
    await logger.aerror(
        "请求失败",
        wrapped_func_name=func_name,
        status_code=error.status,
        error_message=str(error),
        module_name=__name__,
        stack_info=True,
        emoji="❌",
    )


async def log_generic_error(func_name: str, error: Exception) -> None:
    """记录通用异常的 ERROR 日志。"""
    await logger.aerror(
        "执行函数时发生异常",
        wrapped_func_name=func_name,
        error_message=str(error),
        module_name=__name__,
        stack_info=True,
        emoji="🔥",
    )
