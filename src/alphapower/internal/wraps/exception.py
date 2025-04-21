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
    对于 429 错误（请求过多），会自动重试，最大重试次数和等待时间可配置。

    参数:
        func (Callable[..., Awaitable]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Awaitable[Any]:
        retry_count: int = 0
        max_retries: int = 6
        wait_time: int = 5  # 5S
        func_name: str = func.__name__

        # 记录函数调用的 DEBUG 日志
        await log_function_entry(func_name, args, kwargs)

        while True:
            try:
                result = await func(*args, **kwargs)
                await log_function_success(func_name, result)
                return result
            except ClientResponseError as e:
                try:
                    should_retry = await _handle_http_error(
                        func_name, e, retry_count, max_retries, wait_time
                    )
                    if should_retry:
                        retry_count += 1
                        continue
                    else:
                        raise
                except asyncio.CancelledError:
                    # 捕获任务取消异常，记录日志并重新抛出
                    await logger.awarning(
                        "任务被取消",
                        wrapped_func_name=func_name,
                        module_name=__name__,
                        emoji="🛑",
                    )
                    raise
            except Exception as e:
                await log_generic_error(func_name, e)
                raise

    return wrapper  # type: ignore


async def _handle_http_error(
    func_name: str,
    error: ClientResponseError,
    retry_count: int,
    max_retries: int,
    wait_time: int,
) -> bool:
    """
    统一处理 HTTP 错误代码。

    参数:
        func_name (str): 函数名称。
        error (ClientResponseError): 捕获的 HTTP 异常。
        retry_count (int): 当前重试次数。
        max_retries (int): 最大重试次数。
        wait_time (int): 每次重试的等待时间（秒）。

    返回:
        bool: 是否需要重试。
    """
    if error.status in (429, 502, 504) and retry_count < max_retries:
        await log_retry_warning(
            func_name, error, retry_count + 1, max_retries, wait_time
        )
        await asyncio.sleep(wait_time)
        return True
    elif error.status in (429, 502, 504):
        await log_max_retry_error(func_name, error, max_retries)
        raise error
    elif error.status == 400:  # 错误请求（Bad Request）
        # TODO: 实现 400 错误的处理逻辑
        await logger.awarning(
            "捕获到 400 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="⚠️",
        )
        raise error
    elif error.status == 401:  # 未授权（Unauthorized）
        # TODO: 实现 401 错误的处理逻辑
        await logger.awarning(
            "捕获到 401 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="🔒",
        )
        raise error
    elif error.status == 403:  # 禁止访问（Forbidden）
        # TODO: 实现 403 错误的处理逻辑
        await logger.awarning(
            "捕获到 403 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="🚫",
        )
        raise error
    elif error.status == 404:  # 未找到（Not Found）
        # TODO: 实现 404 错误的处理逻辑
        await logger.awarning(
            "捕获到 404 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="❓",
        )
        raise error
    elif error.status == 500:  # 服务器内部错误（Internal Server Error）
        # TODO: 实现 500 错误的处理逻辑
        await logger.aerror(
            "捕获到 500 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="💥",
        )
        raise error
    elif error.status == 503:  # 服务不可用（Service Unavailable）
        # TODO: 实现 503 错误的处理逻辑
        await logger.aerror(
            "捕获到 503 错误",
            wrapped_func_name=func_name,
            status_code=error.status,
            error_message=str(error),
            module_name=__name__,
            emoji="🛑",
        )
        raise error
    else:
        # 未知错误，记录日志并抛出
        await log_request_error(func_name, error)
        raise error

    return False


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
