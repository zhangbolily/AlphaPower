import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar

from aiohttp import ClientResponseError

from alphapower.internal.logging import setup_logging

# 配置日志
logger = setup_logging(__name__)

T = TypeVar("T", bound=Callable[..., Awaitable])


def exception_handler(func: T) -> T:
    """
    异常处理装饰器。

    该装饰器用于捕获被装饰的异步函数中的异常，并记录错误日志。
    如果发生异常，会将其重新抛出。
    对于 429 错误（请求过多），会自动重试，最多重试 4 次，每次等待 5 分钟。

    参数:
        func (Callable[..., Awaitable]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Awaitable:
        retry_count = 0
        max_retries = 6
        wait_time = 300  # 5分钟，单位为秒

        while True:
            try:
                return await func(*args, **kwargs)
            except ClientResponseError as e:
                if e.status == 429 and retry_count < max_retries:
                    retry_count += 1
                    await logger.awarning(
                        f"请求过于频繁，HTTP 状态码: {e.status}，错误信息: {e.message}。"
                        f"将在 {wait_time} 秒后进行第 {retry_count} 次重试，共 {max_retries} 次。"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                elif e.status == 429:
                    await logger.aerror(
                        f"请求过于频繁，已达到最大重试次数 {max_retries}，HTTP 状态码: {e.status}，错误信息: {e.message}",
                    )
                    raise
                else:
                    await logger.aerror(
                        f"请求失败，HTTP 状态码: {e.status}，错误信息: {e.message}",
                    )
                    raise
            except Exception as e:
                await logger.aerror(
                    f"执行 {func.__name__} 时发生异常: {e}",
                )
                raise

    return wrapper  # type: ignore
