"""
限流处理器
"""

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable

from alphapower.internal.logging import setup_logging

from .models import RateLimit

# 配置日志
logger = setup_logging(__name__)


def rate_limit_handler(
    func: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """
    一个装饰器，用于处理速率限制。

    如果被装饰的异步函数返回的结果包含 RateLimit 对象，
    且速率限制已达到（remaining 为 0），
    则会等待指定的重置时间后重试该函数。

    参数:
        func (Callable[..., Awaitable[Any]]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable[Any]]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        response = await func(*args, **kwargs)
        if isinstance(response, tuple) and isinstance(response[-1], RateLimit):
            rate_limit = response[-1]
            if rate_limit.remaining == 0:
                logger.warning("已达到速率限制。将在 %s 秒后重试。", rate_limit.reset)
                await asyncio.sleep(rate_limit.reset)
                response = await func(*args, **kwargs)
        return response

    return wrapper
