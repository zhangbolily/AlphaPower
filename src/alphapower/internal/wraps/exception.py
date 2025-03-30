from functools import wraps
from typing import Callable, TypeVar, Awaitable, Any

from alphapower.internal.utils import setup_logging

# 配置日志
logger = setup_logging(__name__)

T = TypeVar("T", bound=Callable[..., Awaitable])


def exception_handler(func: T) -> T:
    """
    异常处理装饰器。

    该装饰器用于捕获被装饰的异步函数中的异常，并记录错误日志。
    如果发生异常，会将其重新抛出。

    参数:
        func (Callable[..., Awaitable]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Awaitable:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error("执行 %s 时发生异常: %s", func.__name__, e, exc_info=True)
            raise

    return wrapper  # type: ignore
