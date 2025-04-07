import asyncio
import time
from functools import wraps
from typing import Any, Callable, Coroutine, TypeVar, cast

from alphapower.internal.logging import setup_logging

logger = setup_logging(__name__, enable_console=False)

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])


def log_time_elapsed(func: F) -> F:
    """ "
    装饰器，用于记录异步函数的执行时间和参数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        task_id: int = id(asyncio.current_task())
        logger.info(
            "[Task %s] 函数 %s 的入参: args=%s, kwargs=%s",
            task_id,
            func.__name__,
            list(map(str, args)),
            kwargs,
        )
        start_time: float = time.time()
        result: Any = await func(*args, **kwargs)
        elapsed_time: float = time.time() - start_time
        logger.info(
            "[Task %s] 函数 %s 耗时: %.2f 秒", task_id, func.__name__, elapsed_time
        )
        return result

    return cast(F, wrapper)
