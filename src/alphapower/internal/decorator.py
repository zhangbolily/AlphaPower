import functools
import time
from typing import Any, Callable, Coroutine, TypeVar, cast

from structlog.stdlib import BoundLogger

from alphapower.internal.logging import get_logger

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])

logger: BoundLogger = get_logger(__name__)


def async_timed(func: F) -> F:
    """
    异步函数耗时统计装饰器，自动记录函数执行耗时到日志
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        await logger.adebug(
            "函数耗时统计开始",
            func_name=func.__qualname__,
            args=args,
            kwargs=kwargs,
            emoji="⏱️",
        )
        start_time: float = time.perf_counter()
        result = await func(*args, **kwargs)
        elapsed: float = time.perf_counter() - start_time
        await logger.ainfo(
            "函数耗时统计结束",
            func_name=func.__qualname__,
            elapsed=elapsed,
            result=result,
            emoji="✅",
        )
        return result

    return cast(F, wrapper)
