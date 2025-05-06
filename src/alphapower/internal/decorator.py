import functools
import time
from typing import Any, Callable, Coroutine, TypeVar, cast

from structlog.stdlib import BoundLogger

from alphapower.internal.logging import get_logger

F = TypeVar("F", bound=Callable[..., Coroutine[Any, Any, Any]])
E = TypeVar("E", bound=Callable[..., Coroutine[Any, Any, Any]])

logger: BoundLogger = get_logger(__name__)


def async_timed(func: F) -> F:
    """
    异步函数耗时统计装饰器，自动记录函数执行耗时到日志
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        timed_function: str = func.__qualname__  # 确保获取到被装饰函数的名字
        await logger.adebug(
            "函数耗时统计开始",
            timed_function=timed_function,
            args=args,
            kwargs=kwargs,
            emoji="⏱️",
        )
        start_time: float = time.perf_counter()
        result = await func(*args, **kwargs)
        elapsed: float = time.perf_counter() - start_time
        await logger.ainfo(
            "函数耗时统计结束",
            timed_function=timed_function,
            elapsed=f"{elapsed:.4f} seconds",
            emoji="⏱️",
        )
        return result

    return cast(F, wrapper)


def async_exception_handler(func: E) -> E:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        decorated_func: str = func.__qualname__  # 获取被装饰函数的名字
        try:
            await logger.adebug(
                "函数异常处理开始",
                decorated_func=decorated_func,
                args=args,
                kwargs=kwargs,
                emoji="⚠️",
            )
            return await func(*args, **kwargs)
        except Exception as e:
            await logger.aerror(
                "函数执行时捕获到异常",
                decorated_func=decorated_func,
                exception=str(e),
                emoji="❌",
            )

            raise e
        finally:
            await logger.adebug(
                "函数异常处理结束，没有捕获到异常",
                decorated_func=decorated_func,
                args=args,
                kwargs=kwargs,
                emoji="✅",
            )

    return cast(E, wrapper)
