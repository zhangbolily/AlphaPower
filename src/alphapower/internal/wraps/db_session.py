"""
数据库会话管理
"""

from typing import Any, Callable, Coroutine


from alphapower.internal.storage import get_db_session


def with_session(
    db_name: str = "default",
) -> Callable[
    [Callable[..., Coroutine[Any, Any, None]]],
    Callable[..., Coroutine[Any, Any, None]],
]:
    """
    装饰器，用于在异步函数中注入数据库会话。

    参数:
    db_name (str): 数据库名称，默认为 "default"。

    返回:
    function: 包装后的异步函数。
    """

    def decorator(
        func: Callable[..., Coroutine[Any, Any, None]],
    ) -> Callable[..., Coroutine[Any, Any, None]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with get_db_session(db_name) as session:
                return await func(session, *args, **kwargs)

        return wrapper

    return decorator
