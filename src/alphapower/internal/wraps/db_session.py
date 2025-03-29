"""
数据库会话管理
"""

from typing import Any, Callable, Coroutine, Never


from alphapower.internal.storage import get_db


def with_session(
    db_name: str = "default",
) -> Callable[
    [Callable[..., Coroutine[Any, Any, Never]]],
    Callable[..., Coroutine[Any, Any, Never]],
]:
    """
    装饰器，用于在异步函数中注入数据库会话。

    参数:
    db_name (str): 数据库名称，默认为 "default"。

    返回:
    function: 包装后的异步函数。
    """

    def decorator(
        func: Callable[..., Coroutine[Any, Any, Never]],
    ) -> Callable[..., Coroutine[Any, Any, Never]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with get_db(db_name) as session:
                return await func(session, *args, **kwargs)

        return wrapper

    return decorator
