from typing import Any, Awaitable, Callable

from alphapower.internal.storage import get_db

from sqlalchemy.ext.asyncio import AsyncSession


def with_session(
    db_name: str = "default",
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """
    装饰器，用于在异步函数中注入数据库会话。

    参数:
    db_name (str): 数据库名称，默认为 "default"。

    返回:
    function: 包装后的异步函数。
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with get_db(db_name) as session:
                return await func(session, *args, **kwargs)

        return wrapper

    return decorator


def transactional(
    nested_transaction: bool = False,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """
    装饰器，用于在异步函数中注入数据库事务。

    参数:
    session (sqlalchemy.ext.asyncio.AsyncSession): 数据库会话。

    返回:
    function: 包装后的异步函数。
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            session: AsyncSession = args[0]

            if session.in_transaction() and not nested_transaction:
                return await func(*args, **kwargs)
            elif session.in_transaction() and nested_transaction:
                async with session.begin_nested():
                    return await func(*args, **kwargs)
            else:
                async with session.begin():
                    return await func(*args, **kwargs)

        return wrapper

    return decorator
