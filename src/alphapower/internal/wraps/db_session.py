from typing import Any, Awaitable, Callable, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.internal.storage import get_db


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
    nested_transaction (bool): 是否使用嵌套事务，默认为 False。

    返回:
    function: 包装后的异步函数。
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            session: Optional[AsyncSession] = next(
                (arg for arg in args if isinstance(arg, AsyncSession)),
                next(
                    (
                        value
                        for value in kwargs.values()
                        if isinstance(value, AsyncSession)
                    ),
                    None,
                ),
            )

            if not session:
                raise ValueError("入参没有找到 AsyncSession 对象。")

            transaction_context = (
                session.begin_nested()
                if (session.in_transaction() or session.in_nested_transaction())
                and nested_transaction
                else session.begin() if not session.in_transaction() else None
            )

            if transaction_context:
                async with transaction_context as tx_session:
                    for arg in args:
                        if isinstance(arg, AsyncSession):
                            args = tuple(tx_session if a == arg else a for a in args)
                    for key, value in kwargs.items():
                        if isinstance(value, AsyncSession):
                            kwargs[key] = tx_session

                    return await func(*args, **kwargs)
            else:
                return await func(*args, **kwargs)

        return wrapper

    return decorator
