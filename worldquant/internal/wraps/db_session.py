from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession
from worldquant.storage import get_db


@asynccontextmanager
async def get_db_session(db_name: str) -> AsyncGenerator[AsyncSession, None]:
    """
    异步上下文管理器，用于获取数据库会话。

    参数:
    db_name (str): 数据库名称。

    返回:
    sqlalchemy.ext.asyncio.AsyncSession: 异步数据库会话。
    """
    db_generator: AsyncGenerator[AsyncSession, None] = get_db(db_name)
    session: AsyncSession = await anext(db_generator)
    try:
        yield session
    finally:
        await session.close()


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
            async with get_db_session(db_name) as session:
                return await func(session, *args, **kwargs)

        return wrapper

    return decorator
