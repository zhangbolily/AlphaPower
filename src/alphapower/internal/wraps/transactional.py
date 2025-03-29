import asyncio
from contextlib import asynccontextmanager
from enum import Enum
from functools import wraps
from typing import Callable, Optional

from sqlalchemy.ext.asyncio import AsyncSession


class Propagation(Enum):
    REQUIRED = "required"
    REQUIRES_NEW = "requires_new"
    NESTED = "nested"


class Transactional:
    _class_lock = asyncio.Lock()

    def __init__(
        self,
        propagation: Propagation = Propagation.REQUIRED,
    ):
        self.propagation = propagation

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            session = self._find_session(*args, **kwargs)
            if not session:
                raise ValueError("AsyncSession not found")

            async with self._class_lock:
                async with self._begin_transaction(session):
                    return await func(*args, **kwargs)

        return wrapper

    @staticmethod
    def _find_session(*args, **kwargs) -> Optional[AsyncSession]:
        for arg in args:
            if isinstance(arg, AsyncSession):
                return arg
        return kwargs.get("session")

    @asynccontextmanager
    async def _begin_transaction(self, session: AsyncSession):
        existing_transaction = session.in_transaction()

        if self.propagation == Propagation.REQUIRED:
            if existing_transaction:
                yield
                return
            async with await session.begin() as tx:  # Await session.begin()
                yield tx

        elif self.propagation == Propagation.REQUIRES_NEW:
            async with await session.begin() as tx:  # Await session.begin()
                yield tx

        elif self.propagation == Propagation.NESTED:
            if existing_transaction:
                async with (
                    await session.begin_nested()
                ) as tx:  # Await session.begin_nested()
                    yield tx
            else:
                async with await session.begin() as tx:  # Await session.begin()
                    yield tx
