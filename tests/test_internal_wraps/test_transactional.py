import pytest
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession
from alphapower.internal.wraps.transactional import Transactional, Propagation


@pytest.mark.asyncio
async def test_transactional_required_with_existing_transaction():
    session = AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.begin = AsyncMock()

    @Transactional(propagation=Propagation.REQUIRED)
    async def test_func(session: AsyncSession):
        if session.in_transaction() or session.in_nested_transaction():
            return "success"
        return "fail"

    result = await test_func(session)
    assert result == "success"
    session.begin.assert_not_called()


@pytest.mark.asyncio
async def test_transactional_required_without_existing_transaction():
    session = AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = False
    session.begin = AsyncMock()

    @Transactional(propagation=Propagation.REQUIRED)
    async def test_func(session: AsyncSession):
        if session.in_transaction() or session.in_nested_transaction():
            return "success"
        return "fail"

    result = await test_func(session)
    assert result == "success"
    session.begin.assert_awaited_once()


@pytest.mark.asyncio
async def test_transactional_requires_new():
    session = AsyncMock(spec=AsyncSession)
    session.begin = AsyncMock()

    @Transactional(propagation=Propagation.REQUIRES_NEW)
    async def test_func(session: AsyncSession):
        if session.in_transaction():
            return "success"
        return "fail"

    result = await test_func(session)
    assert result == "success"
    session.begin.assert_awaited_once()


@pytest.mark.asyncio
async def test_transactional_nested_with_existing_transaction():
    session = AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.begin_nested = AsyncMock()

    @Transactional(propagation=Propagation.NESTED)
    async def test_func(session: AsyncSession):
        if session.in_transaction() or session.in_nested_transaction():
            return "success"
        return "fail"

    result = await test_func(session)
    assert result == "success"
    session.begin_nested.assert_awaited_once()


@pytest.mark.asyncio
async def test_transactional_nested_without_existing_transaction():
    session = AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = False
    session.begin = AsyncMock()

    @Transactional(propagation=Propagation.NESTED)
    async def test_func(session: AsyncSession):
        if session.in_transaction() or session.in_nested_transaction():
            return "success"
        return "fail"

    result = await test_func(session)
    assert result == "success"
    session.begin.assert_awaited_once()
