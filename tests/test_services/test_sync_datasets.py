"""
测试数据集同步功能。
"""

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.internal.wraps import with_session
from alphapower.services.sync_datasets import sync_datasets


@with_session("data_test")
async def test_sync_datasets(session: AsyncSession) -> None:
    """
    测试数据集同步功能。
    """
    await sync_datasets(
        session=session,
        dataset_id="123",
        region="USA",
        universe="TOP1000",
        delay=1,
    )
