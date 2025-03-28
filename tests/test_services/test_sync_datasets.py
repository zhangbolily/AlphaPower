from alphapower.internal.wraps import with_session
from alphapower.services.sync_datasets import sync_datasets
from sqlalchemy.ext.asyncio import AsyncSession


@with_session("data_test")
def test_sync_datasets(session: AsyncSession):
    sync_datasets(
        session=session,
        dataset_id="123",
        region="USA",
        universe="TOP1000",
        delay=1,
    )
