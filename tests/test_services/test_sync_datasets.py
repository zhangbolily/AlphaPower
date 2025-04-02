"""
测试数据集同步功能。
"""

from alphapower.services.sync_datasets import sync_datasets


async def test_sync_datasets() -> None:
    """
    测试数据集同步功能。
    """
    await sync_datasets(
        dataset_id="123",
        region="USA",
        universe="TOP1000",
        delay=1,
    )
