"""
从 AlphaPower API 获取数据集和数据字段的原始数据。
该模块包含用于获取数据集、数据字段和数据类别的函数。
这些函数使用 aiohttp 库进行异步 HTTP 请求，并解析响应数据。
"""

from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from aiohttp import ClientSession

from alphapower.client.models import (
    DataCategoriesParent,
    DataFieldDetail,
    DatasetDataFields,
    DatasetDetail,
    DataSets,
)

from .common import (
    BASE_URL,
    ENDPOINT_DATA_CATEGORIES,
    ENDPOINT_DATA_FIELDS,
    ENDPOINT_DATA_SETS,
)


async def fetch_dataset_data_fields(
    session: ClientSession, params: Dict[str, Any]
) -> DatasetDataFields:
    """
    从 API 获取数据集字段。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        params (Dict[str, Any]): 请求的查询参数。

    返回:
        DatasetDataFields: 解析后的数据集字段。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_FIELDS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DatasetDataFields.from_json(
        await response.text()
    )  # 修改为 await response.text()


async def fetch_data_field_detail(
    session: ClientSession, field_id: str
) -> DataFieldDetail:
    """
    获取特定数据字段的详细信息。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        field_id (str): 数据字段的 ID。

    返回:
        DataFieldDetail: 解析后的数据字段详细信息。
    """
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_FIELDS}/{field_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataFieldDetail.from_json(
        await response.text()
    )  # 修改为 await response.text()


async def fetch_dataset_detail(
    session: ClientSession, dataset_id: str
) -> DatasetDetail:
    """
    获取特定数据集的详细信息。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        dataset_id (str): 数据集的 ID。

    返回:
        DatasetDetail: 解析后的数据集详细信息。
    """
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_SETS}/{dataset_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDetail.from_json(
        await response.text()
    )  # 修改为 await response.text()


async def fetch_datasets(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> DataSets:
    """
    从 API 获取数据集列表。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        params (Optional[Dict[str, Any]]): 请求的查询参数。

    返回:
        DataSets: 解析后的数据集列表。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_SETS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DataSets.from_json(await response.text())  # 修改为 await response.text()


async def fetch_data_categories(session: ClientSession) -> List[DataCategoriesParent]:
    """
    从 API 获取数据类别。

    参数:
        session (ClientSession): aiohttp 客户端会话。

    返回:
        List[DataCategoriesParent]: 解析后的数据类别列表。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_CATEGORIES)
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataCategoriesParent.from_json(
        await response.text()
    )  # 修改为 await response.text()
