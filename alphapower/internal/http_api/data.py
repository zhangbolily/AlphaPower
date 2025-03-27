from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from aiohttp import ClientSession
from .common import *
from .model import *


async def fetch_dataset_data_fields(
    session: ClientSession, params: Dict[str, Any]
) -> DatasetDataFields:
    url = urljoin(BASE_URL, ENDPOINT_DATA_FIELDS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DatasetDataFields.from_json(await response.content.read())  # 修改为 await


async def fetch_data_field_detail(
    session: ClientSession, field_id: str
) -> DataFieldDetail:
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_FIELDS}/{field_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataFieldDetail.from_json(await response.content.read())  # 修改为 await


async def fetch_dataset_detail(
    session: ClientSession, dataset_id: str
) -> DatasetDetail:
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_SETS}/{dataset_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDetail.from_json(await response.content.read())  # 修改为 await


async def fetch_datasets(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> DataSets:
    url = urljoin(BASE_URL, ENDPOINT_DATA_SETS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DataSets.from_json(await response.content.read())  # 修改为 await


async def fetch_data_categories(session: ClientSession) -> List[DataCategoriesParent]:
    url = urljoin(BASE_URL, ENDPOINT_DATA_CATEGORIES)
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataCategoriesParent.from_json(await response.content.read())  # 修改为 await
