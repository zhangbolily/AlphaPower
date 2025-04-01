"""
@file: sync_datafields.py
"""

import asyncio
import time
from asyncio import Lock
from typing import List, Optional

from aiohttp import ClientError  # Import specific exception for HTTP client errors
from sqlalchemy.exc import (  # Import specific exception for SQL errors
    IntegrityError,
    SQLAlchemyError,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from tqdm import tqdm  # 引入进度条库

from alphapower.client import (
    DataFieldListView,
    DataFieldView,
    GetDataFieldsQueryParams,
    WorldQuantClient,
    create_client,
)
from alphapower.config.settings import get_credentials
from alphapower.entity import Category, DataField, Dataset
from alphapower.internal.utils import setup_logging  # 修复导入
from alphapower.internal.wraps import with_session

from .utils import get_or_create_entity

# 配置日志
file_logger = setup_logging(f"{__name__}_file", enable_console=False)  # 文件日志
console_logger = setup_logging(f"{__name__}_console", enable_console=True)  # 控制台日志


async def create_datafield(
    session: AsyncSession, datafield_data: DataFieldView, dataset_id: int, task_id: int
) -> None:
    """
    异步创建或更新数据字段。
    """

    query = select(DataField).filter(
        DataField.field_id == datafield_data.id,
        DataField.region == datafield_data.region,
        DataField.universe == datafield_data.universe,
        DataField.delay == datafield_data.delay,
    )
    result = await session.execute(query)
    datafield = result.scalars().first()

    category = await get_or_create_entity(
        session,
        Category,
        "name",
        datafield_data.category,
    )

    if not isinstance(category, Category):
        file_logger.error(
            "[任务 %d] 数据字段 %s 的分类 %s 不存在。",
            task_id,
            datafield_data.id,
            datafield_data.category,
        )
        return

    subcategory = await get_or_create_entity(
        session,
        Category,
        "name",
        datafield_data.subcategory,
    )
    if not isinstance(subcategory, Category):
        file_logger.error(
            "[任务 %d] 数据字段 %s 的子分类 %s 不存在。",
            task_id,
            datafield_data.id,
            datafield_data.subcategory,
        )
        return

    new_datafield = DataField(
        dataset_id=dataset_id,
        field_id=datafield_data.id,
        description=datafield_data.description,
        type=datafield_data.type,
        category_id=category.id,
        subcategory_id=subcategory.id,
        universe=datafield_data.universe,
        region=datafield_data.region,
        delay=datafield_data.delay,
        coverage=datafield_data.coverage,
        user_count=datafield_data.user_count,
        alpha_count=datafield_data.alpha_count,
        themes=datafield_data.themes,
    )

    try:
        if datafield is None:
            session.add(new_datafield)
            file_logger.info("[任务 %d] 添加新数据字段: %s", task_id, datafield_data.id)
        else:
            new_datafield.id = datafield.id
            await session.merge(new_datafield)
            file_logger.info(
                "[任务 %d] 更新现有数据字段: %s", task_id, datafield_data.id
            )
        await session.commit()
    except IntegrityError:
        await session.rollback()
        file_logger.error(
            "[任务 %d] 数据字段 %s 操作失败，已回滚。",
            task_id,
            datafield_data.id,
        )


async def fetch_datafields(
    client: WorldQuantClient, query_params: GetDataFieldsQueryParams, task_id: int
) -> Optional[DataFieldListView]:
    """
    异步获取数据字段。
    """
    file_logger.debug("[任务 %d] 开始获取数据字段，参数: %s", task_id, query_params)
    try:
        return await client.get_data_fields_in_dataset(query=query_params)
    except ClientError as e:  # Catch specific client errors
        file_logger.warning("[任务 %d] 获取数据字段时出错: %s，正在重试...", task_id, e)
        try:
            return await client.get_data_fields_in_dataset(query=query_params)
        except ClientError as retry_e:  # Catch specific client errors on retry
            file_logger.error(
                "[任务 %d] 重试获取数据字段时再次出错: %s", task_id, retry_e
            )
            return None


async def process_datafields_concurrently(
    session: AsyncSession,
    client: WorldQuantClient,
    dataset: Dataset,
    instrument_type: Optional[str],
    parallel: int,
    progress_bar: tqdm,
    lock: Lock,
) -> None:
    """
    并发处理数据字段，并及时更新进度条。

    参数:
    session: 数据库会话。
    client: WorldQuant 客户端。
    dataset: 数据集对象。
    instrument_type: 仪器类型过滤条件。
    parallel: 并行度。
    progress_bar: 进度条对象。
    lock: 异步互斥锁。
    """
    semaphore: asyncio.Semaphore = asyncio.Semaphore(parallel)
    offset: int = 0
    limit: int = 50
    field_count: int = dataset.field_count

    async def process_with_semaphore(offset: int, task_id: int) -> int:
        async with semaphore:
            query_params: GetDataFieldsQueryParams = GetDataFieldsQueryParams(
                dataset_id=dataset.dataset_id,
                region=dataset.region,
                universe=dataset.universe,
                delay=dataset.delay,
                instrument_type=instrument_type,
                offset=offset,
                limit=limit,
            )
            datafields = await fetch_datafields(client, query_params, task_id)
            if datafields and datafields.results:
                file_logger.info(
                    "[任务 %d] 数据集 %s 获取到 %d 个数据字段。",
                    task_id,
                    dataset.dataset_id,
                    len(datafields.results),
                )
                for datafield in datafields.results:  # DataField 类型
                    await create_datafield(session, datafield, dataset.id, task_id)
                async with lock:
                    progress_bar.update(len(datafields.results))
                return len(datafields.results)
            else:
                file_logger.info(
                    "[任务 %d] 数据集 %s 没有更多的数据字段。",
                    task_id,
                    dataset.dataset_id,
                )
                return 0

    tasks: list[asyncio.Task] = []
    task_id: int = 0
    while offset < field_count:
        task_id += 1
        tasks.append(asyncio.create_task(process_with_semaphore(offset, task_id)))
        offset += limit

    await asyncio.gather(*tasks)


@with_session("data")
async def sync_datafields(
    session: AsyncSession,
    instrument_type: Optional[str] = None,
    dataset_id: Optional[str] = None,  # 新增参数 dataset_id
    parallel: int = 5,
) -> None:
    """
    同步数据字段。

    参数:
    session: 数据库会话。
    instrument_type: 资产类型过滤条件。
    dataset_id: 指定的数据集 ID，仅同步该数据集的数据字段。
    parallel: 并行度，控制同时运行的任务数量。
    """
    credentials: dict = get_credentials(1)
    client: WorldQuantClient = create_client(credentials)

    async with client:
        try:
            datasets: List[Dataset] = []

            if dataset_id:
                # 根据 dataset_id 查询特定数据集
                query = select(Dataset).filter_by(dataset_id=dataset_id)
                result = await session.execute(query)
                datasets = list(result.scalars().all())
                if not datasets:
                    console_logger.error("未找到指定的数据集: %s", dataset_id)
                    return
                console_logger.info("找到指定数据集 %s，开始同步数据字段。", dataset_id)
            else:
                # 查询所有数据集
                query = select(Dataset)
                result = await session.execute(query)
                datasets = list(result.scalars().all())
                console_logger.info("找到 %d 个数据集。", len(datasets))

            # 初始化进度条
            total_fields: int = sum(dataset.field_count for dataset in datasets)
            progress_bar: tqdm = tqdm(
                total=total_fields, desc="同步数据字段", unit="个", dynamic_ncols=True
            )

            lock: Lock = Lock()  # 创建异步互斥锁
            sync_start_time: float = time.time()  # 开始计时

            for dataset in datasets:
                file_logger.info(
                    "正在处理数据集 %s %s %s %s...",
                    dataset.dataset_id,
                    dataset.universe,
                    dataset.region,
                    dataset.delay,
                )
                await process_datafields_concurrently(
                    session,
                    client,
                    dataset,
                    instrument_type,
                    parallel,
                    progress_bar,
                    lock,
                )

            await session.commit()  # 提交数据库事务
            sync_elapsed_time: float = time.time() - sync_start_time  # 计算同步任务耗时
            progress_bar.close()  # 关闭进度条

            console_logger.info("数据字段同步成功。")
            console_logger.info("同步任务总耗时: %.2f 秒", sync_elapsed_time)
        except SQLAlchemyError as e:  # Catch specific SQL errors
            console_logger.error("同步数据字段时数据库出错: %s", e)
            await session.rollback()  # 回滚事务
        finally:
            if progress_bar:
                progress_bar.close()  # 确保异常时关闭进度条
