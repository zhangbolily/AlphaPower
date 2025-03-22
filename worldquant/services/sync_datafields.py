import asyncio
import time
from asyncio import Lock
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tqdm import tqdm  # 引入进度条库

from worldquant.client import WorldQuantClient
from worldquant.config.settings import get_credentials
from worldquant.entity import (
    Data_Category as Data_CategoryEntity,
    Data_Subcategory as Data_SubcategoryEntity,
    DataField as DataFieldEntity,
    DataSet as DataSetEntity,
)

from worldquant.internal.http_api.model import DataField, GetDataFieldsQueryParams
from worldquant.utils.credentials import create_client
from worldquant.utils.db import with_session  # 修复导入
from worldquant.utils.logging import setup_logging
from worldquant.utils.services import get_or_create_entity

# 配置日志
file_logger = setup_logging(f"{__name__}_file", enable_console=False)  # 文件日志
console_logger = setup_logging(f"{__name__}_console", enable_console=True)  # 控制台日志


async def create_datafield(
    session: Session, datafield_data: DataField, dataset_id: int, task_id: int
) -> None:
    """
    异步创建或更新数据字段。
    """
    datafield = (
        session.query(DataFieldEntity)
        .filter_by(
            field_id=datafield_data.id,
            region=datafield_data.region,
            universe=datafield_data.universe,
            delay=datafield_data.delay,
        )
        .first()
    )

    category = await asyncio.to_thread(
        get_or_create_entity,
        session,
        Data_CategoryEntity,
        "name",
        datafield_data.category,
    )
    subcategory = await asyncio.to_thread(
        get_or_create_entity,
        session,
        Data_SubcategoryEntity,
        "name",
        datafield_data.subcategory,
    )
    new_datafield = DataFieldEntity(
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
        user_count=datafield_data.userCount,
        alpha_count=datafield_data.alphaCount,
        themes=datafield_data.themes,
    )

    try:
        if datafield is None:
            session.add(new_datafield)
            file_logger.info(f"[任务 {task_id}] 添加新数据字段: {datafield_data.id}")
        else:
            new_datafield.id = datafield.id
            session.merge(new_datafield)
            file_logger.info(f"[任务 {task_id}] 更新现有数据字段: {datafield_data.id}")
        session.commit()
    except IntegrityError:
        session.rollback()
        file_logger.error(
            f"[任务 {task_id}] 数据字段 {datafield_data.id} 操作失败，已回滚。"
        )


async def fetch_datafields(
    client: WorldQuantClient, query_params: GetDataFieldsQueryParams, task_id: int
):
    """
    异步获取数据字段。
    """
    file_logger.debug(f"[任务 {task_id}] 开始获取数据字段，参数: {query_params}")
    try:
        return await client.get_data_fields_in_dataset(query=query_params)
    except Exception as e:
        file_logger.warning(f"[任务 {task_id}] 获取数据字段时出错: {e}，正在重试...")
        try:
            return await client.get_data_fields_in_dataset(query=query_params)
        except Exception as retry_e:
            file_logger.error(f"[任务 {task_id}] 重试获取数据字段时再次出错: {retry_e}")
            return None


async def process_datafields_concurrently(
    session: Session,
    client: WorldQuantClient,
    dataset: DataSetEntity,
    instrument_type: Optional[str],
    parallel: int,
    progress_bar,
    lock: Lock,
):
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
    semaphore = asyncio.Semaphore(parallel)
    offset = 0
    limit = 50
    field_count = dataset.field_count

    async def process_with_semaphore(offset, task_id):
        async with semaphore:
            query_params = GetDataFieldsQueryParams(
                dataset_id=dataset.dataset_id,
                region=dataset.region,
                universe=dataset.universe,
                delay=dataset.delay,
                instrumentType=instrument_type,
                offset=offset,
                limit=limit,
            )
            datafields_response = await fetch_datafields(client, query_params, task_id)
            if datafields_response and datafields_response.results:
                for datafield in datafields_response.results:  # DataField 类型
                    await create_datafield(session, datafield, dataset.id, task_id)
                async with lock:
                    progress_bar.update(len(datafields_response.results))
                return len(datafields_response.results)
            else:
                file_logger.info(
                    f"[任务 {task_id}] 数据集 {dataset.dataset_id} 没有更多的数据字段。"
                )
                return 0

    tasks = []
    task_id = 0
    while offset < field_count:
        task_id += 1
        tasks.append(process_with_semaphore(offset, task_id))
        offset += limit

    return await asyncio.gather(*tasks)


@with_session("data")
async def sync_datafields(
    session: Session,
    instrument_type: Optional[str] = None,
    dataset_id: Optional[str] = None,  # 新增参数 dataset_id
    parallel: int = 5,
):
    """
    同步数据字段。

    参数:
    session: 数据库会话。
    instrument_type: 资产类型过滤条件。
    dataset_id: 指定的数据集 ID，仅同步该数据集的数据字段。
    parallel: 并行度，控制同时运行的任务数量。
    """
    credentials = get_credentials(1)
    client = create_client(credentials)  # 修改为协程调用

    async with client:
        try:
            if dataset_id:
                # 根据 dataset_id 查询特定数据集
                datasets = (
                    session.query(DataSetEntity).filter_by(dataset_id=dataset_id).all()
                )
                if not datasets:
                    console_logger.error(f"未找到指定的数据集: {dataset_id}")
                    return
                console_logger.info(f"找到指定数据集 {dataset_id}，开始同步数据字段。")
            else:
                # 查询所有数据集
                datasets = session.query(DataSetEntity).all()
                console_logger.info(f"找到 {len(datasets)} 个数据集。")

            # 初始化进度条
            total_fields = sum(dataset.field_count for dataset in datasets)
            progress_bar = tqdm(
                total=total_fields, desc="同步数据字段", unit="个", dynamic_ncols=True
            )

            lock = Lock()  # 创建异步互斥锁
            sync_start_time = time.time()  # 开始计时

            for dataset in datasets:
                file_logger.info(
                    f"正在处理数据集 {dataset.dataset_id} {dataset.universe} {dataset.region} {dataset.delay}..."
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

            session.commit()  # 提交数据库事务
            sync_elapsed_time = time.time() - sync_start_time  # 计算同步任务耗时
            progress_bar.close()  # 关闭进度条

            console_logger.info("数据字段同步成功。")
            console_logger.info(f"同步任务总耗时: {sync_elapsed_time:.2f} 秒")
        except Exception as e:
            console_logger.error(f"同步数据字段时出错: {e}")
            session.rollback()  # 回滚事务
            if progress_bar:
                progress_bar.close()  # 确保异常时关闭进度条
