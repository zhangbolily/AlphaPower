"""
数据集同步模块
"""

import asyncio
import time  # 引入时间模块
from asyncio import Lock
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from tqdm import tqdm  # 引入进度条库

from alphapower.client import (
    DatasetDetailView,
    DatasetListView,
    DataSetsQueryParams,
    DatasetView,
    WorldQuantClient,
    create_client,
)
from alphapower.config.settings import get_credentials
from alphapower.entity import DataCategory, Dataset, ResearchPaper, StatsData
from alphapower.internal.utils import setup_logging
from alphapower.internal.wraps import log_time_elapsed, with_session  # 引入公共方法

from .utils import get_or_create_entity  # 引入公共方法

# 配置日志，禁用控制台日志输出
logger = setup_logging(f"{__name__}_file", enable_console=False)
console_logger = setup_logging(f"{__name__}_console", enable_console=True)


@log_time_elapsed
async def fetch_dataset_detail(
    client: WorldQuantClient, dataset_id: str, task_id: int
) -> Optional[DatasetDetailView]:
    """
    异步获取单个数据集的详细信息，增加异常处理和重试逻辑。
    """
    logger.debug("[任务 %d] 开始获取数据集详情: %s", task_id, dataset_id)
    for attempt in range(2):  # 尝试两次
        try:
            return await client.get_dataset_detail(dataset_id)  # 假设为异步函数
        except Exception as e:
            logger.warning(
                "[任务 %d] 获取数据集详情时出错: %s，尝试第 %d 次重试...",
                task_id,
                e,
                attempt + 1,
            )
    logger.error("[任务 %d] 获取数据集详情失败: %s", task_id, dataset_id)
    return None


async def fetch_dataset_details_concurrently(
    client: WorldQuantClient, dataset_ids: List[str], parallel: int
) -> List[Optional[DatasetDetailView]]:
    """
    并发获取多个数据集的详细信息，增加异常处理。
    """
    semaphore = asyncio.Semaphore(parallel)

    async def fetch_with_semaphore(
        dataset_id: str, task_id: int
    ) -> Optional[DatasetDetailView]:
        """
        使用信号量限制并发请求的数量。
        """
        async with semaphore:
            return await fetch_dataset_detail(client, dataset_id, task_id)

    tasks = [
        fetch_with_semaphore(dataset_id, task_id)
        for task_id, dataset_id in enumerate(dataset_ids, start=1)
    ]
    return await asyncio.gather(*tasks)


async def process_dataset(
    session: AsyncSession, dataset: DatasetView, detail: DatasetDetailView
) -> None:
    """
    处理单个数据集的同步逻辑。

    参数:
    session: 数据库会话，用于操作数据库。
    dataset: 数据集对象，包含基本信息。
    detail: 数据集详情对象，包含详细信息。
    """
    logger.debug(
        "开始处理数据集: id=%s, name=%s, region=%s, universe=%s, delay=%s",
        dataset.id,
        dataset.name,
        dataset.region,
        dataset.universe,
        dataset.delay,
    )

    # 查询数据库中是否已存在该数据集（改为异步查询）
    query_result = await session.execute(
        select(Dataset).filter_by(
            dataset_id=dataset.id,
            region=dataset.region,
            universe=dataset.universe,
            delay=dataset.delay,
        )
    )

    existing_dataset: Optional[Dataset] = query_result.scalar_one_or_none()

    # 获取或创建数据集的分类和子分类
    category: DataCategory = await get_or_create_entity(
        session, DataCategory, "category_id", dataset.category
    )
    subcategory: DataCategory = await get_or_create_entity(
        session, DataCategory, "subcategory_id", dataset.subcategory
    )

    # 创建新的数据集对象
    new_dataset = Dataset(
        dataset_id=dataset.id,
        name=dataset.name,
        description=dataset.description,
        region=dataset.region,
        delay=dataset.delay,
        universe=dataset.universe,
        coverage=dataset.coverage,
        value_score=dataset.value_score,
        user_count=dataset.user_count,
        alpha_count=dataset.alpha_count,
        field_count=dataset.field_count,
        themes=dataset.themes,
        category_id=category.id,
        subcategory_id=subcategory.id,
        pyramid_multiplier=dataset.pyramid_multiplier,
    )

    # 使用传入的 detail 数据填充统计信息
    new_dataset.stats_data = [
        StatsData(
            data_set_id=new_dataset.id,
            region=data_item.region,
            delay=data_item.delay,
            universe=data_item.universe,
            coverage=data_item.coverage,
            value_score=data_item.value_score,
            user_count=data_item.user_count,
            alpha_count=data_item.alpha_count,
            field_count=data_item.field_count,
            themes=data_item.themes,
        )
        for data_item in detail.data
    ]

    # 添加研究论文信息
    new_dataset.research_papers = [
        ResearchPaper(type=paper.type, title=paper.title, url=paper.url)
        for paper in dataset.research_papers
    ]

    # 如果数据集已存在，则更新；否则添加新数据集
    try:
        if existing_dataset:
            new_dataset.id = existing_dataset.id
            await session.merge(new_dataset)  # 合并更新
            logger.info(
                "更新现有数据集: id=%s, name=%s, region=%s, universe=%s, delay=%s",
                dataset.id,
                dataset.name,
                dataset.region,
                dataset.universe,
                dataset.delay,
            )
        else:
            session.add(new_dataset)  # 添加新数据集
            logger.info(
                "添加新数据集: id=%s, name=%s, region=%s, universe=%s, delay=%s",
                dataset.id,
                dataset.name,
                dataset.region,
                dataset.universe,
                dataset.delay,
            )
    except Exception as e:
        logger.error(
            "处理数据集时出错: id=%s, name=%s, region=%s, universe=%s, delay=%s - %s",
            dataset.id,
            dataset.name,
            dataset.region,
            dataset.universe,
            dataset.delay,
            e,
        )
        raise  # 重新抛出异常以便上层处理


async def process_datasets_concurrently(
    session: AsyncSession,
    client: WorldQuantClient,
    query_params_list: List[DataSetsQueryParams],
    parallel: int,
    progress_bar: tqdm,
    lock: Lock,
) -> List[int]:
    """
    并发处理数据集，并及时更新进度条。

    参数:
    session: 数据库会话。
    client: WorldQuant 客户端。
    query_params_list: 查询参数列表。
    parallel: 并行度。
    progress_bar: 进度条对象。
    lock: 异步互斥锁。
    """
    semaphore = asyncio.Semaphore(parallel)

    async def process_with_semaphore(params: DataSetsQueryParams, task_id: int) -> int:
        async with semaphore:
            logger.debug("[任务 %d] 开始处理查询参数: %s", task_id, params)
            datasets_response: DatasetListView = await client.get_datasets(params)
            if not datasets_response or not datasets_response.results:
                logger.info("[任务 %d] 没有更多数据集。", task_id)
                return 0

            dataset_ids = [dataset.id for dataset in datasets_response.results]
            details: List[Optional[DatasetDetailView]] = (
                await fetch_dataset_details_concurrently(client, dataset_ids, parallel)
            )

            for dataset, detail in zip(datasets_response.results, details):
                if detail:
                    logger.debug(
                        "[任务 %d] 正在处理数据集: id=%s, name=%s, region=%s, universe=%s, delay=%s",
                        task_id,
                        dataset.id,
                        dataset.name,
                        dataset.region,
                        dataset.universe,
                        dataset.delay,
                    )
                    await process_dataset(session, dataset, detail)

            async with lock:
                progress_bar.update(len(datasets_response.results))
            return len(datasets_response.results)

    tasks = [
        process_with_semaphore(params, task_id)
        for task_id, params in enumerate(query_params_list, start=1)
    ]
    return await asyncio.gather(*tasks)


@with_session("data")
async def sync_datasets(
    session: AsyncSession,
    region: Optional[str] = None,
    universe: Optional[str] = None,
    delay: Optional[int] = None,
    parallel: int = 5,
) -> None:
    """
    同步数据集，增加异常处理和进度条更新。

    参数:
    session: 数据库会话。
    region: 区域过滤条件。
    universe: 股票池过滤条件。
    delay: 延迟过滤条件。
    parallel: 并行度，控制同时运行的任务数量。
    """
    credentials = get_credentials(1)  # 获取认证信息
    client = create_client(
        credentials, pool_connections=parallel, pool_maxsize=parallel * parallel
    )  # 创建客户端实例

    async with client:
        try:
            offset: int = 0
            limit: int = 50  # 每次查询的最大数据量限制
            total_count: int = 0
            sync_start_time: float = 0.0
            query_params_list: List[DataSetsQueryParams] = []
            lock: Lock = Lock()  # 创建异步互斥锁

            console_logger.info("=== 数据集同步任务开始 ===")
            console_logger.info(
                "过滤参数 - 地区: %s, 股票池: %s, delay: %s", region, universe, delay
            )
            console_logger.info("正在获取数据集总数...")

            # 获取数据集总数
            count_query_params = DataSetsQueryParams(
                limit=1, offset=0, region=region, universe=universe, delay=delay
            )
            datasets_response = await client.get_datasets(count_query_params)
            total_count = datasets_response.count  # 数据集总数
            console_logger.info("总计 %d 个数据集需要同步。", total_count)

            # 初始化进度条
            progress_bar = tqdm(
                total=total_count, desc="同步数据集", unit="个", dynamic_ncols=True
            )  # dynamic_ncols=True 确保进度条在同一行刷新

            # 构建查询参数列表
            while offset < total_count:
                query_params = DataSetsQueryParams(
                    limit=limit,
                    offset=offset,
                    region=region,
                    universe=universe,
                    delay=delay,
                )
                query_params_list.append(query_params)
                offset += limit

            # 并发处理数据集
            sync_start_time = time.time()  # 开始计时
            await process_datasets_concurrently(
                session, client, query_params_list, parallel, progress_bar, lock
            )
            await session.commit()  # 提交数据库事务
        except Exception as e:
            logger.error("同步数据集时出错: %s", e)
            await session.rollback()  # 回滚事务
            raise  # 重新抛出异常
        finally:
            progress_bar.close()  # 确保进度条关闭
            sync_elapsed_time = time.time() - sync_start_time  # 计算同步任务耗时
            console_logger.info("同步任务总耗时: %.2f 秒", sync_elapsed_time)
            console_logger.info("成功同步 %d 个数据集。", total_count)
            logger.info("资源已释放。")
