"""
同步因子数据的模块。
该模块提供了从 AlphaPower API 同步因子数据到数据库的功能。
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select  # 添加导入

from alphapower.client import (
    AlphaView,
    ClassificationView,
    CompetitionView,
    RegularView,
    SelfAlphaListQueryParams,
    WorldQuantClient,
    wq_client,
)
from alphapower.entity import (
    Alpha,
    Classification,
    Competition,
    Regular,
    Setting,
)
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import setup_logging  # 引入公共方法

from .utils import create_sample, get_or_create_entity

# 配置日志
console_logger = setup_logging(__name__, enable_console=True)
file_logger = setup_logging(__name__, enable_console=False)

# 定义全局锁
alpha_lock = asyncio.Lock()


def create_alphas_settings(alpha_data: AlphaView) -> Setting:
    """
    创建 AlphaSettings 实例。
    """
    return Setting(
        instrument_type=alpha_data.settings.instrument_type,
        region=alpha_data.settings.region,
        universe=alpha_data.settings.universe,
        delay=alpha_data.settings.delay,
        decay=alpha_data.settings.decay,
        neutralization=alpha_data.settings.neutralization,
        truncation=alpha_data.settings.truncation,
        pasteurization=alpha_data.settings.pasteurization,
        unit_handling=alpha_data.settings.unit_handling,
        nan_handling=alpha_data.settings.nan_handling,
        language=alpha_data.settings.language,
        visualization=alpha_data.settings.visualization,
        test_period=getattr(alpha_data.settings, "test_period", None),
    )


def create_alphas_regular(regular: RegularView) -> Regular:
    """
    创建 AlphaRegular 实例。

    参数:
    regular: AlphaView.Regular 对象，包含因子规则的详细信息。

    返回:
    AlphaRegular 实例。
    """
    return Regular(
        code=regular.code,
        description=getattr(regular, "description", None),
        operator_count=regular.operator_count,
    )


async def create_alpha_classifications(
    session: AsyncSession, classifications_data: Optional[List[ClassificationView]]
) -> List[Classification]:
    """
    创建或获取 Classification 实例列表。

    参数:
    session: 数据库会话。
    classifications_data: 分类数据列表。

    返回:
    Classification 实例列表。
    """
    if classifications_data is None:
        return []

    entity_objs = [
        await get_or_create_entity(session, Classification, "classification_id", data)
        for data in classifications_data
    ]

    return entity_objs


async def create_alpha_competitions(
    session: AsyncSession, competitions_data: Optional[List[CompetitionView]]
) -> List[Competition]:
    """
    创建或获取 AlphaCompetition 实例列表。

    参数:
    session: 数据库会话。
    competitions_data: 比赛数据列表。

    返回:
    AlphaCompetition 实例列表。
    """
    if competitions_data is None:
        return []

    entity_objs: List[Competition] = [
        await get_or_create_entity(session, Competition, "competition_id", data)
        for data in competitions_data
    ]

    return entity_objs


def create_alphas(
    alpha_data: AlphaView,
    settings: Setting,
    regular: Regular,
    classifications: List[Classification],
    competitions: List[Competition],
) -> Alpha:
    """
    创建 Alpha 实例。

    参数:
    alpha_data: AlphaView 对象，包含因子详细信息。
    settings: AlphaSettings 实例。
    regular: AlphaRegular 实例。
    classifications: Classification 实例列表。
    competitions: AlphaCompetition 实例列表。

    返回:
    Alpha 实例。
    """
    return Alpha(
        alpha_id=alpha_data.id,
        type=alpha_data.type,
        author=alpha_data.author,
        settings=settings,
        regular=regular,
        date_created=alpha_data.date_created,
        date_submitted=getattr(alpha_data, "date_submitted", None),
        date_modified=alpha_data.date_modified,
        name=getattr(alpha_data, "name", None),
        favorite=alpha_data.favorite,
        hidden=alpha_data.hidden,
        color=getattr(alpha_data, "color", None),
        category=getattr(alpha_data, "category", None),
        tags=",".join(alpha_data.tags) if alpha_data.tags else None,
        classifications=classifications,
        grade=alpha_data.grade,
        stage=alpha_data.stage,
        status=alpha_data.status,
        in_sample=create_sample(alpha_data.in_sample),
        out_sample=create_sample(alpha_data.out_sample),
        train=create_sample(alpha_data.train),
        test=create_sample(alpha_data.test),
        prod=create_sample(alpha_data.prod),
        competitions=competitions,
        themes=",".join(alpha_data.themes) if alpha_data.themes else None,
        # TODO: pyramids 字段需要重新设计
        # pyramids=",".join(alpha_data.pyramids) if alpha_data.pyramids else None,
        pyramids=None,
        team=",".join(alpha_data.team) if alpha_data.team else None,
    )


async def process_alphas_page(
    session: AsyncSession, alphas_results: List[AlphaView]
) -> tuple[int, int]:
    """
    异步处理单页 alphas 数据。
    """
    inserted_alphas = 0
    updated_alphas = 0

    for alpha_data in alphas_results:
        alpha_id = alpha_data.id

        result = await session.execute(select(Alpha).filter_by(alpha_id=alpha_id))
        existing_alpha = result.scalar_one_or_none()

        settings = create_alphas_settings(alpha_data)
        regular = create_alphas_regular(alpha_data.regular)
        classifications = await create_alpha_classifications(
            session, alpha_data.classifications
        )
        competitions = await create_alpha_competitions(session, alpha_data.competitions)
        alpha = create_alphas(
            alpha_data, settings, regular, classifications, competitions
        )

        # 使用锁保护数据库写操作
        async with alpha_lock:
            if existing_alpha:
                alpha.id = existing_alpha.id
                await session.merge(alpha)
                updated_alphas += 1
            else:
                session.add(alpha)
                inserted_alphas += 1

    # 提交事务
    async with alpha_lock:
        await session.commit()

    return inserted_alphas, updated_alphas


async def process_alphas_for_date(
    client: WorldQuantClient, session: AsyncSession, cur_time: datetime, parallel: int
) -> tuple[int, int, int]:
    """
    同步处理指定日期的 alphas 数据，支持分片并行处理。
    """
    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0

    # 初始化时间范围
    start_time = cur_time
    end_time = cur_time + timedelta(days=1)

    while start_time < cur_time + timedelta(days=1):
        query_params = SelfAlphaListQueryParams(
            limit=1,
            date_created_gt=start_time.isoformat(),
            date_created_lt=end_time.isoformat(),
        )
        alphas_data_result, _ = await client.get_self_alphas(query=query_params)

        if alphas_data_result.count < 10000:
            file_logger.info(
                "%s 至 %s 总计 %d 条 alphas 数据。",
                start_time,
                end_time,
                alphas_data_result.count,
            )

            # 分片处理
            tasks = []
            page_size = 100
            total_pages = (alphas_data_result.count + page_size - 1) // page_size
            pages_per_task = (total_pages + parallel - 1) // parallel

            for i in range(parallel):
                start_page = i * pages_per_task + 1
                end_page = min((i + 1) * pages_per_task, total_pages)
                if start_page > end_page:
                    break

                tasks.append(
                    process_alphas_pages(
                        client,
                        session,
                        start_time,
                        end_time,
                        start_page,
                        end_page,
                        page_size,
                    )
                )

            results = await asyncio.gather(*tasks)
            for fetched, inserted, updated in results:
                fetched_alphas += fetched
                inserted_alphas += inserted
                updated_alphas += updated

            # 更新时间范围，继续处理后续时间段
            start_time = end_time
            end_time = cur_time + timedelta(days=1)
        else:
            # 缩小时间范围
            mid_time = start_time + (end_time - start_time) / 2
            end_time = mid_time
            file_logger.info(
                "数据量超过限制，缩小日期范围为 %s 至 %s。",
                start_time,
                end_time,
            )

    return fetched_alphas, inserted_alphas, updated_alphas


async def process_alphas_pages(
    client: WorldQuantClient,
    session: AsyncSession,
    start_time: datetime,
    end_time: datetime,
    start_page: int,
    end_page: int,
    page_size: int,
) -> tuple[int, int, int]:
    """
    处理指定页范围内的 alphas 数据。
    """
    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0

    for page in range(start_page, end_page + 1):
        query_params = SelfAlphaListQueryParams(
            limit=page_size,
            offset=(page - 1) * page_size,
            date_created_gt=start_time.isoformat(),
            date_created_lt=end_time.isoformat(),
            order="dateCreated",
        )

        alphas_data_result, _ = await client.get_self_alphas(query=query_params)

        if not alphas_data_result.results:
            break

        fetched_alphas += len(alphas_data_result.results)
        file_logger.info(
            "为 %s 至 %s 第 %d 页获取了 %d 个 alphas。",
            start_time,
            end_time,
            page,
            len(alphas_data_result.results),
        )

        inserted, updated = await process_alphas_page(
            session, alphas_data_result.results
        )
        inserted_alphas += inserted
        updated_alphas += updated

    return fetched_alphas, inserted_alphas, updated_alphas


async def sync_alphas(start_time: datetime, end_time: datetime, parallel: int) -> None:
    """
    异步同步因子。

    参数:
    session: 数据库会话。
    start_time: 开始时间。
    end_time: 结束时间。
    parallel: 并行任务数。
    """
    if start_time >= end_time:
        raise ValueError("start_time 必须早于 end_time。")

    file_logger.info("开始同步因子...")

    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0

    async with wq_client:
        async with get_db_session(Database.ALPHAS) as session:
            try:
                for cur_time in (
                    start_time + timedelta(days=i)
                    for i in range((end_time - start_time).days + 1)
                ):
                    fetched, inserted, updated = await process_alphas_for_date(
                        wq_client, session, cur_time, parallel
                    )
                    fetched_alphas += fetched
                    inserted_alphas += inserted
                    updated_alphas += updated

                file_logger.info(
                    "因子同步完成。获取: %d, 插入: %d, 更新: %d。",
                    fetched_alphas,
                    inserted_alphas,
                    updated_alphas,
                )
            except Exception as e:
                file_logger.error("同步因子时出错: %s", e)
                await session.rollback()
