import time
from datetime import datetime, timedelta

from worldquant import Alpha, SelfAlphaListQueryParams, WorldQuantClient
from worldquant.config.settings import get_credentials
from worldquant.entity import (
    Alphas,
    Alphas_Classification,
    Alphas_Competition,
    Alphas_Regular,
    Alphas_Sample,
    Alphas_Settings,
)
from worldquant.utils.credentials import create_client
from worldquant.utils.db import with_session
from worldquant.utils.logging import setup_logging
from worldquant.utils.services import (
    create_sample,
    get_or_create_entity,
)  # 引入公共方法

# 配置日志
logger = setup_logging(__name__)


def create_alphas_settings(alpha_data: Alpha) -> Alphas_Settings:
    return Alphas_Settings(
        instrument_type=alpha_data.settings.instrumentType,
        region=alpha_data.settings.region,
        universe=alpha_data.settings.universe,
        delay=alpha_data.settings.delay,
        decay=alpha_data.settings.decay,
        neutralization=alpha_data.settings.neutralization,
        truncation=alpha_data.settings.truncation,
        pasteurization=alpha_data.settings.pasteurization,
        unit_handling=alpha_data.settings.unitHandling,
        nan_handling=alpha_data.settings.nanHandling,
        language=alpha_data.settings.language,
        visualization=alpha_data.settings.visualization,
        test_period=getattr(alpha_data.settings, "testPeriod", None),
    )


def create_alphas_regular(regular) -> Alphas_Regular:
    return Alphas_Regular(
        code=regular.code,
        description=getattr(regular, "description", None),
        operator_count=regular.operatorCount,
    )


def create_alpha_classifications(session, classifications_data):
    if classifications_data is None:
        return []
    return [
        get_or_create_entity(session, Alphas_Classification, "classification_id", data)
        for data in classifications_data
    ]


def create_alpha_competitions(session, competitions_data):
    if competitions_data is None:
        return []
    return [
        get_or_create_entity(session, Alphas_Competition, "competition_id", data)
        for data in competitions_data
    ]


def create_alphas(
    alpha_data: Alpha,
    settings: Alphas_Settings,
    regular: Alphas_Regular,
    classifications,
    competitions,
) -> Alphas:
    return Alphas(
        alpha_id=alpha_data.id,
        type=alpha_data.type,
        author=alpha_data.author,
        settings=settings,
        regular=regular,
        date_created=alpha_data.dateCreated,
        date_submitted=getattr(alpha_data, "dateSubmitted", None),
        date_modified=alpha_data.dateModified,
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
        in_sample=create_sample(alpha_data.inSample, Alphas_Sample),
        out_sample=create_sample(alpha_data.outSample, Alphas_Sample),
        train=create_sample(alpha_data.train, Alphas_Sample),
        test=create_sample(alpha_data.test, Alphas_Sample),
        prod=create_sample(alpha_data.prod, Alphas_Sample),
        competitions=competitions,
        themes=",".join(alpha_data.themes) if alpha_data.themes else None,
        pyramids=",".join(alpha_data.pyramids) if alpha_data.pyramids else None,
        team=",".join(alpha_data.team) if alpha_data.team else None,
    )


def process_alphas_page(session, alphas_results):
    inserted_alphas = 0
    updated_alphas = 0

    for alpha_data in alphas_results:
        alpha_id = alpha_data.id
        existing_alpha = session.query(Alphas).filter_by(alpha_id=alpha_id).first()

        settings = create_alphas_settings(alpha_data)
        regular = create_alphas_regular(alpha_data.regular)
        classifications = create_alpha_classifications(
            session, alpha_data.classifications
        )
        competitions = create_alpha_competitions(session, alpha_data.competitions)
        alpha = create_alphas(
            alpha_data, settings, regular, classifications, competitions
        )

        if existing_alpha:
            alpha.id = existing_alpha.id
            session.merge(alpha)
            logger.info(f"Alpha {alpha_id} 已更新。")
            updated_alphas += 1
        else:
            session.add(alpha)
            logger.info(f"Alpha {alpha_id} 已插入。")
            inserted_alphas += 1

    session.commit()
    return inserted_alphas, updated_alphas


def process_alphas_for_date(client: WorldQuantClient, session, cur_time: datetime):
    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0
    page = 1
    page_size = 100

    while True:
        query_params = SelfAlphaListQueryParams(
            limit=page_size,
            offset=(page - 1) * page_size,
            date_created_gt=cur_time.isoformat(),
            date_created_lt=(cur_time + timedelta(days=1)).isoformat(),
            order="dateCreated",
        )

        alphas_data, rate_limit = client.get_self_alphas(query=query_params)

        while rate_limit.remaining < 1:
            logger.info(f"达到速率限制，等待 {rate_limit.reset} 秒...")
            time.sleep(rate_limit.reset)

        if not alphas_data.results:
            logger.info(f"{cur_time} 没有更多的 alphas。")
            break

        fetched_alphas += len(alphas_data.results)
        logger.info(f"为 {cur_time} 获取了 {len(alphas_data.results)} 个 alphas。")

        inserted, updated = process_alphas_page(session, alphas_data.results)
        inserted_alphas += inserted
        updated_alphas += updated

        logger.info(f"第 {page} 页处理完成。")
        page += 1

    return fetched_alphas, inserted_alphas, updated_alphas


@with_session("alphas")
def sync_alphas(session, start_time: datetime, end_time: datetime):
    """
    同步因子。

    参数:
    session: 数据库会话。
    start_time: 开始时间。
    end_time: 结束时间。
    """
    if start_time >= end_time:
        raise ValueError("start_time 必须早于 end_time。")

    logger.info("开始同步因子...")
    credentials = get_credentials(1)
    client = create_client(credentials)

    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0

    try:
        for cur_time in (
            start_time + timedelta(days=i)
            for i in range((end_time - start_time).days + 1)
        ):
            fetched, inserted, updated = process_alphas_for_date(
                client, session, cur_time
            )
            fetched_alphas += fetched
            inserted_alphas += inserted
            updated_alphas += updated

        logger.info(
            f"因子同步完成。获取: {fetched_alphas}, 插入: {inserted_alphas}, 更新: {updated_alphas}。"
        )
    except Exception as e:
        logger.error(f"同步因子时出错: {e}")
        session.rollback()
