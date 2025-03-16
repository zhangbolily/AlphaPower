import logging
import json
import time
from model import (
    get_db,
    Alphas,
    Alphas_Settings,
    Alphas_Regular,
    Alphas_Sample,
    Alphas_Classification,
    Alphas_Competition,
)
from worldquant import (
    WorldQuantClient,
    SelfAlphaListQueryParams,
    Alpha,
)
from typing import Generator
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta, timezone

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_credentials(index: int = 0) -> dict:
    with open("credentials.json") as f:
        credentials = json.load(f)
    return credentials[f"{index}"]


def create_client(credential: dict) -> WorldQuantClient:
    return WorldQuantClient(
        username=credential["username"], password=credential["password"]
    )


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


def create_alphas_sample(sample_data) -> Alphas_Sample:
    if sample_data is None:
        return None

    return Alphas_Sample(
        pnl=sample_data.pnl,
        book_size=sample_data.bookSize,
        long_count=sample_data.longCount,
        short_count=sample_data.shortCount,
        turnover=sample_data.turnover,
        returns=sample_data.returns,
        drawdown=sample_data.drawdown,
        margin=sample_data.margin,
        sharpe=sample_data.sharpe,
        fitness=sample_data.fitness,
        start_date=sample_data.startDate,
    )


def get_or_create_classification(session, classification_data) -> Alphas_Classification:
    classification = (
        session.query(Alphas_Classification)
        .filter_by(classification_id=classification_data.id)
        .first()
    )
    if classification is None:
        classification = Alphas_Classification(
            classification_id=classification_data.id, name=classification_data.name
        )
        try:
            session.add(classification)
            session.commit()
        except IntegrityError:
            session.rollback()
            classification = (
                session.query(Alphas_Classification)
                .filter_by(classification_id=classification_data.id)
                .first()
            )
    return classification


def create_alpha_classifications(
    session, classifications_data
) -> list[Alphas_Classification]:
    if classifications_data is None:
        return []

    classifications = []
    for classification_data in classifications_data:
        classification = get_or_create_classification(session, classification_data)
        classifications.append(classification)
    return classifications


def get_or_create_competition(session, competition_data) -> Alphas_Competition:
    competition = (
        session.query(Alphas_Competition)
        .filter_by(competition_id=competition_data.id)
        .first()
    )
    if competition is None:
        competition = Alphas_Competition(
            competition_id=competition_data.id, name=competition_data.name
        )
        try:
            session.add(competition)
            session.commit()
        except IntegrityError:
            session.rollback()
            competition = (
                session.query(Alphas_Competition)
                .filter_by(competition_id=competition_data.id)
                .first()
            )
    return competition


def create_alpha_competitions(session, competitions_data) -> list[Alphas_Competition]:
    if competitions_data is None:
        return []

    competitions = []
    for competition_data in competitions_data:
        competition = get_or_create_competition(session, competition_data)
        competitions.append(competition)
    return competitions


def create_alphas(
    alpha_data: Alpha,
    settings: Alphas_Settings,
    regular: Alphas_Regular,
    classifications: list[Alphas_Competition],
    competitions: list[Alphas_Competition],
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
        in_sample=(
            create_alphas_sample(alpha_data.inSample) if alpha_data.inSample else None
        ),
        out_sample=(
            create_alphas_sample(alpha_data.outSample) if alpha_data.outSample else None
        ),
        train=(create_alphas_sample(alpha_data.train) if alpha_data.train else None),
        test=(create_alphas_sample(alpha_data.test) if alpha_data.test else None),
        prod=(create_alphas_sample(alpha_data.prod) if alpha_data.prod else None),
        competitions=competitions,
        themes=",".join(alpha_data.themes) if alpha_data.themes else None,
        pyramids=",".join(alpha_data.pyramids) if alpha_data.pyramids else None,
        team=",".join(alpha_data.team) if alpha_data.team else None,
    )


def validate_datetime_range(start_time: datetime, end_time: datetime) -> None:
    if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
        raise ValueError("start_time 和 end_time 必须是 datetime 类型。")
    if start_time >= end_time:
        raise ValueError("start_time 必须早于 end_time。")


def process_alphas_page(db, alphas_results):
    inserted_alphas = 0
    updated_alphas = 0
    count = 0

    for alpha_data in alphas_results:
        alpha_id = alpha_data.id
        existing_alpha = db.query(Alphas).filter_by(alpha_id=alpha_id).first()

        settings = create_alphas_settings(alpha_data)
        regular = create_alphas_regular(alpha_data.regular)
        classifications = create_alpha_classifications(db, alpha_data.classifications)
        competitions = create_alpha_competitions(db, alpha_data.competitions)
        alpha = create_alphas(
            alpha_data, settings, regular, classifications, competitions
        )

        if existing_alpha:
            alpha.id = existing_alpha.id
            db.merge(alpha)
            logger.info(f"Alpha {alpha_id} 已合并。")
            updated_alphas += 1
        else:
            db.add(alpha)
            inserted_alphas += 1

        count += 1

        if count % 100 == 0:
            db.commit()
            logger.info(f"已提交 {count} 个alphas到数据库。")

    db.commit()
    return inserted_alphas, updated_alphas


def process_alphas_for_date(client: WorldQuantClient, db, cur_time: datetime):
    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0
    page = 1
    page_size = 100

    while True:
        query_params = SelfAlphaListQueryParams(
            limit=page_size,
            offset=(page - 1) * page_size,
            date_created_gt=cur_time.astimezone(
                tz=timezone(timedelta(hours=-5))
            ).isoformat(),
            date_created_lt=(cur_time + timedelta(days=1))
            .astimezone(tz=timezone(timedelta(hours=-5)))
            .isoformat(),
            order="dateCreated",
        )

        alphas_data, rate_limit = client.get_self_alphas(query=query_params)

        while rate_limit.remaining < 1:
            logger.info(f"达到速率限制。等待重置 {rate_limit.reset}。")
            time.sleep(rate_limit.reset)

        if len(alphas_data.results) == 0:
            logger.info(f"{cur_time} 没有找到更多的alphas。")
            break

        fetched_alphas += len(alphas_data.results)
        logger.info(
            f"为 {cur_time} 从第 {page} 页获取了 {len(alphas_data.results)} 个alphas。"
        )

        inserted, updated = process_alphas_page(db, alphas_data.results)
        inserted_alphas += inserted
        updated_alphas += updated

        logger.info(f"第 {page} 页处理并提交完成。")
        page += 1

    return fetched_alphas, inserted_alphas, updated_alphas


def sync_alphas(start_time: datetime, end_time: datetime) -> None:
    validate_datetime_range(start_time, end_time)
    logger.info("开始获取和存储alphas。")
    credential: dict = get_credentials(1)
    client: WorldQuantClient = create_client(credential)

    db_generator = get_db("alphas")
    db = next(db_generator)

    fetched_alphas = 0
    inserted_alphas = 0
    updated_alphas = 0

    try:
        for cur_time in [
            start_time + timedelta(days=i)
            for i in range((end_time - start_time).days + 1)
        ]:
            fetched, inserted, updated = process_alphas_for_date(client, db, cur_time)
            fetched_alphas += fetched
            inserted_alphas += inserted
            updated_alphas += updated

        logger.info(
            f"获取和存储alphas完成。获取: {fetched_alphas}, 插入: {inserted_alphas}, 更新: {updated_alphas}。"
        )
    except Exception as e:
        logger.error(f"错误: {e}")
        db.rollback()
    finally:
        db.close()
        logger.info("数据库连接已关闭。")


def get_alphas_from_db(limit: int = 10, offset: int = 0) -> list[Alphas]:
    db_generator = get_db()
    db = next(db_generator)
    alphas = db.query(Alphas).limit(limit).offset(offset).all()
    return alphas


if __name__ == "__main__":
    sync_alphas()
    # alphas = get_alphas_from_db()
    # print(alphas)
