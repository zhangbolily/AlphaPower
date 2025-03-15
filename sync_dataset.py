import logging
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from worldquant import WorldQuantClient, DataSetsQueryParams
from model import (
    DataBase,
    DataSet,
    Data_Category,
    Data_Subcategory,
    StatsData,
    ResearchPaper,
)
from sqlalchemy.exc import IntegrityError

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库连接配置
DATABASE_URL = "sqlite:///data.db"  # 替换为你的数据库连接字符串
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# 创建所有表
DataBase.metadata.create_all(engine)


def get_credentials(index: int = 0) -> dict:
    with open("credentials.json") as f:
        credentials = json.load(f)
    return credentials[f"{index}"]


def create_client(credential: dict) -> WorldQuantClient:
    return WorldQuantClient(
        username=credential["username"], password=credential["password"]
    )


def get_or_create_category(session, category_data) -> Data_Category:
    category = (
        session.query(Data_Category).filter_by(category_id=category_data.id).first()
    )
    if category is None:
        category = Data_Category(category_id=category_data.id, name=category_data.name)
        try:
            session.add(category)
            session.commit()
        except IntegrityError:
            session.rollback()
            category = (
                session.query(Data_Category)
                .filter_by(category_id=category_data.id)
                .first()
            )
    return category


def get_or_create_subcategory(session, subcategory_data) -> Data_Subcategory:
    subcategory = (
        session.query(Data_Subcategory)
        .filter_by(subcategory_id=subcategory_data.id)
        .first()
    )
    if subcategory is None:
        subcategory = Data_Subcategory(
            subcategory_id=subcategory_data.id, name=subcategory_data.name
        )
        try:
            session.add(subcategory)
            session.commit()
        except IntegrityError:
            session.rollback()
            subcategory = (
                session.query(Data_Subcategory)
                .filter_by(subcategory_id=subcategory_data.id)
                .first()
            )
    return subcategory


def sync_datasets():
    credentials = get_credentials()
    client = create_client(credentials)

    try:
        query_params = DataSetsQueryParams(
            limit=10,
            offset=0,
        )  # 根据需要设置查询参数
        datasets_response = client.get_datasets(query_params)

        for dataset in datasets_response.results:
            existing_dataset = (
                session.query(DataSet)
                .filter_by(
                    dataset_id=dataset.id,
                    region=dataset.region,
                    universe=dataset.universe,
                    delay=dataset.delay,
                )
                .first()
            )

            category = get_or_create_category(session, dataset.category)
            subcategory = get_or_create_subcategory(session, dataset.subcategory)

            new_dataset = DataSet(
                dataset_id=dataset.id,
                name=dataset.name,
                description=dataset.description,
                region=dataset.region,
                delay=dataset.delay,
                universe=dataset.universe,
                coverage=dataset.coverage,
                value_score=dataset.valueScore,
                user_count=dataset.userCount,
                alpha_count=dataset.alphaCount,
                field_count=dataset.fieldCount,
                themes=dataset.themes,
                category_id=category.id,
                subcategory_id=subcategory.id,
            )

            detail = client.get_dataset_detail(dataset.id)
            stats_data = []
            for data_item in detail.data:
                stats_data.append(
                    StatsData(
                        data_set_id=new_dataset.id,
                        region=data_item.region,
                        delay=data_item.delay,
                        universe=data_item.universe,
                        coverage=data_item.coverage,
                        value_score=data_item.valueScore,
                        user_count=data_item.userCount,
                        alpha_count=data_item.alphaCount,
                        field_count=data_item.fieldCount,
                        themes=data_item.themes,
                    )
                )

            new_dataset.stats_data = stats_data

            research_papers = []
            for paper in dataset.researchPapers:
                research_paper = ResearchPaper(
                    type=paper.type, title=paper.title, url=paper.url
                )
                research_papers.append(research_paper)

            new_dataset.research_papers = research_papers

            if existing_dataset:
                new_dataset.id = existing_dataset.id
                session.merge(new_dataset)
            else:
                session.add(new_dataset)

        session.commit()
        logger.info("Datasets synchronized successfully.")
    except Exception as e:
        logger.error(f"Error synchronizing datasets: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    sync_datasets()
