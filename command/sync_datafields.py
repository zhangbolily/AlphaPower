import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from worldquant import WorldQuantClient, GetDataFieldsQueryParams
from model import DataSet, DataField
from sqlalchemy.exc import IntegrityError
from model._session import get_db
from .sync_datasets import (
    get_credentials,
    create_client,
    get_or_create_category,
    get_or_create_subcategory,
)

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_datafield(session, datafield_data, dataset_id, task_id) -> DataField:
    datafield = session.query(DataField).filter_by(field_id=datafield_data.id).first()

    category = get_or_create_category(session, datafield_data.category)
    subcategory = get_or_create_subcategory(session, datafield_data.subcategory)
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
        user_count=datafield_data.userCount,
        alpha_count=datafield_data.alphaCount,
        themes=datafield_data.themes,
    )

    try:
        if datafield is None:
            session.add(new_datafield)
            logger.info(f"[任务 {task_id}] 添加新数据字段: {datafield_data.id}")
        else:
            new_datafield.id = datafield.id
            session.merge(new_datafield)
            logger.info(f"[任务 {task_id}] 更新现有数据字段: {datafield_data.id}")
        session.commit()
    except IntegrityError:
        session.rollback()
        logger.error(
            f"[任务 {task_id}] 数据字段 {datafield_data.id} 操作失败，已回滚。"
        )


def fetch_datafields(client, dataset, instrument_type, offset, limit, task_id):
    query_params = GetDataFieldsQueryParams(
        dataset_id=dataset.dataset_id,
        region=dataset.region,
        universe=dataset.universe,
        delay=dataset.delay,
        instrumentType=instrument_type,
        offset=offset,
        limit=limit,
    )
    try:
        return client.get_data_fields_in_dataset(query=query_params)
    except Exception as e:
        logger.error(f"[任务 {task_id}] 获取数据字段时出错: {e}，正在重试...")
        try:
            return client.get_data_fields_in_dataset(query=query_params)
        except Exception as retry_e:
            logger.error(f"[任务 {task_id}] 重试获取数据字段时再次出错: {retry_e}")
            return None


def create_and_process_tasks(
    executor, client, dataset, instrument_type, session, max_workers
):
    futures = []
    task_ids = []
    offset = 0
    limit = 50
    field_count = dataset.field_count
    count = 0
    task_id = 0

    while offset < field_count:
        task_id += 1
        future = executor.submit(
            fetch_datafields,
            client,
            dataset,
            instrument_type,
            offset,
            limit,
            task_id,
        )
        futures.append(future)
        task_ids.append(task_id)
        offset += limit

        if len(futures) >= max_workers:
            for future in as_completed(futures):
                task_id = task_ids[futures.index(future)]
                datafields_response = future.result()
                if datafields_response and datafields_response.results:
                    for datafield_data in datafields_response.results:
                        create_datafield(session, datafield_data, dataset.id, task_id)
                        count += 1
                else:
                    logger.info(
                        f"[任务 {task_id}] 数据集 {dataset.dataset_id} {dataset.universe} {dataset.region} {dataset.delay} 没有更多的数据字段。"
                    )
            logger.info(f"已处理数据集 {dataset.dataset_id} 的 {count} 个数据字段。")
            futures = []
            task_ids = []

    # 处理剩余的 futures
    for future in as_completed(futures):
        task_id = task_ids[futures.index(future)]
        datafields_response = future.result()
        if datafields_response and datafields_response.results:
            for datafield_data in datafields_response.results:
                create_datafield(session, datafield_data, dataset.id, task_id)
                count += 1
            logger.info(
                f"[任务 {task_id}] 已处理数据集 {dataset.dataset_id} {len(datafields_response.results)} 个数据字段。"
            )
        else:
            logger.info(
                f"[任务 {task_id}] 数据集 {dataset.dataset_id} 没有更多的数据字段。"
            )


def sync_datafields(instrument_type=None, max_workers=5):
    credentials = get_credentials()
    client = create_client(credentials)

    try:
        db_generator = get_db("datasets")
        session = next(db_generator)

        datasets = session.query(DataSet).all()
        logger.info(f"找到 {len(datasets)} 个数据集。")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for dataset in datasets:
                logger.info(
                    f"正在创建数据集 {dataset.dataset_id} {dataset.universe} {dataset.region} {dataset.delay} 的数据字段同步任务..."
                )
                create_and_process_tasks(
                    executor, client, dataset, instrument_type, session, max_workers
                )

        logger.info("数据字段同步成功。")
    except Exception as e:
        logger.error(f"同步数据字段时出错: {e}")
        session.rollback()
    finally:
        session.close()
        logger.info("数据库连接已关闭。")


if __name__ == "__main__":
    sync_datafields("EQUITY", 5)
