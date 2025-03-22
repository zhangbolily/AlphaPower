from sqlalchemy.exc import IntegrityError
from worldquant.entity import Data_Category as Category, Data_Subcategory as Subcategory


def get_or_create_entity(session, model, unique_field, data):
    """
    通用方法，用于获取或创建数据库实体。

    参数:
    session: 数据库会话。
    model: 数据库模型类。
    unique_field: 唯一字段名称。
    data: 实体数据。

    返回:
    实体对象。
    """
    entity = session.query(model).filter_by(**{unique_field: data.id}).first()
    if entity is None:
        entity = model(**{unique_field: data.id, "name": data.name})
        try:
            session.add(entity)
            session.commit()
        except IntegrityError:
            session.rollback()
            entity = session.query(model).filter_by(**{unique_field: data.id}).first()
    return entity


def get_or_create_category(session, category_name):
    category = session.query(Category).filter_by(name=category_name).first()
    if category is None:
        category = Category(name=category_name)
        session.add(category)
        session.commit()
    return category


def get_or_create_subcategory(session, subcategory_name):
    subcategory = session.query(Subcategory).filter_by(name=subcategory_name).first()
    if subcategory is None:
        subcategory = Subcategory(name=subcategory_name)
        session.add(subcategory)
        session.commit()
    return subcategory


def create_sample(sample_data, sample_model):
    """
    通用方法，用于创建样本数据。

    参数:
    sample_data: 样本数据。
    sample_model: 样本模型类。

    返回:
    样本实例。
    """
    if sample_data is None:
        return None

    return sample_model(
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
