"""
通用方法，用于获取或创建数据库实体。
"""

from typing import Optional, Protocol, Type, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from alphapower.entity import AggregateData, Category
from alphapower.view.alpha import AggregateDataView


class HasIdAndName(Protocol):
    """
    定义一个协议，约束 T 必须包含 id 和 name 属性
    """

    id: str
    name: str


T = TypeVar("T")


async def get_or_create_entity(
    session: AsyncSession, model: Type[T], unique_field: str, data: HasIdAndName
) -> T:
    """
    获取或创建数据库实体。

    参数:
    session: 数据库会话。
    model: 实体模型类。
    unique_field: 唯一字段名称。
    data: 包含实体数据的对象，必须包含 id 和 name 属性。

    返回:
    数据库实体对象，类型为 T。
    """
    if not hasattr(model, unique_field):
        raise ValueError(f"实体类型 {model.__name__} 没有属性 {unique_field}")

    result = await session.execute(select(model).filter_by(**{unique_field: data.id}))
    entity: Optional[T] = result.scalars().first()
    if entity is None:
        entity = model(**{unique_field: data.id, "name": data.name})  # 类型为 T
        session.add(entity)
        await session.commit()
    return entity


async def get_or_create_category(session: AsyncSession, category_name: str) -> Category:
    """
    获取或创建数据分类。

    参数:
    session: 数据库会话。
    category_name: 分类名称。

    返回:
    数据分类对象。
    """
    result = await session.execute(select(Category).filter_by(name=category_name))
    category: Optional[Category] = result.scalars().first()
    if category is None:
        category = Category(name=category_name)
        session.add(category)
        await session.commit()
    return category


async def get_or_create_subcategory(
    session: AsyncSession, subcategory_name: str
) -> Category:
    """
    获取或创建数据子分类。

    参数:
    session: 数据库会话。
    subcategory_name: 子分类名称。

    返回:
    数据子分类对象。
    """
    result = await session.execute(select(Category).filter_by(name=subcategory_name))
    subcategory: Optional[Category] = result.scalars().first()
    if subcategory is None:
        subcategory = Category(name=subcategory_name)
        session.add(subcategory)
        await session.commit()
    return subcategory


def create_aggregate_data(
    sample_data: Optional[AggregateDataView],
) -> Optional[AggregateData]:
    """
    创建样本数据。

    参数:
    sample_data: 样本数据对象。

    返回:
    样本实体对象，或 None 如果样本数据为空。
    """
    if sample_data is None:
        return None

    try:
        aggregate_data: AggregateData = AggregateData(
            pnl=sample_data.pnl,
            book_size=sample_data.book_size,
            long_count=sample_data.long_count,
            short_count=sample_data.short_count,
            turnover=sample_data.turnover,
            returns=sample_data.returns,
            drawdown=sample_data.drawdown,
            margin=sample_data.margin,
            sharpe=sample_data.sharpe,
            fitness=sample_data.fitness,
            self_correration=sample_data.self_correlation,
            prod_correration=sample_data.prod_correlation,
            os_is_sharpe_ratio=sample_data.os_is_sharpe_ratio,
            pre_close_sharpe_ratio=sample_data.pre_close_sharpe_ratio,
            start_date=sample_data.start_date,
            checks=sample_data.checks,
        )

        return aggregate_data
    except Exception as e:
        raise ValueError(f"创建样本数据失败: {e}, 样本数据: {sample_data}") from e
