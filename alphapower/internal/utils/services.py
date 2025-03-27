from typing import Optional, Type

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from alphapower.entity import (
    Alphas_Sample as AlphasSampleEntity,
    Data_Category as Category,
    Data_Subcategory as Subcategory,
)
from alphapower.internal.http_api import AlphaSample as AlphaSampleModel


async def get_or_create_entity(
    session: AsyncSession, model: Type, unique_field: str, data: object
) -> object:
    """
    通用方法，用于获取或创建数据库实体。
    """
    result = await session.execute(select(model).filter_by(**{unique_field: data.id}))
    entity: Optional[object] = result.scalars().first()
    if entity is None:
        entity = model(**{unique_field: data.id, "name": data.name})
        await session.add(entity)
    return entity


async def get_or_create_category(session: AsyncSession, category_name: str) -> Category:
    result = await session.execute(select(Category).filter_by(name=category_name))
    category: Optional[Category] = result.scalars().first()
    if category is None:
        category = Category(name=category_name)
        await session.add(category)
    return category


async def get_or_create_subcategory(
    session: AsyncSession, subcategory_name: str
) -> Subcategory:
    result = await session.execute(select(Subcategory).filter_by(name=subcategory_name))
    subcategory: Optional[Subcategory] = result.scalars().first()
    if subcategory is None:
        subcategory = Subcategory(name=subcategory_name)
        await session.add(subcategory)
    return subcategory


def create_sample(sample_data: AlphaSampleModel) -> Optional[AlphasSampleEntity]:
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

    return AlphasSampleEntity(
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
