"""
数据集数据访问层模块
提供对数据集、分类、字段等数据实体的访问操作。
"""

from typing import List, Optional, Type

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Select

from alphapower.constants import Region  # 添加枚举类型导入
from alphapower.dal.base import EntityDAL
from alphapower.entity.data import (
    Category,
    DataField,
    Dataset,
    Pyramid,
    ResearchPaper,
    StatsData,
)


class DatasetDAL(EntityDAL[Dataset]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[Dataset] = Dataset

    async def find_by_dataset_id(
        self, dataset_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Dataset]:
        """
        通过 dataset_id 查询数据集。

        Args:
            dataset_id: 数据集的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的数据集实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, dataset_id=dataset_id)

    async def find_by_region(
        self, region: Region, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询特定区域的所有数据集。

        使用区域枚举值筛选数据集，简化区域相关查询。

        Args:
            region: 区域枚举值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的数据集列表。
        """
        return await self.deprecated_find_by(session=session, region=region.name)

    async def find_high_value_datasets(
        self, min_value: float, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询价值分数高于指定值的数据集。

        此方法用于筛选高价值数据集，支持数据价值分析。

        Args:
            min_value: 最小价值分数阈值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的数据集列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(Dataset).where(Dataset.value_score >= min_value)
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_category(
        self, category_id: str, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询属于特定分类的所有数据集。

        通过分类ID查找所有关联的数据集，使用连接查询实现。

        Args:
            category_id: 分类 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的数据集列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = (
            select(Dataset)
            .join(Dataset.categories)
            .where(Category.category_id == category_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_with_fields_count(
        self, min_count: int, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询包含至少指定数量字段的数据集。

        此方法用于筛选字段丰富的数据集，支持数据完整性分析。

        Args:
            min_count: 最小字段数量阈值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的数据集列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(Dataset).where(Dataset.field_count >= min_count)
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class CategoryDAL(EntityDAL[Category]):
    """
    Category 数据访问层类，提供对 Category 实体的特定操作。

    管理数据分类的CRUD操作，支持分类层次结构查询和管理。
    """

    entity_class: Type[Category] = Category

    async def find_by_category_id(
        self, category_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Category]:
        """
        通过 category_id 查询分类。

        Args:
            category_id: 分类的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的分类实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, category_id=category_id)

    async def find_top_level_categories(
        self, session: Optional[AsyncSession] = None
    ) -> List[Category]:
        """
        查询所有顶级分类（没有父分类的分类）。

        此方法用于获取分类层次结构的根节点，支持分类导航。

        Args:
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            所有顶级分类列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(Category).where(Category.parent_id.is_(None))
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_children_categories(
        self, parent_id: int, session: Optional[AsyncSession] = None
    ) -> List[Category]:
        """
        查询特定父分类的所有子分类。

        此方法用于获取分类层次结构的子节点，支持分类导航。

        Args:
            parent_id: 父分类的ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定父分类的所有子分类列表。
        """
        return await self.deprecated_find_by(session=session, parent_id=parent_id)


class DataFieldDAL(EntityDAL[DataField]):
    """
    DataField 数据访问层类，提供对 DataField 实体的特定操作。

    管理数据字段的CRUD操作，支持按类型、覆盖率等多种方式查询字段。
    """

    entity_class: Type[DataField] = DataField

    async def find_by_field_id(
        self, field_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[DataField]:
        """
        通过 field_id 查询数据字段。

        Args:
            field_id: 字段的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的字段实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, field_id=field_id)

    async def find_by_dataset(
        self, dataset_id: int, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """
        查询属于特定数据集的所有字段。

        Args:
            dataset_id: 数据集的ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            属于指定数据集的所有字段列表。
        """
        return await self.deprecated_find_by(session=session, dataset_id=dataset_id)

    async def find_by_type(
        self, field_type: str, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """
        查询特定类型的所有字段。

        此方法用于按类型筛选字段，便于类型分析和管理。

        Args:
            field_type: 字段类型字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定类型的所有字段列表。
        """
        return await self.deprecated_find_by(session=session, type=field_type)

    async def find_high_coverage_fields(
        self, min_coverage: float, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """
        查询覆盖率高于指定值的字段。

        此方法用于筛选高质量字段，支持数据完整性分析。

        Args:
            min_coverage: 最小覆盖率阈值，0.0到1.0之间。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的字段列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(DataField).where(DataField.coverage >= min_coverage)
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class StatsDataDAL(EntityDAL[StatsData]):
    """
    StatsData 数据访问层类，提供对 StatsData 实体的特定操作。

    管理统计数据的CRUD操作，支持按数据集和字段查询统计信息。
    """

    entity_class: Type[StatsData] = StatsData

    async def find_by_dataset_id(
        self, dataset_id: int, session: Optional[AsyncSession] = None
    ) -> List[StatsData]:
        """
        查询与特定数据集关联的所有统计数据。

        Args:
            dataset_id: 数据集的ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            与指定数据集关联的所有统计数据列表。
        """
        return await self.deprecated_find_by(session=session, data_set_id=dataset_id)

    async def find_by_data_field_id(
        self, data_field_id: int, session: Optional[AsyncSession] = None
    ) -> List[StatsData]:
        """
        查询与特定数据字段关联的所有统计数据。

        Args:
            data_field_id: 数据字段的ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            与指定数据字段关联的所有统计数据列表。
        """
        return await self.deprecated_find_by(session=session, data_field_id=data_field_id)


class ResearchPaperDAL(EntityDAL[ResearchPaper]):
    """
    ResearchPaper 数据访问层类，提供对 ResearchPaper 实体的特定操作。

    管理研究论文的CRUD操作，支持按类型查询论文。
    """

    entity_class: Type[ResearchPaper] = ResearchPaper

    async def find_by_type(
        self, paper_type: str, session: Optional[AsyncSession] = None
    ) -> List[ResearchPaper]:
        """
        查询特定类型的所有研究论文。

        Args:
            paper_type: 论文类型字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定类型的所有研究论文列表。
        """
        return await self.deprecated_find_by(session=session, type=paper_type)


class PyramidDAL(EntityDAL[Pyramid]):
    """
    Pyramid 数据访问层类，提供对 Pyramid 实体的特定操作。

    管理金字塔模型的CRUD操作，支持按区域和分类查询。
    """

    entity_class: Type[Pyramid] = Pyramid

    async def find_by_region(
        self, region: Region, session: Optional[AsyncSession] = None
    ) -> List[Pyramid]:
        """
        查询特定区域的所有金字塔。

        使用区域枚举值筛选金字塔模型，简化区域相关查询。

        Args:
            region: 区域枚举值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的金字塔列表。
        """
        return await self.deprecated_find_by(session=session, region=region.name)

    async def find_by_category(
        self, category_id: int, session: Optional[AsyncSession] = None
    ) -> List[Pyramid]:
        """
        查询与特定分类关联的所有金字塔。

        Args:
            category_id: 分类的ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            与指定分类关联的所有金字塔列表。
        """
        return await self.deprecated_find_by(session=session, category_id=category_id)
