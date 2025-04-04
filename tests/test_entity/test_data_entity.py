"""针对数据模型类的单元测试模块。

本模块包含用于测试数据模型类的测试用例，验证模型类的属性和关系是否正确设置，
以及数据库操作是否正常工作。

测试覆盖以下实体类:
- Category: 数据分类
- Dataset: 数据集
- DataField: 数据字段
- StatsData: 统计数据
- ResearchPaper: 研究论文
- Pyramid: 金字塔模型
"""

from typing import AsyncGenerator

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import DB_DATA, DataFieldType, Delay, Region, Universe

# 导入会自动注册 DB_DATA 测试数据库
from alphapower.entity.data import (
    Category,
    DataField,
    Dataset,
    Pyramid,
    ResearchPaper,
    StatsData,
)
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="db_session")
async def fixture_db_session() -> AsyncGenerator[AsyncSession, None]:
    """获取测试数据库会话。

    使用异步上下文管理器提供测试数据库会话，确保会话在测试后正确关闭。
    通过 alphapower.entity 导入时已自动注册了 DB_DATA 测试数据库。

    Yields:
        AsyncSession: 用于测试的异步会话对象
    """
    # 直接使用已注册的 DB_DATA 数据库
    async with get_db_session(DB_DATA) as session:
        yield session


class TestCategory:
    """测试 Category 实体类的各项功能。"""

    @pytest.mark.asyncio
    async def test_category_crud(self, db_session: AsyncSession) -> None:
        """测试 Category 模型的 CRUD 操作。

        验证 Category 对象的创建、查询、更新和删除操作是否正常工作。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建新分类
        category: Category = Category(category_id="CAT001", name="金融数据")
        db_session.add(category)
        await db_session.commit()

        # 查询分类
        result: Result = await db_session.execute(
            select(Category).where(Category.category_id == "CAT001")
        )
        retrieved_category: Category = result.scalar_one()

        # 验证分类数据
        assert retrieved_category.id is not None
        assert retrieved_category.category_id == "CAT001"
        assert retrieved_category.name == "金融数据"

        # 更新分类
        retrieved_category.name = "更新后的金融数据"
        await db_session.commit()

        # 验证更新后的数据
        result = await db_session.execute(
            select(Category).where(Category.category_id == "CAT001")
        )
        updated_category: Category = result.scalar_one()
        assert updated_category.name == "更新后的金融数据"

        # 删除分类
        await db_session.delete(updated_category)
        await db_session.commit()

        # 验证删除成功
        result = await db_session.execute(
            select(Category).where(Category.category_id == "CAT001")
        )
        assert result.scalar_one_or_none() is None


class TestDataset:
    """测试 Dataset 实体类的各项功能。"""

    @pytest.mark.asyncio
    async def test_dataset_crud(self, db_session: AsyncSession) -> None:
        """测试 Dataset 模型的 CRUD 操作。

        验证 Dataset 对象的创建、查询和更新操作是否正常工作，
        以及与 Category 的关联关系是否正确建立。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建分类
        category: Category = Category(category_id="CAT002", name="经济数据")
        db_session.add(category)

        # 创建子分类
        subcategory: Category = Category(
            category_id="SUBCAT001", name="GDP相关数据", parent=category
        )
        db_session.add(subcategory)
        await db_session.commit()

        # 创建数据集
        dataset: Dataset = Dataset(
            dataset_id="DS001",
            name="GDP数据集",
            description="包含全球各国GDP数据",
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=0.95,
            value_score=8.5,
            user_count=1000,
            alpha_count=50,
            field_count=20,
            pyramid_multiplier=1.5,
        )
        # 建立多对多关联关系
        dataset.categories.append(category)
        dataset.subcategories.append(subcategory)
        db_session.add(dataset)
        await db_session.commit()

        # 查询数据集
        result: Result = await db_session.execute(
            select(Dataset).where(Dataset.dataset_id == "DS001")
        )
        retrieved_dataset: Dataset = result.scalar_one()

        # 验证数据集数据
        assert retrieved_dataset.id is not None
        assert retrieved_dataset.dataset_id == "DS001"
        assert retrieved_dataset.name == "GDP数据集"
        assert retrieved_dataset.region == Region.GLB
        assert retrieved_dataset.delay == Delay.ONE
        assert retrieved_dataset.universe == Universe.TOP3000
        assert retrieved_dataset.pyramid_multiplier == 1.5
        assert retrieved_dataset.categories[0].category_id == "CAT002"
        assert retrieved_dataset.subcategories[0].category_id == "SUBCAT001"
        assert retrieved_dataset.subcategories[0].parent.category_id == "CAT002"

        # 更新数据集
        retrieved_dataset.value_score = 9.0
        await db_session.commit()

        # 验证更新后的数据
        result = await db_session.execute(
            select(Dataset).where(Dataset.dataset_id == "DS001")
        )
        updated_dataset: Dataset = result.scalar_one()
        assert updated_dataset.value_score == 9.0


class TestDataField:
    """测试 DataField 实体类的各项功能。"""

    @pytest.mark.asyncio
    async def test_data_field_crud(self, db_session: AsyncSession) -> None:
        """测试 DataField 模型的 CRUD 操作。

        验证 DataField 对象的创建和查询功能，以及与 Dataset 和
        Category 的关联关系是否正确建立。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建分类和数据集
        category: Category = Category(category_id="CAT003", name="股票数据")
        db_session.add(category)

        dataset: Dataset = Dataset(
            dataset_id="DS002",
            name="股价数据集",
            description="包含股票价格数据",
            region=Region.CHN,
            delay=Delay.ONE,
            universe=Universe.TOP2000U,
            coverage=1.0,
            value_score=9.5,
            user_count=5000,
            alpha_count=200,
            field_count=10,
        )
        db_session.add(dataset)
        await db_session.commit()

        # 创建数据字段
        data_field: DataField = DataField(
            field_id="FD001",
            description="收盘价",
            dataset=dataset,
            region=Region.CHN,
            delay=Delay.ONE,
            universe=Universe.TOP2000U,
            type=DataFieldType.VECTOR,
            coverage=1.0,
            user_count=4000,
            alpha_count=180,
        )
        # 建立多对多关系
        data_field.categories.append(category)
        db_session.add(data_field)
        await db_session.commit()

        # 查询数据字段
        result: Result = await db_session.execute(
            select(DataField).where(DataField.field_id == "FD001")
        )
        retrieved_field: DataField = result.scalar_one()

        # 验证数据字段数据
        assert retrieved_field.id is not None
        assert retrieved_field.field_id == "FD001"
        assert retrieved_field.description == "收盘价"
        assert retrieved_field.dataset.dataset_id == "DS002"
        assert retrieved_field.region == Region.CHN
        assert retrieved_field.delay == Delay.ONE
        assert retrieved_field.universe == Universe.TOP2000U
        assert retrieved_field.type == DataFieldType.VECTOR
        assert retrieved_field.categories[0].category_id == "CAT003"


class TestStatsData:
    """测试 StatsData 实体类的各项功能。"""

    @pytest.mark.asyncio
    async def test_stats_data_crud(self, db_session: AsyncSession) -> None:
        """测试 StatsData 模型的 CRUD 操作。

        验证 StatsData 对象的创建和查询功能，以及与 Dataset 和
        DataField 的关联关系是否正确建立。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建数据集和数据字段
        dataset: Dataset = Dataset(
            dataset_id="DS003",
            name="经济指标",
            description="经济指标数据集",
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=0.9,
            value_score=8.0,
            user_count=2000,
            alpha_count=100,
            field_count=30,
        )
        db_session.add(dataset)

        data_field: DataField = DataField(
            field_id="FD002",
            description="通胀率",
            dataset=dataset,
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            type=DataFieldType.VECTOR,
            coverage=0.9,
            user_count=1500,
            alpha_count=80,
        )
        db_session.add(data_field)
        await db_session.commit()

        # 创建统计数据
        stats_data0: StatsData = StatsData(
            data_set=dataset,
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=0.9,
            value_score=8.0,
            user_count=1000,
            alpha_count=50,
            field_count=1,
        )

        stats_data1: StatsData = StatsData(
            data_field=data_field,
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=0.9,
            value_score=8.0,
            user_count=1000,
            alpha_count=50,
            field_count=1,
        )

        db_session.add_all([stats_data0, stats_data1])
        await db_session.commit()

        # 查询统计数据
        result: Result = await db_session.execute(
            select(StatsData).where((StatsData.data_set_id == dataset.id))
        )
        retrieved_stats: StatsData = result.scalar_one()
        await retrieved_stats.awaitable_attrs.data_set

        # 验证统计数据
        assert retrieved_stats.id is not None
        assert retrieved_stats.region == Region.GLB
        assert retrieved_stats.delay == Delay.ONE
        assert retrieved_stats.universe == Universe.TOP3000
        assert retrieved_stats.value_score == 8.0
        assert retrieved_stats.data_set.dataset_id == "DS003"

        result = await db_session.execute(
            select(StatsData).where((StatsData.data_field_id == data_field.id))
        )
        retrieved_stats: StatsData = result.scalar_one()
        await retrieved_stats.awaitable_attrs.data_field
        # 验证统计数据
        assert retrieved_stats.id is not None
        assert retrieved_stats.region == Region.GLB
        assert retrieved_stats.delay == Delay.ONE
        assert retrieved_stats.universe == Universe.TOP3000
        assert retrieved_stats.value_score == 8.0
        assert retrieved_stats.data_field.field_id == "FD002"


class TestResearchPaper:
    """测试 ResearchPaper 实体类及其关联关系。"""

    @pytest.mark.asyncio
    async def test_research_paper_and_dataset_relationship(
        self, db_session: AsyncSession
    ) -> None:
        """测试 ResearchPaper 与 Dataset 之间的多对多关系。

        验证 ResearchPaper 与 Dataset 之间的多对多关系是否正确建立和查询。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建数据集
        dataset1: Dataset = Dataset(
            dataset_id="DS004",
            name="金融市场数据",
            description="金融市场分析数据",
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=1.0,
            value_score=9.0,
            user_count=3000,
            alpha_count=150,
            field_count=40,
        )

        dataset2: Dataset = Dataset(
            dataset_id="DS005",
            name="宏观经济数据",
            description="宏观经济分析数据",
            region=Region.GLB,
            delay=Delay.ONE,
            universe=Universe.TOP3000,
            coverage=0.95,
            value_score=8.5,
            user_count=2500,
            alpha_count=120,
            field_count=35,
        )

        db_session.add_all([dataset1, dataset2])
        await db_session.commit()

        # 创建研究论文
        paper1: ResearchPaper = ResearchPaper(
            type="学术论文",
            title="金融市场波动性研究",
            url="https://example.com/paper1",
        )

        paper2: ResearchPaper = ResearchPaper(
            type="行业报告", title="宏观经济趋势分析", url="https://example.com/paper2"
        )

        # 建立多对多关系
        paper1.datasets.append(dataset1)
        paper2.datasets.extend([dataset1, dataset2])

        db_session.add_all([paper1, paper2])
        await db_session.commit()

        # 验证数据集与论文的多对多关系
        result: Result = await db_session.execute(
            select(Dataset).where(Dataset.dataset_id == "DS004")
        )
        ds1: Dataset = result.scalar_one()
        await ds1.awaitable_attrs.research_papers
        assert len(ds1.research_papers) == 2
        assert any(p.title == "金融市场波动性研究" for p in ds1.research_papers)
        assert any(p.title == "宏观经济趋势分析" for p in ds1.research_papers)

        result = await db_session.execute(
            select(Dataset).where(Dataset.dataset_id == "DS005")
        )
        ds2: Dataset = result.scalar_one()
        await ds2.awaitable_attrs.research_papers
        assert len(ds2.research_papers) == 1
        assert ds2.research_papers[0].title == "宏观经济趋势分析"

        # 验证论文与数据集的多对多关系
        result = await db_session.execute(
            select(ResearchPaper).where(ResearchPaper.title == "宏观经济趋势分析")
        )
        paper: ResearchPaper = result.scalar_one()
        await paper.awaitable_attrs.datasets
        assert len(paper.datasets) == 2
        assert any(d.dataset_id == "DS004" for d in paper.datasets)
        assert any(d.dataset_id == "DS005" for d in paper.datasets)


class TestCategoryRelationships:
    """测试 Category 与其他实体的关联关系。"""

    @pytest.mark.asyncio
    async def test_category_relationship_with_dataset_and_datafield(
        self, db_session: AsyncSession
    ) -> None:
        """测试 Category 与 Dataset 和 DataField 之间的多对多关系。

        验证 Category 对象与 Dataset 和 DataField 的多对多关系是否正确建立和查询。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建分类
        category: Category = Category(category_id="CAT004", name="债券数据")
        db_session.add(category)
        await db_session.commit()

        # 创建数据集和数据字段
        dataset: Dataset = Dataset(
            dataset_id="DS006",
            name="政府债券数据",
            description="政府债券相关数据",
            region=Region.CHN,
            delay=Delay.ONE,
            universe=Universe.TOP2000U,
            coverage=0.98,
            value_score=8.8,
            user_count=1800,
            alpha_count=90,
            field_count=15,
        )
        # 添加分类关系
        dataset.categories.append(category)

        data_field1: DataField = DataField(
            field_id="FD003",
            description="收益率",
            dataset=dataset,
            region=Region.CHN,
            delay=Delay.ONE,
            universe=Universe.TOP2000U,
            type=DataFieldType.VECTOR,
            coverage=0.98,
            user_count=1500,
            alpha_count=75,
        )
        # 添加分类关系
        data_field1.categories.append(category)

        data_field2: DataField = DataField(
            field_id="FD004",
            description="到期日",
            dataset=dataset,
            region=Region.CHN,
            delay=Delay.ONE,
            universe=Universe.TOP2000U,
            type=DataFieldType.VECTOR,
            coverage=1.0,
            user_count=1600,
            alpha_count=70,
        )
        # 添加分类关系
        data_field2.categories.append(category)

        db_session.add_all([dataset, data_field1, data_field2])
        await db_session.commit()

        # 验证关系
        result: Result = await db_session.execute(
            select(Category).where(Category.category_id == "CAT004")
        )
        retrieved_category: Category = result.scalar_one()
        await retrieved_category.awaitable_attrs.datasets
        await retrieved_category.awaitable_attrs.data_fields

        assert len(retrieved_category.datasets) == 1
        assert retrieved_category.datasets[0].dataset_id == "DS006"
        assert len(retrieved_category.data_fields) == 2
        assert any(f.field_id == "FD003" for f in retrieved_category.data_fields)
        assert any(f.field_id == "FD004" for f in retrieved_category.data_fields)


class TestPyramid:
    """测试 Pyramid 实体类的各项功能。"""

    @pytest.mark.asyncio
    async def test_pyramid_crud(self, db_session: AsyncSession) -> None:
        """测试 Pyramid 模型的 CRUD 操作。

        验证 Pyramid 对象的创建、查询和更新操作是否正常工作，
        以及与 Category 的关联关系是否正确建立。

        Args:
            db_session: 数据库会话对象。
        """
        # 创建分类
        category: Category = Category(category_id="CAT005", name="金字塔测试分类")
        db_session.add(category)
        await db_session.commit()

        # 创建金字塔模型
        pyramid: Pyramid = Pyramid(
            delay=Delay.ONE,
            multiplier=2.5,
            region=Region.CHN,
            category=category,
        )
        db_session.add(pyramid)
        await db_session.commit()

        # 查询金字塔模型
        result: Result = await db_session.execute(
            select(Pyramid).where(Pyramid.category_id == category.id)
        )
        retrieved_pyramid: Pyramid = result.scalar_one()

        # 验证金字塔模型数据
        assert retrieved_pyramid.id is not None
        assert retrieved_pyramid.delay == Delay.ONE
        assert retrieved_pyramid.multiplier == 2.5
        assert retrieved_pyramid.region == Region.CHN
        assert retrieved_pyramid.category.category_id == "CAT005"
        assert retrieved_pyramid.category.name == "金字塔测试分类"

        # 更新金字塔模型
        retrieved_pyramid.multiplier = 3.0
        retrieved_pyramid.delay = Delay.ZERO
        await db_session.commit()

        # 验证更新后的数据
        result = await db_session.execute(
            select(Pyramid).where(Pyramid.id == retrieved_pyramid.id)
        )
        updated_pyramid: Pyramid = result.scalar_one()
        assert updated_pyramid.multiplier == 3.0
        assert updated_pyramid.delay == Delay.ZERO
