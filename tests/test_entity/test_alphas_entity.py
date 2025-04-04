"""测试 alphapower.entity.alphas 模块中定义的实体类。

本模块包含一系列测试，用于验证 alphapower.entity.alphas 模块中定义的所有数据库实体类
是否正确实现。测试使用真实数据库连接进行，验证实体类的创建、查询和关系映射。

测试覆盖以下实体类:
- AlphaBase: 基础映射类
- Setting: Alpha 设置
- Regular: Alpha 规则
- Classification: Alpha 分类
- Competition: Alpha 比赛
- SampleCheck: 样本检查
- Sample: 样本数据
- Alpha: 主要 Alpha 实体

以及它们之间的关系和中间表。
"""

import datetime
from typing import AsyncGenerator, Optional

import pytest
from sqlalchemy import Result, select
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import (
    DB_ALPHAS,
    AlphaType,
    Color,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    Stage,
    Status,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.entity import (
    Alpha,
    AlphaBase,
    Classification,
    Competition,
    Regular,
    Sample,
    SampleCheck,
    Setting,
    alphas_classifications,
    alphas_competitions,
)
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="session")
async def fixture_session() -> AsyncGenerator[AsyncSession, None]:
    """创建数据库会话用于测试。

    创建与真实数据库的连接会话，用于测试实体类的数据库操作。
    测试完成后会自动清理会话。

    Yields:
        AsyncSession: SQLAlchemy 异步会话对象。
    """
    async with get_db_session(DB_ALPHAS) as session:
        yield session
        # 注意：在生产环境测试中可能需要更复杂的数据清理策略
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


class TestBase:
    """测试 AlphaBase 基础映射类的基本属性。"""

    def test_base_class_exists(self) -> None:
        """验证 AlphaBase 类是否存在且具有必要的元数据属性。

        测试 AlphaBase 类是否正确定义，并具有 SQLAlchemy ORM 所需的
        metadata 和 registry 属性。
        """
        assert hasattr(AlphaBase, "metadata"), "AlphaBase 应该有 metadata 属性"
        assert hasattr(AlphaBase, "registry"), "AlphaBase 应该有 registry 属性"


class TestMiddleTables:
    """测试中间关系表结构。"""

    def test_alphas_classifications_table(self) -> None:
        """验证 alphas_classifications 中间表是否具有正确结构。

        测试 Alpha 和 Classification 之间的多对多关系表是否正确定义。
        """
        assert hasattr(alphas_classifications.c, "alpha_id"), "应包含 alpha_id 列"
        assert hasattr(
            alphas_classifications.c, "classification_id"
        ), "应包含 classification_id 列"
        assert (
            "alpha_classification" == alphas_classifications.name
        ), "表名应为 alpha_classification"

    def test_alphas_competitions_table(self) -> None:
        """验证 alphas_competitions 中间表是否具有正确结构。

        测试 Alpha 和 Competition 之间的多对多关系表是否正确定义。
        """
        assert hasattr(alphas_competitions.c, "alpha_id"), "应包含 alpha_id 列"
        assert hasattr(
            alphas_competitions.c, "competition_id"
        ), "应包含 competition_id 列"
        assert (
            "alpha_competition" == alphas_competitions.name
        ), "表名应为 alpha_competition"


class TestSetting:
    """测试 Setting 实体类的各项功能。"""

    async def test_create_setting(self, session: AsyncSession) -> None:
        """测试创建和查询 Setting 实例。

        验证是否可以创建 Setting 对象，将其保存到数据库，然后再次查询出来，
        并确保所有字段的值都正确保存。

        Args:
            session: 数据库会话对象。
        """
        setting: Setting = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOP1000,
            delay=Delay.ONE,
            decay=5,
            neutralization=Neutralization.INDUSTRY,
            truncation=3.0,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.ON,
            language="python",
            visualization=True,
            test_period="3m",
            max_trade=Switch.DEFAULT,
        )
        session.add(setting)
        await session.flush()  # 使用flush而非commit，让fixture管理事务

        result: Result = await session.execute(
            select(Setting).where(Setting.id == setting.id)
        )
        db_setting: Optional[Setting] = result.scalars().first()

        # 验证查询结果包含所有原始字段
        assert db_setting is not None
        assert db_setting.instrument_type == InstrumentType.EQUITY
        assert db_setting.region == Region.USA
        assert db_setting.universe == Universe.TOP1000
        assert db_setting.delay == Delay.ONE
        assert db_setting.decay == 5
        assert db_setting.neutralization == Neutralization.INDUSTRY
        assert db_setting.truncation == 3.0
        assert db_setting.pasteurization == Switch.ON
        assert db_setting.unit_handling == UnitHandling.VERIFY
        assert db_setting.nan_handling == Switch.ON
        assert db_setting.language == "python"
        assert db_setting.visualization is True
        assert db_setting.test_period == "3m"
        assert db_setting.max_trade == Switch.DEFAULT


class TestAlphaRegular:
    """测试 Regular 实体类（Alpha 规则）的各项功能。"""

    async def test_create_alpha_regular(self, session: AsyncSession) -> None:
        """测试创建和查询 Regular 实例。

        验证 Regular 对象的创建、保存和查询功能，确保所有字段正确保存。

        Args:
            session: 数据库会话对象。
        """
        regular: Regular = Regular(
            code="x = close(0) - open(0)",
            description="简单的日内收益率",
            operator_count=3,
        )
        session.add(regular)
        await session.flush()

        result: Result = await session.execute(
            select(Regular).where(Regular.id == regular.id)
        )
        db_regular: Optional[Regular] = result.scalars().first()

        assert db_regular is not None
        assert db_regular.code == "x = close(0) - open(0)"
        assert db_regular.description == "简单的日内收益率"
        assert db_regular.operator_count == 3


class TestClassification:
    """测试 Classification 实体类的各项功能。"""

    async def test_create_classification(self, session: AsyncSession) -> None:
        """测试创建和查询 Classification 实例。

        验证 Classification 对象的创建、保存和查询功能，
        特别是测试使用 classification_id 作为查询条件。

        Args:
            session: 数据库会话对象。
        """
        classification: Classification = Classification(
            classification_id="MOMENTUM", name="动量因子"
        )
        session.add(classification)
        await session.flush()

        result: Result = await session.execute(
            select(Classification).where(Classification.classification_id == "MOMENTUM")
        )
        db_classification: Optional[Classification] = result.scalars().first()

        assert db_classification is not None
        assert db_classification.classification_id == "MOMENTUM"
        assert db_classification.name == "动量因子"


class TestCompetition:
    """测试 Competition 实体类的各项功能。"""

    async def test_create_competition(self, session: AsyncSession) -> None:
        """测试创建和查询 Competition 实例。

        验证 Competition 对象的创建、保存和查询功能，
        特别是测试使用 competition_id 作为查询条件。

        Args:
            session: 数据库会话对象。
        """
        competition: Competition = Competition(
            competition_id="GLOBAL2023", name="2023全球Alpha大赛"
        )
        session.add(competition)
        await session.flush()

        result: Result = await session.execute(
            select(Competition).where(Competition.competition_id == "GLOBAL2023")
        )
        db_competition: Optional[Competition] = result.scalars().first()

        assert db_competition is not None
        assert db_competition.competition_id == "GLOBAL2023"
        assert db_competition.name == "2023全球Alpha大赛"


class TestSampleCheck:
    """测试 SampleCheck 实体类的各项功能。"""

    async def test_create_sample_check(self, session: AsyncSession) -> None:
        """测试创建和查询 SampleCheck 实例。

        验证 SampleCheck 对象的创建、保存和查询功能，包括日期字段的处理。

        Args:
            session: 数据库会话对象。
        """
        now: datetime.datetime = datetime.datetime.now()
        sample_check: SampleCheck = SampleCheck(
            name="mean_exposure_check",
            result="PASS",
            limit=0.05,
            value=0.03,
            date=now,
            competitions="GLOBAL2023",
            message="通过检查",
        )
        session.add(sample_check)
        await session.flush()

        result: Result = await session.execute(
            select(SampleCheck).where(SampleCheck.name == "mean_exposure_check")
        )
        db_sample_check: Optional[SampleCheck] = result.scalars().first()

        assert db_sample_check is not None
        assert db_sample_check.name == "mean_exposure_check"
        assert db_sample_check.result == "PASS"
        assert db_sample_check.limit == 0.05
        assert db_sample_check.value == 0.03
        assert db_sample_check.date == now
        assert db_sample_check.competitions == "GLOBAL2023"
        assert db_sample_check.message == "通过检查"


class TestSample:
    """测试 Sample 实体类的各项功能。"""

    async def test_create_sample(self, session: AsyncSession) -> None:
        """测试创建和查询 Sample 实例，以及与 SampleCheck 的关联关系。

        本测试验证 Sample 对象的创建、保存和查询功能，同时测试 Sample 与
        SampleCheck 之间的外键关联关系是否正确保存和恢复。

        Args:
            session: 数据库会话对象。
        """
        # 先创建一个 SampleCheck 用于建立关联关系
        sample_check: SampleCheck = SampleCheck(name="basic_check", result="PASS")
        session.add(sample_check)
        await session.flush()

        now: datetime.datetime = datetime.datetime.now()
        sample: Sample = Sample(
            pnl=5000.0,
            book_size=100000.0,
            long_count=50,
            short_count=50,
            turnover=0.2,
            returns=0.05,
            drawdown=0.02,
            margin=0.1,
            sharpe=1.8,
            fitness=0.75,
            start_date=now,
            checks_id=sample_check.id,
            self_correration=0.2,
            prod_correration=0.3,
            os_is_sharpe_ratio=0.9,
            pre_close_sharpe_ratio=1.2,
        )
        session.add(sample)
        await session.flush()

        result: Result = await session.execute(
            select(Sample).where(Sample.id == sample.id)
        )
        first_db_sample = result.scalars().first()
        assert first_db_sample is not None  # Ensure db_sample is not None
        db_sample: Optional[Sample] = first_db_sample  # Type hint after the assertion

        # 验证所有字段都正确保存
        assert db_sample is not None
        assert db_sample.pnl == 5000.0
        assert db_sample.book_size == 100000.0
        assert db_sample.long_count == 50
        assert db_sample.short_count == 50
        assert db_sample.turnover == 0.2
        assert db_sample.returns == 0.05
        assert db_sample.drawdown == 0.02
        assert db_sample.margin == 0.1
        assert db_sample.sharpe == 1.8
        assert db_sample.fitness == 0.75
        assert db_sample.start_date == now
        assert db_sample.checks_id == sample_check.id
        assert db_sample.self_correration == 0.2
        assert db_sample.prod_correration == 0.3
        assert db_sample.os_is_sharpe_ratio == 0.9
        assert db_sample.pre_close_sharpe_ratio == 1.2


class TestAlpha:
    """测试 Alpha 实体类的各项功能，包括各种关联关系。"""

    async def test_create_alpha(self, session: AsyncSession) -> None:
        """测试创建和查询 Alpha 实例及其复杂关联关系。

        本测试验证 Alpha 实体类的完整功能，包括：
        1. 基本字段的创建和查询
        2. 与 Setting 和 Regular 的一对多关系
        3. 与 Sample 的多个一对一关系（用于不同分析阶段）
        4. 与 Classification 和 Competition 的多对多关系

        Args:
            session: 数据库会话对象。
        """
        # 创建依赖的相关实例：Setting, Regular, Samples, Classifications, Competitions
        setting: Setting = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            decay=10,
            neutralization=Neutralization.SECTOR,
            truncation=2.5,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.ON,
            language="python",
            visualization=True,
        )

        regular: Regular = Regular(
            code="return (close(0) - close(5)) / close(5)",
            description="5日收益率",
            operator_count=4,
        )

        session.add_all([setting, regular])
        await session.flush()

        # 创建 Sample 实例用于关联
        sample1: Sample = Sample(pnl=1000.0, start_date=datetime.datetime.now())
        sample2: Sample = Sample(pnl=1200.0, start_date=datetime.datetime.now())
        sample3: Sample = Sample(pnl=900.0, start_date=datetime.datetime.now())
        sample4: Sample = Sample(pnl=1100.0, start_date=datetime.datetime.now())
        sample5: Sample = Sample(pnl=1300.0, start_date=datetime.datetime.now())

        session.add_all([sample1, sample2, sample3, sample4, sample5])
        await session.flush()

        # 创建分类和比赛
        classification1: Classification = Classification(
            classification_id="TREND", name="趋势因子"
        )
        classification2: Classification = Classification(
            classification_id="VALUE", name="价值因子"
        )
        competition1: Competition = Competition(
            competition_id="CN2023", name="中国Alpha大赛2023"
        )

        session.add_all([classification1, classification2, competition1])
        await session.flush()

        # 创建 Alpha 实例
        now: datetime.datetime = datetime.datetime.now()
        alpha: Alpha = Alpha(
            alpha_id="ALPHA123",
            type=AlphaType.SUPER,
            author="test_user",
            settings_id=setting.id,
            regular_id=regular.id,
            date_created=now,
            date_submitted=now,
            date_modified=now,
            name="测试Alpha",
            favorite=True,
            hidden=False,
            color=Color.RED,
            category="stock",
            tags=["trend", "momentum"],
            grade=Grade.EXCELLENT,  # 修改：用 Grade.EXCELLENT 替换 Grade.A
            stage=Stage.PROD,  # 修改：用 Stage.PROD 替换 Stage.PRODUCTION
            status=Status.ACTIVE,
            in_sample_id=sample1.id,
            out_sample_id=sample2.id,
            train_id=sample3.id,
            test_id=sample4.id,
            prod_id=sample5.id,
            themes="market_neutral",
            pyramids="basic",
            team="alpha_team",
        )

        # 建立多对多关系
        alpha.classifications.append(classification1)
        alpha.classifications.append(classification2)
        alpha.competitions.append(competition1)

        session.add(alpha)
        await session.flush()

        # 验证基本字段
        result: Result = await session.execute(
            select(Alpha).where(Alpha.alpha_id == "ALPHA123")
        )
        db_alpha: Optional[Alpha] = result.scalars().first()

        assert db_alpha is not None
        assert db_alpha.alpha_id == "ALPHA123"
        assert db_alpha.type == AlphaType.SUPER
        assert db_alpha.author == "test_user"
        assert db_alpha.name == "测试Alpha"
        assert db_alpha.favorite is True
        assert db_alpha.hidden is False
        assert db_alpha.color == Color.RED
        assert db_alpha.grade == Grade.EXCELLENT  # 修改：与创建时一致
        assert db_alpha.stage == Stage.PROD  # 修改：与创建时一致
        assert db_alpha.status == Status.ACTIVE

        # 验证关联关系
        assert db_alpha.settings_id == setting.id
        assert db_alpha.regular_id == regular.id
        assert db_alpha.in_sample_id == sample1.id
        assert db_alpha.out_sample_id == sample2.id
        assert db_alpha.train_id == sample3.id
        assert db_alpha.test_id == sample4.id
        assert db_alpha.prod_id == sample5.id

        # 验证多对多关系
        assert len(db_alpha.classifications) == 2
        assert len(db_alpha.competitions) == 1

        # 使用集合验证分类名称，避免顺序问题
        classification_names = {c.name for c in db_alpha.classifications}
        assert classification_names == {"趋势因子", "价值因子"}

        assert db_alpha.competitions[0].name == "中国Alpha大赛2023"

        # 测试字段属性
        assert db_alpha.themes == "market_neutral"
        assert db_alpha.pyramids == "basic"
        assert db_alpha.team == "alpha_team"
        assert set(db_alpha.tags) == {"trend", "momentum"}

        # 测试标签相关方法
        db_alpha.add_tag("new_tag")
        await session.flush()

        result = await session.execute(
            select(Alpha).where(Alpha.alpha_id == "ALPHA123")
        )
        updated_alpha = result.scalars().first()
        assert updated_alpha is not None
        assert "new_tag" in updated_alpha.tags
        assert len(updated_alpha.tags) == 3

        updated_alpha.remove_tag("trend")
        await session.flush()

        result = await session.execute(
            select(Alpha).where(Alpha.alpha_id == "ALPHA123")
        )
        final_alpha = result.scalars().first()
        assert final_alpha is not None
        assert "trend" not in final_alpha.tags
        assert "momentum" in final_alpha.tags
        assert "new_tag" in final_alpha.tags
