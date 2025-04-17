"""测试 alphapower.entity.dal 模块中与 Alpha 相关的数据访问层功能。

本模块包含一系列测试，用于验证 alphapower.entity.dal 模块中定义的与 Alpha 相关的
数据访问层功能是否正确实现。测试使用真实数据库连接进行，验证数据访问操作的正确性。

测试覆盖以下数据访问层功能:
- AlphaDAL: Alpha数据访问层
- RegularDAL: 规则数据访问层
- ClassificationDAL: 分类数据访问层
- CompetitionDAL: 比赛数据访问层
- SampleDAL: 样本数据访问层
- SampleCheckDAL: 样本检查数据访问层
"""

from datetime import datetime
from typing import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import (
    ALPHA_ID_LENGTH,
    AlphaType,
    Color,
    CompetitionScoring,
    CompetitionStatus,
    Database,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Stage,
    Status,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.dal.alphas import (
    AlphaDAL,
    ClassificationDAL,
    CompetitionDAL,
    RegularDAL,
    SampleCheckDAL,
    SampleDAL,
)
from alphapower.entity import (
    Alpha,
    Classification,
    Competition,
    Regular,
    Sample,
    Setting,
)
from alphapower.internal.db_session import get_db_session

# pylint: disable=too-many-lines


@pytest.fixture(name="alphas_session")
async def fixture_alphas_session() -> AsyncGenerator[AsyncSession, None]:
    """创建Alpha数据库会话用于测试。

    创建与真实数据库的连接会话，用于测试Alpha相关的数据访问层的操作。
    测试完成后会自动清理会话。

    Yields:
        AsyncSession: SQLAlchemy 异步会话对象。
    """
    async with get_db_session(Database.ALPHAS) as alphas_session:
        yield alphas_session
        # 注意：在生产环境测试中可能需要更复杂的数据清理策略
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


@pytest.fixture(name="test_setting")
async def fixture_test_setting(alphas_session: AsyncSession) -> Setting:
    """创建一个用于测试的 Setting 对象。

    Args:
        alphas_session: 数据库会话对象。

    Returns:
        Setting: 创建的 Setting 对象。
    """
    setting = Setting(
        language=RegularLanguage.PYTHON,
        decay=10,
        truncation=0.01,
        visualization=False,
        instrument_type=InstrumentType.EQUITY,
        region=Region.USA,
        universe=Universe.TOP3000,
        delay=Delay.ONE,
        neutralization=Neutralization.MARKET,
        pasteurization=Switch.OFF,
        unit_handling=UnitHandling.VERIFY,
        nan_handling=Switch.OFF,
    )
    alphas_session.add(setting)
    await alphas_session.flush()
    return setting


@pytest.fixture(name="test_regular")
async def fixture_test_regular(alphas_session: AsyncSession) -> Regular:
    """创建一个用于测试的 Regular 对象。

    Args:
        alphas_session: 数据库会话对象。

    Returns:
        Regular: 创建的 Regular 对象。
    """
    regular = Regular(code="test code", operator_count=1)
    alphas_session.add(regular)
    await alphas_session.flush()
    return regular


@pytest.fixture(name="test_sample")
async def fixture_test_sample(alphas_session: AsyncSession) -> Sample:
    """创建一个用于测试的 Sample 对象。

    Args:
        alphas_session: 数据库会话对象。

    Returns:
        Sample: 创建的 Sample 对象。
    """
    sample = Sample(start_date=datetime.now(), sharpe=1.0)
    alphas_session.add(sample)
    await alphas_session.flush()
    return sample


class TestAlphaDAL:
    """测试 AlphaDAL 类的各项功能。"""

    async def test_find_by_alpha_id(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_by_alpha_id 方法。

        验证 find_by_alpha_id 方法是否能够正确查询指定 alpha_id 的 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        alpha = Alpha(
            alpha_id="SPECIFIC_TEST_1".ljust(ALPHA_ID_LENGTH),  # 确保长度符合要求
            type=AlphaType.REGULAR,
            author="tester",
            name="特定DAL测试1",
            settings_id=test_setting.id,
            regular_id=test_regular.id,
            date_created=datetime.now(),
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.IS,
            status=Status.UNSUBMITTED,
        )
        alphas_session.add(alpha)
        await alphas_session.flush()

        # 使用特定方法查询
        result = await alpha_dal.find_by_alpha_id(
            "SPECIFIC_TEST_1".ljust(ALPHA_ID_LENGTH)
        )

        # 验证查询结果
        assert result is not None
        assert result.alpha_id == "SPECIFIC_TEST_1".ljust(ALPHA_ID_LENGTH)
        assert result.name == "特定DAL测试1"

    async def test_find_by_author(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_by_author 方法。

        验证 find_by_author 方法是否能够正确查询指定作者的所有 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        author = "test_author"
        alphas = [
            Alpha(
                alpha_id=f"AUTHOR_TEST_{i}".ljust(ALPHA_ID_LENGTH),
                type=AlphaType.REGULAR,
                author=author,
                name=f"作者测试_{i}",
                settings_id=test_setting.id,
                regular_id=test_regular.id,
                date_created=datetime.now(),
                favorite=False,
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.IS,
                status=Status.UNSUBMITTED,
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(alphas)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await alpha_dal.find_by_author(author)

        # 验证查询结果
        assert len(results) >= 3
        assert all(a.author == author for a in results)
        assert any(a.alpha_id.startswith("AUTHOR_TEST_") for a in results)

    async def test_find_by_status(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_by_status 方法。

        验证 find_by_status 方法是否能够正确查询指定状态的所有 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        statuses = [Status.UNSUBMITTED, Status.ACTIVE, Status.DECOMMISSIONED]
        for status in statuses:
            alphas = [
                Alpha(
                    alpha_id=f"{status.name}_ALPHA_{i}".ljust(ALPHA_ID_LENGTH),
                    type=AlphaType.REGULAR,
                    author=f"author_{i}",
                    name=f"{status.value}测试_{i}",
                    status=status,
                    settings_id=test_setting.id,
                    regular_id=test_regular.id,
                    date_created=datetime.now(),
                    favorite=False,
                    hidden=False,
                    color=Color.NONE,
                    grade=Grade.DEFAULT,
                    stage=Stage.IS,
                )
                for i in range(1, 3)
            ]
            alphas_session.add_all(alphas)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await alpha_dal.find_by_status(Status.ACTIVE)  # 使用枚举成员

        # 验证查询结果
        assert len(results) >= 2
        assert all(a.status == Status.ACTIVE for a in results)

    async def test_find_favorites(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_favorites 方法。

        验证 find_favorites 方法是否能够正确查询指定作者的收藏 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据 - 收藏和非收藏的 Alpha
        author = "favorite_tester"
        favorite_alphas = [
            Alpha(
                alpha_id=f"FAV_ALPHA_{i}".ljust(ALPHA_ID_LENGTH),
                type=AlphaType.REGULAR,
                author=author,
                name=f"收藏测试_{i}",
                favorite=True,
                settings_id=test_setting.id,
                regular_id=test_regular.id,
                date_created=datetime.now(),
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.IS,
                status=Status.UNSUBMITTED,
            )
            for i in range(1, 4)
        ]
        non_favorite_alphas = [
            Alpha(
                alpha_id=f"NON_FAV_ALPHA_{i}".ljust(ALPHA_ID_LENGTH),
                type=AlphaType.REGULAR,
                author=author,
                name=f"非收藏测试_{i}",
                favorite=False,
                settings_id=test_setting.id,
                regular_id=test_regular.id,
                date_created=datetime.now(),
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.IS,
                status=Status.UNSUBMITTED,
            )
            for i in range(1, 3)
        ]
        alphas_session.add_all(favorite_alphas + non_favorite_alphas)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await alpha_dal.find_favorites(author)

        # 验证查询结果
        assert len(results) >= 3
        assert all(a.favorite is True and a.author == author for a in results)

    async def test_find_by_classification(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_by_classification 方法。

        验证 find_by_classification 方法能否查询属于特定分类的所有 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)
        classification_dal = ClassificationDAL(alphas_session)

        # 创建测试数据 - 分类
        classification = Classification(
            classification_id="TEST_FIND_CLASS", name="测试分类查询"
        )
        classification_dal.session.add(classification)
        await classification_dal.session.flush()

        # 创建测试数据 - Alpha
        alpha = Alpha(
            alpha_id="CLASS_REL_TEST".ljust(ALPHA_ID_LENGTH),
            type=AlphaType.REGULAR,
            author="class_tester",
            name="分类关联测试",
            settings_id=test_setting.id,
            regular_id=test_regular.id,
            date_created=datetime.now(),
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.IS,
            status=Status.UNSUBMITTED,
        )
        alphas_session.add(alpha)
        await alphas_session.flush()  # 先提交 Alpha 获取 ID

        # 使用 ORM 关系建立关联
        alpha.classifications.append(classification)
        await alphas_session.flush()

        # 执行测试 - 使用特定方法查询
        results = await alpha_dal.find_by_classification(
            classification.classification_id
        )

        # 验证查询结果
        assert len(results) >= 1
        found_alpha = next((a for a in results if a.id == alpha.id), None)
        assert found_alpha is not None
        assert classification in found_alpha.classifications

    async def test_find_by_competition(
        self,
        alphas_session: AsyncSession,
        test_setting: Setting,
        test_regular: Regular,
    ) -> None:
        """测试 AlphaDAL 的 find_by_competition 方法。

        验证 find_by_competition 方法能否查询参与特定比赛的所有 Alpha。

        Args:
            alphas_session: 数据库会话对象。
            test_setting: 测试用的 Setting 对象。
            test_regular: 测试用的 Regular 对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)
        competition_dal = CompetitionDAL(alphas_session)

        # 创建测试数据 - 比赛
        competition = Competition(
            competition_id="TEST_FIND_COMP",
            name="测试比赛查询",
            status=CompetitionStatus.ACCEPTED,
            team_based=False,
            scoring=CompetitionScoring.CHALLENGE,
            prize_board=False,
            university_board=False,
            submissions=True,
        )
        competition_dal.session.add(competition)
        await competition_dal.session.flush()

        # 创建测试数据 - Alpha
        alpha = Alpha(
            alpha_id="COMP_REL_TEST".ljust(ALPHA_ID_LENGTH),
            type=AlphaType.REGULAR,
            author="comp_tester",
            name="比赛关联测试",
            settings_id=test_setting.id,
            regular_id=test_regular.id,
            date_created=datetime.now(),
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.IS,
            status=Status.UNSUBMITTED,
        )
        alphas_session.add(alpha)
        await alphas_session.flush()  # 先提交 Alpha 获取 ID

        # 使用 ORM 关系建立关联
        alpha.competitions.append(competition)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await alpha_dal.find_by_competition(competition.competition_id)

        # 验证查询结果
        assert len(results) >= 1
        found_alpha = next((a for a in results if a.id == alpha.id), None)
        assert found_alpha is not None
        assert competition in found_alpha.competitions


class TestRegularDAL:
    """测试 RegularDAL 类的功能。"""

    async def test_find_similar_code(self, alphas_session: AsyncSession) -> None:
        """测试 RegularDAL 的 find_similar_code 方法。

        验证 find_similar_code 方法是否能够正确查询包含特定代码片段的所有规则。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        regular_dal = RegularDAL(alphas_session)

        # 创建测试数据
        regulars = [
            Regular(
                code="x = mavg(close, 5)",
                description="5日均线",
                operator_count=4,
            ),
            Regular(
                code="x = mavg(open, 10)",
                description="10日开盘均线",
                operator_count=4,
            ),
            Regular(
                code="x = sum(volume, 5)",
                description="5日成交量",
                operator_count=3,
            ),
        ]
        alphas_session.add_all(regulars)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await regular_dal.find_similar_code("mavg")

        # 验证查询结果
        assert len(results) >= 2
        assert all("mavg" in r.code for r in results)


class TestClassificationDAL:
    """测试 ClassificationDAL 类的各项功能。"""

    async def test_find_by_classification_id(
        self, alphas_session: AsyncSession
    ) -> None:
        """测试通过 classification_id 查询分类的方法。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        classification_dal = ClassificationDAL(alphas_session)

        # 创建测试数据
        classification = Classification(
            classification_id="TEST_CLASS_ID", name="测试分类"
        )
        alphas_session.add(classification)
        await alphas_session.flush()

        # 使用特定方法查询
        result = await classification_dal.find_by_classification_id("TEST_CLASS_ID")

        # 验证查询结果
        assert result is not None
        assert result.classification_id == "TEST_CLASS_ID"
        assert result.name == "测试分类"

    async def test_basic_crud_operations(self, alphas_session: AsyncSession) -> None:
        """测试 ClassificationDAL 的基本 CRUD 操作。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        classification_dal = ClassificationDAL(alphas_session)

        # 测试创建
        classification = await classification_dal.create_entity(
            classification_id="CRUD_TEST",
            name="CRUD测试分类",
        )
        assert classification.id is not None
        assert classification.classification_id == "CRUD_TEST"

        # 测试更新
        updated = await classification_dal.update_by_id(
            classification.id, name="更新后的分类"
        )
        assert updated is not None
        assert updated.name == "更新后的分类"
        assert updated.classification_id == "CRUD_TEST"  # 未修改的字段保持不变

        # 测试删除
        result = await classification_dal.delete_by_id(classification.id)
        assert result is True

        # 验证删除结果
        assert await classification_dal.find_by_classification_id("CRUD_TEST") is None


class TestCompetitionDAL:
    """测试 CompetitionDAL 类的各项功能。"""

    async def test_find_by_competition_id(self, alphas_session: AsyncSession) -> None:
        """测试通过 competition_id 查询比赛的方法。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        competition_dal = CompetitionDAL(alphas_session)

        # 创建测试数据
        competition = Competition(
            competition_id="TEST_COMP_ID",
            name="测试比赛",
            status=CompetitionStatus.ACCEPTED,  # 添加必填字段
            team_based=False,  # 添加必填字段
            scoring=CompetitionScoring.CHALLENGE,  # 添加必填字段
            prize_board=False,  # 添加必填字段
            university_board=False,  # 添加必填字段
            submissions=True,  # 添加必填字段
        )
        alphas_session.add(competition)
        await alphas_session.flush()

        # 使用特定方法查询
        result = await competition_dal.find_by_competition_id("TEST_COMP_ID")

        # 验证查询结果
        assert result is not None
        assert result.competition_id == "TEST_COMP_ID"
        assert result.name == "测试比赛"

    async def test_basic_crud_operations(self, alphas_session: AsyncSession) -> None:
        """测试 CompetitionDAL 的基本 CRUD 操作。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        competition_dal = CompetitionDAL(alphas_session)

        # 测试创建
        competition = await competition_dal.create_entity(
            competition_id="COMP_CRUD_TEST",
            name="CRUD测试比赛",
            status=CompetitionStatus.ACCEPTED,  # 添加必填字段
            team_based=False,  # 添加必填字段
            scoring=CompetitionScoring.CHALLENGE,  # 添加必填字段
            prize_board=False,  # 添加必填字段
            university_board=False,  # 添加必填字段
            submissions=True,  # 添加必填字段
        )
        assert competition.id is not None
        assert competition.competition_id == "COMP_CRUD_TEST"

        # 测试更新
        updated = await competition_dal.update_by_id(
            competition.id, name="更新后的比赛"
        )
        assert updated is not None
        assert updated.name == "更新后的比赛"

        # 测试删除
        result = await competition_dal.delete_by_id(competition.id)
        assert result is True

        # 验证删除结果
        assert await competition_dal.find_by_competition_id("COMP_CRUD_TEST") is None


class TestSampleDAL:
    """测试 SampleDAL 类的各项功能。"""

    async def test_find_by_performance(self, alphas_session: AsyncSession) -> None:
        """测试通过性能指标查询样本的方法。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        sample_dal = SampleDAL(alphas_session)

        # 创建测试数据
        samples = [
            Sample(
                sharpe=0.5 + i * 0.5,  # 1.0, 1.5, 2.0
                start_date=datetime.now(),  # 添加必填字段
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(samples)
        await alphas_session.flush()

        # 使用特定方法查询
        results = await sample_dal.find_by_performance(1.2)

        # 验证查询结果
        assert len(results) >= 2  # 应该至少有两个样本的 sharpe > 1.2
        assert all(s.sharpe and s.sharpe >= 1.2 for s in results)

    async def test_basic_crud_operations(self, alphas_session: AsyncSession) -> None:
        """测试 SampleDAL 的基本 CRUD 操作。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        sample_dal = SampleDAL(alphas_session)

        # 测试创建
        sample = await sample_dal.create_entity(
            sharpe=1.5,
            start_date=datetime.now(),  # 添加必填字段
        )
        assert sample.id is not None

        # 测试更新
        updated = await sample_dal.update_by_id(sample.id, sharpe=2.0, drawdown=0.1)
        assert updated is not None
        assert updated.sharpe == 2.0

        # 测试删除
        result = await sample_dal.delete_by_id(sample.id)
        assert result is True

        # 验证删除结果
        assert await sample_dal.get_by_id(sample.id) is None


class TestSampleCheckDAL:
    """测试 SampleCheckDAL 类的各项功能。"""

    async def test_basic_crud_operations(
        self, alphas_session: AsyncSession, test_sample: Sample
    ) -> None:
        """测试 SampleCheckDAL 的基本 CRUD 操作。

        Args:
            alphas_session: 数据库会话对象。
            test_sample: 测试用的 Sample 对象。
        """
        # 创建 DAL 实例
        sample_check_dal = SampleCheckDAL(alphas_session)

        # 测试创建
        sample_check = await sample_check_dal.create_entity(
            sample_id=test_sample.id,  # 添加必填字段
            name="测试检查",
            result="通过",
        )
        assert sample_check.id is not None
        assert sample_check.sample_id == test_sample.id  # 验证外键

        # 测试更新
        updated = await sample_check_dal.update_by_id(
            sample_check.id, result="不通过", message="测试信息"
        )
        assert updated is not None
        assert updated.result == "不通过"

        # 测试删除
        result = await sample_check_dal.delete_by_id(sample_check.id)
        assert result is True

        # 验证删除结果
        assert await sample_check_dal.get_by_id(sample_check.id) is None
