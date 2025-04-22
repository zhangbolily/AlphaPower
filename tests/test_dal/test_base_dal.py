"""测试 alphapower.entity.dal 模块中的基础数据访问层功能。

本模块包含一系列测试，用于验证 alphapower.entity.dal 模块中定义的基础数据访问层
类是否正确实现。测试使用真实数据库连接进行，验证基础数据访问操作的正确性。

测试覆盖以下数据访问层功能:
- BaseDAL: 基础数据访问层类
- DALFactory: DAL工厂类
- DALRegistry: DAL注册中心
"""

from datetime import datetime
from typing import AsyncGenerator, Optional

import pytest
from sqlalchemy import Result, select
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.constants import (
    AlphaType,
    Color,
    Database,
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
from alphapower.dal.alphas import AlphaDAL, RegularDAL, SettingDAL
from alphapower.dal.base import (
    BaseDAL,
    DALFactory,
)
from alphapower.entity import Alpha, Expression, Setting
from alphapower.internal.db_session import get_db_session


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


@pytest.fixture(name="dal")
async def fixture_dal(alphas_session: AsyncSession) -> BaseDAL:
    """创建 BaseDAL 实例用于测试。

    Args:
        alphas_session: 数据库会话对象。

    Returns:
        BaseDAL: 初始化好的数据访问层实例。
    """
    return BaseDAL(Setting, alphas_session)


class TestBaseDAL:
    """测试 BaseDAL 基础类的各项功能。"""

    def test_init_dal(self, alphas_session: AsyncSession) -> None:
        """测试 BaseDAL 的初始化。

        验证 BaseDAL 实例是否能够正确初始化，并持有会话对象。

        Args:
            alphas_session: 数据库会话对象。
        """
        dal = BaseDAL(Setting, alphas_session)
        assert dal.session is alphas_session, "DAL 应该持有传入的会话对象"
        assert dal.entity_type is Setting, "DAL 应该持有实体类型"

    async def test_get_by_id(self, dal: BaseDAL) -> None:
        """测试通过 ID 获取实体的方法。

        验证 get_by_id 方法是否能够正确根据 ID 获取实体。

        Args:
            dal: 数据访问层实例。
        """
        # 创建测试数据
        setting: Setting = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            decay=5,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        dal.session.add(setting)
        await dal.session.flush()

        # 通过 ID 获取实体
        retrieved_setting = await dal.get_by_id(setting.id)

        # 验证结果
        assert retrieved_setting is not None
        assert retrieved_setting.id == setting.id
        assert retrieved_setting.instrument_type == InstrumentType.EQUITY
        assert retrieved_setting.region == Region.CHN

        # 测试获取不存在的 ID
        non_existent = await dal.get_by_id(-1)
        assert non_existent is None

    async def test_get_all(self, alphas_session: AsyncSession) -> None:
        """测试获取所有实体的方法。

        验证 get_all 方法是否能够正确获取指定类型的所有实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建测试数据
        regular_dal = RegularDAL(alphas_session)
        regular1: Expression = Expression(
            code="x = close(0)", description="收盘价", operator_count=1
        )
        regular2: Expression = Expression(
            code="x = open(0)", description="开盘价", operator_count=1
        )
        alphas_session.add_all([regular1, regular2])
        await alphas_session.flush()

        # 获取所有实体
        regulars = await regular_dal.get_all()

        # 验证结果
        assert len(regulars) >= 2  # 可能有其他测试创建的数据
        assert any(r.code == "x = close(0)" for r in regulars)
        assert any(r.code == "x = open(0)" for r in regulars)

    async def test_create_entity(self, alphas_session: AsyncSession) -> None:
        """测试创建实体的方法。

        验证 create_entity 方法是否能够正确创建实体并返回。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        setting_dal = SettingDAL(alphas_session)

        # 创建实体
        setting = await setting_dal.create_entity(
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOPSP500,
            delay=Delay.ONE,
            decay=7,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )

        # 验证创建结果
        assert setting.id is not None
        assert setting.instrument_type == InstrumentType.EQUITY
        assert setting.region == Region.USA
        assert setting.universe == Universe.TOPSP500
        assert setting.delay == Delay.ONE
        assert setting.decay == 7

        # 通过查询验证创建是否成功
        result: Result = await alphas_session.execute(
            select(Setting).where(Setting.id == setting.id)
        )
        db_setting: Optional[Setting] = result.scalars().first()

        assert db_setting is not None
        assert db_setting.id == setting.id
        assert db_setting.instrument_type == InstrumentType.EQUITY

    async def test_update(self, alphas_session: AsyncSession) -> None:
        """测试更新实体的方法。

        验证 update 方法是否能够正确更新实体并保存更改。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        setting_dal = SettingDAL(alphas_session)

        # 创建测试数据
        setting: Setting = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            decay=5,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        alphas_session.add(setting)
        await alphas_session.flush()

        # 执行更新
        updated_setting = await setting_dal.update_by_id(
            setting.id,
            universe=Universe.TOP1000,
            delay=Delay.ZERO,
            visualization=True,
        )

        # 验证更新结果
        assert updated_setting is not None
        assert updated_setting.id == setting.id
        assert updated_setting.universe == Universe.TOP1000
        assert updated_setting.delay == Delay.ZERO
        assert updated_setting.region == Region.CHN  # 未修改字段保持不变

        # 通过新查询验证更新是否成功
        result: Result = await alphas_session.execute(
            select(Setting).where(Setting.id == setting.id)
        )
        db_setting: Optional[Setting] = result.scalars().first()

        assert db_setting is not None
        assert db_setting.universe == Universe.TOP1000
        assert db_setting.delay == Delay.ZERO

    async def test_delete(self, alphas_session: AsyncSession) -> None:
        """测试删除实体的方法。

        验证 delete 方法是否能够正确删除实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        regular_dal = RegularDAL(alphas_session)

        # 创建测试数据
        regular: Expression = Expression(
            code="x = (high(0) + low(0)) / 2",
            description="中间价",
            operator_count=5,
        )
        alphas_session.add(regular)
        await alphas_session.flush()

        # 记录 ID
        regular_id = regular.id

        # 执行删除
        result = await regular_dal.delete_by_id(regular_id)
        assert result is True

        # 验证删除结果
        db_regular = await regular_dal.get_by_id(regular_id)
        assert db_regular is None

        # 测试删除不存在的实体
        result = await regular_dal.delete_by_id(-1)
        assert result is False

    async def test_find_by(self, alphas_session: AsyncSession) -> None:
        """测试按条件查找实体的方法。

        验证 find_by 方法是否能够根据提供的过滤条件正确查询实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        setting_dal = SettingDAL(alphas_session)

        # 创建测试数据
        setting1 = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.CHN,
            universe=Universe.TOP2000U,
            delay=Delay.ONE,
            decay=5,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        setting2 = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOPSP500,
            delay=Delay.ONE,
            decay=5,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        setting3 = Setting(
            instrument_type=InstrumentType.CRYPTO,
            region=Region.GLB,
            universe=Universe.TOP50,
            delay=Delay.ONE,
            decay=5,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        alphas_session.add_all([setting1, setting2, setting3])
        await alphas_session.flush()

        # 测试单一过滤条件
        results1 = await setting_dal.find_by(region=Region.CHN)
        assert len(results1) >= 1
        assert all(s.region == Region.CHN for s in results1)

        # 测试多个过滤条件
        results2 = await setting_dal.find_by(
            instrument_type=InstrumentType.EQUITY, region=Region.USA
        )
        assert len(results2) >= 1
        assert all(
            s.instrument_type == InstrumentType.EQUITY and s.region == Region.USA
            for s in results2
        )

        # 测试没有匹配的过滤条件
        results3 = await setting_dal.find_by(region=Region.EUR)
        assert len(results3) == 0

    async def test_find_one_by(self, alphas_session: AsyncSession) -> None:
        """测试按条件查找单个实体的方法。

        验证 find_one_by 方法是否能够根据指定条件查询单个实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        alpha1: Alpha = Alpha(
            alpha_id="TEST001",
            type=AlphaType.REGULAR,
            author="user1",
            name="测试Alpha1",
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.DEFAULT,
            status=Status.DEFAULT,
            date_created=datetime.now(),
            settings_id=1,
            regular_id=1,
        )
        alpha2: Alpha = Alpha(
            alpha_id="TEST002",
            type=AlphaType.REGULAR,
            author="user2",
            name="测试Alpha2",
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.DEFAULT,
            status=Status.DEFAULT,
            date_created=datetime.now(),
            settings_id=1,
            regular_id=1,
        )
        alphas_session.add_all([alpha1, alpha2])
        await alphas_session.flush()

        # 通过条件获取实体
        result1 = await alpha_dal.find_one_by(alpha_id="TEST001")
        assert result1 is not None
        assert result1.alpha_id == "TEST001"
        assert result1.name == "测试Alpha1"

        # 测试查询不存在的值
        result2 = await alpha_dal.find_one_by(alpha_id="NOT_EXIST")
        assert result2 is None

    async def test_update_by_query(self, alphas_session: AsyncSession) -> None:
        """测试通过查询条件批量更新实体的方法。

        验证 update_by_query 方法是否能够正确批量更新实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        regular_dal = RegularDAL(alphas_session)

        # 创建测试数据
        regulars = [
            Expression(
                code=f"batch_code_{i}", description="批量更新测试", operator_count=i
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(regulars)
        await alphas_session.flush()

        # 执行批量更新
        filter_kwargs = {"description": "批量更新测试"}
        update_kwargs = {"description": "已更新", "operator_count": 10}
        updated_count = await regular_dal.update_by_filter(filter_kwargs, update_kwargs)

        # 验证更新结果
        assert updated_count >= 3

        # 查询更新后的实体
        updated_regulars = await regular_dal.find_by(description="已更新")
        assert len(updated_regulars) >= 3
        assert all(r.operator_count == 10 for r in updated_regulars)

    async def test_delete_by(self, alphas_session: AsyncSession) -> None:
        """测试按条件删除实体的方法。

        验证 delete_by 方法是否能够正确按条件删除实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        regular_dal = RegularDAL(alphas_session)

        # 创建测试数据
        regulars = [
            Expression(
                code=f"delete_test_{i}", description="批量删除测试", operator_count=i
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(regulars)
        await alphas_session.flush()

        # 执行批量删除
        deleted_count = await regular_dal.delete_by_filter(description="批量删除测试")
        assert deleted_count >= 3

        # 验证删除结果
        remaining_regulars = await regular_dal.find_by(description="批量删除测试")
        assert len(remaining_regulars) == 0

    async def test_count(self, alphas_session: AsyncSession) -> None:
        """测试计数方法。

        验证 count 方法是否能够正确计算实体数量。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        regular_dal = RegularDAL(alphas_session)

        # 创建测试数据
        regulars = [
            Expression(
                code=f"count_test_{i}", description=f"测试描述_{i}", operator_count=i
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(regulars)
        await alphas_session.flush()

        # 使用过滤条件计数
        filtered_count = await regular_dal.count(code="count_test_1")
        assert filtered_count == 1

    async def test_query_and_execute(self, alphas_session: AsyncSession) -> None:
        """测试查询构建和执行方法。

        验证 query 和 execute_query 方法是否能够正确构建和执行查询。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        alphas = [
            Alpha(
                alpha_id=f"QUERY_TEST_{i}",
                type=AlphaType.REGULAR,
                author=f"author_{i}",
                name=f"查询测试_{i}",
                favorite=False,
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.DEFAULT,
                status=Status.DEFAULT,
                date_created=datetime.now(),
                settings_id=1,
                regular_id=1,
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(alphas)
        await alphas_session.flush()

        # 构建并执行查询
        query = alpha_dal.query().where(Alpha.type == AlphaType.REGULAR)
        results = await alpha_dal.execute_query(query)

        # 验证查询结果
        assert len(results) >= 3
        assert all(a.type == AlphaType.REGULAR for a in results)
        assert any(a.alpha_id.startswith("QUERY_TEST_") for a in results)

    async def test_execute_stream_query(self, alphas_session: AsyncSession) -> None:
        """测试流式查询执行方法。

        验证 execute_stream_query 方法是否能够正确执行查询并返回异步生成器。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(alphas_session)

        # 创建测试数据
        stream_alphas = [
            Alpha(
                alpha_id=f"STREAM_TEST_{i}",
                type=AlphaType.REGULAR,
                author=f"stream_author_{i}",
                name=f"流式查询测试_{i}",
                favorite=False,
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.DEFAULT,
                status=Status.DEFAULT,
                date_created=datetime.now(),
                settings_id=1,
                regular_id=1,
            )
            for i in range(1, 4)
        ]
        alphas_session.add_all(stream_alphas)
        await alphas_session.flush()

        # 构建查询
        query = alpha_dal.query().where(Alpha.author.like("stream_author_%"))

        # 执行流式查询并验证结果
        count = 0
        streamed_ids = set()
        async for alpha in alpha_dal.execute_stream_query(query):
            assert isinstance(alpha, Alpha)
            assert alpha.author.startswith("stream_author_")
            assert alpha.alpha_id.startswith("STREAM_TEST_")
            streamed_ids.add(alpha.alpha_id)
            count += 1

        # 验证数量和内容
        assert count == 3
        assert streamed_ids == {"STREAM_TEST_1", "STREAM_TEST_2", "STREAM_TEST_3"}

    async def test_upsert(self, alphas_session: AsyncSession) -> None:
        """测试 upsert 方法。

        验证 upsert 方法是否能够正确插入或更新实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        setting_dal = SettingDAL(alphas_session)

        # 插入新实体
        setting = Setting(
            instrument_type=InstrumentType.EQUITY,
            region=Region.USA,
            universe=Universe.TOPSP500,
            delay=Delay.ONE,
            decay=7,
            truncation=0.5,
            visualization=True,
            neutralization=Neutralization.SECTOR,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,
            nan_handling=Switch.OFF,
        )
        upserted_setting = await setting_dal.upsert(setting)
        assert upserted_setting.id is not None

        # 更新已有实体
        upserted_setting.decay = 10
        updated_setting = await setting_dal.upsert(upserted_setting)
        assert updated_setting.decay == 10

    async def test_upsert_by_unique_key(self, alphas_session: AsyncSession) -> None:
        """测试 upsert_by_unique_key 方法。

        验证 upsert_by_unique_key 方法是否能够根据唯一键插入或更新实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        alpha_dal = AlphaDAL(alphas_session)

        # 插入新实体
        alpha = Alpha(
            alpha_id="UNIQUE001",
            type=AlphaType.REGULAR,
            author="user1",
            name="测试Alpha",
            favorite=False,
            hidden=False,
            color=Color.NONE,
            grade=Grade.DEFAULT,
            stage=Stage.DEFAULT,
            status=Status.DEFAULT,
            date_created=datetime.now(),
            settings_id=1,
            regular_id=1,
        )
        upserted_alpha = await alpha_dal.upsert_by_unique_key(alpha, "alpha_id")
        assert upserted_alpha.id is not None

        # 更新已有实体
        upserted_alpha.name = "更新后的Alpha"
        updated_alpha = await alpha_dal.upsert_by_unique_key(upserted_alpha, "alpha_id")
        assert updated_alpha.name == "更新后的Alpha"

    async def test_bulk_upsert(self, alphas_session: AsyncSession) -> None:
        """测试 bulk_upsert 方法。

        验证 bulk_upsert 方法是否能够批量插入或更新实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        setting_dal = SettingDAL(alphas_session)

        # 插入新实体
        settings = [
            Setting(
                instrument_type=InstrumentType.EQUITY,
                region=Region.USA,
                universe=Universe.TOPSP500,
                delay=Delay.ONE,
                decay=i,
                truncation=0.5,
                visualization=True,
                neutralization=Neutralization.SECTOR,
                pasteurization=Switch.ON,
                unit_handling=UnitHandling.VERIFY,
                nan_handling=Switch.OFF,
            )
            for i in range(1, 4)
        ]
        upserted_settings = await setting_dal.bulk_upsert(settings)
        assert len(upserted_settings) == 3

        # 更新已有实体
        for setting in upserted_settings:
            setting.decay = 10
        updated_settings = await setting_dal.bulk_upsert(upserted_settings)
        assert all(s.decay == 10 for s in updated_settings)

    async def test_bulk_upsert_by_unique_key(
        self, alphas_session: AsyncSession
    ) -> None:
        """测试 bulk_upsert_by_unique_key 方法。

        验证 bulk_upsert_by_unique_key 方法是否能够根据唯一键批量插入或更新实体。

        Args:
            alphas_session: 数据库会话对象。
        """
        alpha_dal = AlphaDAL(alphas_session)

        # 插入新实体
        alphas = [
            Alpha(
                alpha_id=f"UNIQUE_BULK_{i}",
                type=AlphaType.REGULAR,
                author=f"user{i}",
                name=f"测试Alpha{i}",
                favorite=False,
                hidden=False,
                color=Color.NONE,
                grade=Grade.DEFAULT,
                stage=Stage.DEFAULT,
                status=Status.DEFAULT,
                date_created=datetime.now(),
                settings_id=1,
                regular_id=1,
            )
            for i in range(1, 4)
        ]
        upserted_alphas = await alpha_dal.bulk_upsert_by_unique_key(alphas, "alpha_id")
        assert len(upserted_alphas) == 3

        # 更新已有实体
        for alpha in upserted_alphas:
            alpha.name = "批量更新后的Alpha"
        updated_alphas = await alpha_dal.bulk_upsert_by_unique_key(
            upserted_alphas, "alpha_id"
        )
        assert all(a.name == "批量更新后的Alpha" for a in updated_alphas)


class TestDALFactory:
    """测试 DALFactory 类的功能。"""

    def test_create_dal(self, alphas_session: AsyncSession) -> None:
        """测试 DALFactory.create_dal 方法。

        验证 create_dal 方法是否能够正确创建 DAL 实例。

        Args:
            alphas_session: 数据库会话对象。
        """
        # 创建 AlphaDAL 实例
        alpha_dal = DALFactory.create_dal(AlphaDAL, alphas_session)
        assert isinstance(alpha_dal, AlphaDAL)
        assert alpha_dal.session is alphas_session
        assert alpha_dal.entity_type is Alpha

        # 创建 RegularDAL 实例
        regular_dal = DALFactory.create_dal(RegularDAL, alphas_session)
        assert isinstance(regular_dal, RegularDAL)
        assert regular_dal.session is alphas_session
        assert regular_dal.entity_type is Expression
