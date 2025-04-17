"""
模块名称: test_evaluate_base_evaluator

模块功能:
    为 BaseEvaluator 类提供单元测试。
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Generator, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import TypeAdapter
from sqlalchemy import Select

from alphapower.client import TableSchemaView  # 导入 TableSchemaView
from alphapower.client import (
    BeforeAndAfterPerformanceView,
    CompetitionRefView,
    TableView,
    WorldQuantClient,
)
from alphapower.client.checks_view import StatsView  # 导入 StatsView
from alphapower.constants import (
    ALPHA_ID_LENGTH,
    AlphaCheckType,
    AlphaType,
    Color,
    CompetitionScoring,
    CompetitionStatus,
    CorrelationCalcType,
    CorrelationType,
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
    UserRole,
)
from alphapower.dal.base import DALFactory
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.engine.evaluate.original_base_evaluator import BaseEvaluator
from alphapower.entity import (
    Alpha,
    Check,
    CheckRecord,
    Competition,
    Correlation,
    Regular,
    Sample,
    Setting,
)

# pylint: disable=W0621, R0913, C0301, W0613


@pytest.fixture
def mock_wq_client() -> MagicMock:
    """提供一个 WorldQuantClient 的模拟对象。"""
    client = MagicMock(spec=WorldQuantClient)
    client.alpha_correlation_check = AsyncMock()
    client.alpha_fetch_before_and_after_performance = AsyncMock()
    # 模拟异步上下文管理器
    client.__aenter__ = AsyncMock(return_value=client)

    # 修改 __aexit__ 以正确处理异常
    # 它接收 exc_type, exc_val, exc_tb 三个参数
    # 如果 exc_type 不为 None，表示上下文中发生了异常
    # 默认行为是不处理异常（返回 None 或 False），让异常继续传播
    # 如果需要模拟抑制异常，可以让它返回 True
    async def mock_aexit(exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # 默认不抑制异常，让其传播
        pass

    client.__aexit__ = AsyncMock(side_effect=mock_aexit)
    return client


@pytest.fixture
def mock_db_session() -> Generator[dict[str, AsyncMock], None, None]:
    """提供一个数据库会话和 DAL 的模拟对象。"""
    session = AsyncMock()
    session.stream_scalars = AsyncMock()
    session.add = AsyncMock()
    session.merge = AsyncMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()  # 添加 commit 的模拟

    mock_check_record_dal = MagicMock(spec=CheckRecordDAL)
    mock_check_record_dal.create = AsyncMock()

    mock_correlation_dal = MagicMock(spec=CorrelationDAL)
    mock_correlation_dal.bulk_upsert = AsyncMock()

    # 模拟 DALFactory.create_dal
    def create_dal_side_effect(session: AsyncMock, dal_class: type) -> MagicMock:
        if dal_class == CheckRecordDAL:
            return mock_check_record_dal
        if dal_class == CorrelationDAL:
            return mock_correlation_dal
        raise TypeError(f"未知的 DAL 类型: {dal_class}")

    with patch(
        "alphapower.engine.evaluate.base_evaluator.get_db_session"
    ) as mock_get_session:
        mock_get_session.return_value.__aenter__.return_value = session
        with patch(
            "alphapower.engine.evaluate.base_evaluator.DALFactory", spec=DALFactory
        ) as mock_dal_factory:
            mock_dal_factory.create_dal.side_effect = create_dal_side_effect
            yield {
                "session": session,
                "check_record_dal": mock_check_record_dal,
                "correlation_dal": mock_correlation_dal,
                "mock_dal_factory": mock_dal_factory,
                "mock_get_session": mock_get_session,
            }


@pytest.fixture
def sample_setting() -> Setting:
    """创建一个示例 Setting 对象。"""
    return Setting(
        id=1,
        instrument_type=InstrumentType.EQUITY,
        region=Region.USA,
        universe=Universe.TOP3000,
        delay=Delay.ONE,  # 修复: 使用正确的枚举值 Delay.ONE
        decay=4,
        neutralization=Neutralization.MARKET,
        truncation=0.01,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修复: 使用正确的枚举值 UnitHandling.VERIFY
        nan_handling=Switch.OFF,
        language=RegularLanguage.PYTHON,
        visualization=False,
        test_period="2010-2020",
        max_trade=Switch.ON,
    )


@pytest.fixture
def sample_regular() -> Regular:
    """创建一个示例 Regular 对象。"""
    return Regular(
        id=1,
        code="ts_mean(close, 10)",
        description="10日收盘价均值",
        operator_count=2,
    )


@pytest.fixture
def sample_competition() -> Competition:
    """创建一个示例 Competition 对象。"""
    return Competition(
        id=1,
        competition_id="COMP001",
        name="全球Alpha竞赛",
        description="一个测试竞赛",
        universities=["MIT", "Stanford"],
        countries=["USA", "CAN"],
        excluded_countries=["CHN"],
        status=CompetitionStatus.ACCEPTED,  # 修复: 使用正确的枚举值 CompetitionStatus.ACCEPTED
        team_based=False,
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 12, 31),
        sign_up_start_date=datetime(2022, 11, 1),
        sign_up_end_date=datetime(2022, 12, 31),
        scoring=CompetitionScoring.PERFORMANCE,  # 修复: 使用正确的枚举值 CompetitionScoring.PERFORMANCE
        prize_board=True,
        university_board=True,
        submissions=True,
        faq="http://example.com/faq",
    )


@pytest.fixture
def sample_check_matches(sample_competition: Competition) -> Check:
    """创建一个示例 Check 对象 (MATCHES_COMPETITION)。"""
    competitions_list = [
        CompetitionRefView(
            id=sample_competition.competition_id, name=sample_competition.name
        )
    ]
    competitions_adapter: TypeAdapter[List[CompetitionRefView]] = TypeAdapter(
        List[CompetitionRefView]
    )
    competitions_json = competitions_adapter.dump_json(competitions_list).decode(
        "utf-8"
    )
    return Check(
        id=1,
        sample_id=1,  # 假设关联的 Sample ID 为 1
        name=AlphaCheckType.MATCHES_COMPETITION.value,
        result="PASS",
        competitions=competitions_json,
    )


@pytest.fixture
def sample_check_other() -> Check:
    """创建一个示例 Check 对象 (非 MATCHES_COMPETITION)。"""
    return Check(
        id=2,
        sample_id=1,
        name="SOME_OTHER_CHECK",
        result="PASS",
        value=0.5,
        limit=0.8,
    )


@pytest.fixture
def sample_sample(sample_check_matches: Check, sample_check_other: Check) -> Sample:
    """创建一个示例 Sample 对象。"""
    return Sample(
        id=1,
        long_count=100,
        short_count=50,
        pnl=10000.0,
        book_size=1000000.0,
        turnover=0.15,
        returns=0.1,
        drawdown=0.05,
        margin=0.2,
        sharpe=2.1,
        fitness=1.6,
        self_correration=0.2,
        prod_correration=0.3,
        os_is_sharpe_ratio=1.1,
        pre_close_sharpe_ratio=2.0,
        start_date=datetime(2022, 1, 1),
        checks=[sample_check_matches, sample_check_other],
    )


@pytest.fixture
def sample_alpha(
    sample_setting: Setting,
    sample_regular: Regular,
    sample_sample: Sample,
    sample_competition: Competition,
) -> Alpha:
    """创建一个示例 Alpha 对象。"""
    # 确保 alpha_id 长度符合要求
    alpha_id = "TESTALP"[:ALPHA_ID_LENGTH]  # 修复: 确保 alpha_id 长度正确
    return Alpha(
        id=1,
        alpha_id=alpha_id,
        author="test_user",
        settings_id=sample_setting.id,
        settings=sample_setting,
        regular_id=sample_regular.id,
        regular=sample_regular,
        date_created=datetime.now(),
        name="测试Alpha",
        favorite=False,
        hidden=False,
        color=Color.BLUE,
        category="PRICE_MOMENTUM",  # 修复: 使用 Category 枚举值或有效字符串
        tags=["高频", "美股"],
        grade=Grade.GOOD,  # 修复: 使用正确的枚举值 Grade.GOOD
        stage=Stage.IS,  # 修复: 使用正确的枚举值 Stage.IS
        status=Status.ACTIVE,
        type=AlphaType.REGULAR,
        in_sample_id=sample_sample.id,
        in_sample=sample_sample,
        # 关联其他 Sample 和 Competition
        out_sample_id=None,
        train_id=None,
        test_id=None,
        prod_id=None,
        classifications=[],
        competitions=[sample_competition],
    )


@pytest.fixture
def base_evaluator(sample_alpha: Alpha) -> BaseEvaluator:
    """创建一个 BaseEvaluator 实例。"""
    return BaseEvaluator(alpha=sample_alpha)


class TestBaseEvaluator:
    """BaseEvaluator 的测试类。"""

    @pytest.mark.asyncio
    async def test_fetch_alphas_for_evaluation_consultant(
        self, mock_db_session: dict, sample_alpha: Alpha
    ) -> None:
        """测试顾问角色的 Alpha 获取。"""
        mock_session = mock_db_session["session"]
        alphas_to_yield = [sample_alpha]

        # 模拟 stream_scalars 返回异步生成器
        async def async_gen() -> AsyncGenerator[Alpha, None]:
            for alpha in alphas_to_yield:
                yield alpha

        mock_session.stream_scalars.return_value = async_gen()

        # 确保查询对象被正确构建和传递
        # 这里我们不直接验证查询字符串，而是验证 stream_scalars 被调用
        result_alphas = []
        async for alpha in BaseEvaluator.fetch_alphas_for_evaluation(
            role=UserRole.CONSULTANT,
            alpha_type=AlphaType.REGULAR,
            start_time=datetime.now() - timedelta(days=1),
            end_time=datetime.now(),
        ):
            result_alphas.append(alpha)

        assert result_alphas == alphas_to_yield
        mock_session.stream_scalars.assert_called_once()
        # 检查调用 stream_scalars 时使用的查询是否是 Select 对象
        call_args, _ = mock_session.stream_scalars.call_args
        assert isinstance(call_args[0], Select)

    @pytest.mark.asyncio
    async def test_fetch_alphas_for_evaluation_user_not_implemented(self) -> None:
        """测试用户角色的 Alpha 获取是否引发 NotImplementedError。"""
        with pytest.raises(NotImplementedError):
            async for _ in BaseEvaluator.fetch_alphas_for_evaluation(
                role=UserRole.USER,
                alpha_type=AlphaType.REGULAR,
                start_time=datetime.now() - timedelta(days=1),
                end_time=datetime.now(),
            ):
                pass  # pragma: no cover

    @pytest.mark.asyncio
    async def test_fetch_alphas_for_evaluation_no_query(self) -> None:
        """测试顾问查询未初始化时是否引发 ValueError。"""
        original_query = BaseEvaluator.consultant_alpha_select_query
        BaseEvaluator.consultant_alpha_select_query = None  # 强制设为 None
        with pytest.raises(ValueError, match="顾问因子筛选查询未初始化"):
            async for _ in BaseEvaluator.fetch_alphas_for_evaluation(
                role=UserRole.CONSULTANT,
                alpha_type=AlphaType.REGULAR,
                start_time=datetime.now() - timedelta(days=1),
                end_time=datetime.now(),
            ):
                pass  # pragma: no cover
        BaseEvaluator.consultant_alpha_select_query = original_query  # 恢复

    def test_init(self, sample_alpha: Alpha) -> None:
        """测试 BaseEvaluator 初始化。"""
        evaluator = BaseEvaluator(alpha=sample_alpha)
        assert evaluator._alpha == sample_alpha

    @pytest.mark.asyncio
    async def test_matched_competitions_found(
        self, base_evaluator: BaseEvaluator, sample_competition: Competition
    ) -> None:
        """测试找到匹配竞赛的情况。"""
        competitions = await base_evaluator.matched_competitions()
        assert len(competitions) == 1
        assert competitions[0].id == sample_competition.competition_id
        assert competitions[0].name == sample_competition.name

    @pytest.mark.asyncio
    async def test_matched_competitions_not_found(
        self, base_evaluator: BaseEvaluator, sample_sample: Sample
    ) -> None:
        """测试未找到匹配竞赛检查项的情况。"""
        # 移除匹配竞赛的检查项
        sample_sample.checks = [
            c
            for c in sample_sample.checks
            if c.name != AlphaCheckType.MATCHES_COMPETITION.value
        ]
        competitions = await base_evaluator.matched_competitions()
        assert competitions == []

    @pytest.mark.asyncio
    async def test_matched_competitions_empty(
        self, base_evaluator: BaseEvaluator, sample_sample: Sample
    ) -> None:
        """测试匹配竞赛检查项存在但列表为空的情况。"""
        for check in sample_sample.checks:
            if check.name == AlphaCheckType.MATCHES_COMPETITION.value:
                check.competitions = "[]"  # 设置为空列表 JSON
        competitions = await base_evaluator.matched_competitions()
        assert competitions == []

    @pytest.mark.asyncio
    async def test_matched_competitions_invalid_json(
        self, base_evaluator: BaseEvaluator, sample_sample: Sample
    ) -> None:
        """测试匹配竞赛检查项 JSON 无效的情况。"""
        for check in sample_sample.checks:
            if check.name == AlphaCheckType.MATCHES_COMPETITION.value:
                check.competitions = "invalid json"
        with pytest.raises(ValueError, match="竞赛列表 JSON 无效"):
            await base_evaluator.matched_competitions()

    @pytest.mark.asyncio
    async def test_matched_competitions_no_in_sample(
        self, base_evaluator: BaseEvaluator, sample_alpha: Alpha
    ) -> None:
        """测试 Alpha 缺少 in_sample 数据的情况。"""
        sample_alpha.in_sample = None  # type: ignore
        competitions = await base_evaluator.matched_competitions()
        assert competitions == []

    @pytest.mark.asyncio
    async def test_correlation_check_self_finished(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
    ) -> None:
        """测试自相关性检查立即完成。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        mock_correlation_dal = mock_db_session["correlation_dal"]
        mock_get_session = mock_db_session["mock_get_session"]

        # 模拟 API 返回 - 修复 TableView 结构
        mock_result = TableView(
            table_schema=TableSchemaView(  # 使用 TableSchemaView
                name="correlation_schema",
                title="Correlation Data",
                properties=[
                    TableSchemaView.Property(
                        name="id", title="Alpha ID", data_type="string"
                    ),
                    TableSchemaView.Property(
                        name="correlation", title="Correlation", data_type="number"
                    ),
                ],
            ),
            records=[["ALPHA_1", 0.5], ["ALPHA_2", 0.6]],
            # total_count 不是 TableView 的直接字段，通常在响应的顶层
        )
        mock_wq_client.alpha_correlation_check.return_value = (True, None, mock_result)

        # 使用 patch 模拟 wq_client 上下文管理器
        with patch(
            "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
        ):
            await base_evaluator.correlation_check(CorrelationType.SELF)

        # 验证 API 调用
        mock_wq_client.alpha_correlation_check.assert_called_once_with(
            alpha_id=sample_alpha.alpha_id, corr_type=CorrelationType.SELF
        )
        # 验证数据库操作
        mock_get_session.assert_called()  # 确保调用了 get_db_session
        mock_check_record_dal.create.assert_called_once()
        mock_correlation_dal.bulk_upsert.assert_called_once()

        # 验证 CheckRecord 内容
        created_check_record: CheckRecord = mock_check_record_dal.create.call_args[0][0]
        assert created_check_record.alpha_id == sample_alpha.alpha_id
        assert created_check_record.record_type == AlphaCheckType.CORRELATION_SELF
        assert created_check_record.content == mock_result.model_dump(mode="python")

        # 验证 Correlation 内容
        created_correlations: List[Correlation] = (
            mock_correlation_dal.bulk_upsert.call_args[0][0]
        )
        assert len(created_correlations) == 2
        assert created_correlations[0].alpha_id_a == min(
            sample_alpha.alpha_id, "ALPHA_1"
        )
        assert created_correlations[0].alpha_id_b == max(
            sample_alpha.alpha_id, "ALPHA_2"
        )
        assert created_correlations[0].correlation == 0.5
        assert created_correlations[0].calc_type == CorrelationCalcType.PLATFORM

    @pytest.mark.asyncio
    async def test_correlation_check_prod_finished(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
    ) -> None:
        """测试生产相关性检查立即完成。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        mock_correlation_dal = mock_db_session["correlation_dal"]

        # 模拟 API 返回 (生产相关性通常不返回具体列表) - 修复 TableView 结构
        mock_result = TableView(
            table_schema=TableSchemaView(  # 使用 TableSchemaView
                name="prod_correlation_schema",
                title="Prod Correlation Data",
                properties=[
                    TableSchemaView.Property(
                        name="range", title="Range", data_type="string"
                    ),
                    TableSchemaView.Property(
                        name="count", title="Count", data_type="integer"
                    ),
                ],
            ),
            records=[["0.0-0.1", 10], ["0.1-0.2", 5]],
            # total_count 不是 TableView 的直接字段
        )
        mock_wq_client.alpha_correlation_check.return_value = (True, None, mock_result)

        with patch(
            "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
        ):
            await base_evaluator.correlation_check(CorrelationType.PROD)

        mock_wq_client.alpha_correlation_check.assert_called_once_with(
            alpha_id=sample_alpha.alpha_id, corr_type=CorrelationType.PROD
        )
        mock_check_record_dal.create.assert_called_once()
        # 生产相关性不写入 Correlation 表
        mock_correlation_dal.bulk_upsert.assert_not_called()

        created_check_record: CheckRecord = mock_check_record_dal.create.call_args[0][0]
        assert created_check_record.alpha_id == sample_alpha.alpha_id
        assert created_check_record.record_type == AlphaCheckType.CORRELATION_PROD
        assert created_check_record.content == mock_result.model_dump(mode="python")

    @pytest.mark.asyncio
    async def test_correlation_check_retry_then_finished(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
    ) -> None:
        """测试相关性检查需要重试。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        mock_correlation_dal = mock_db_session["correlation_dal"]

        mock_result = TableView(
            table_schema=TableSchemaView(  # 使用 TableSchemaView
                name="correlation_schema",
                title="Correlation Data",
                properties=[
                    TableSchemaView.Property(
                        name="id", title="Alpha ID", data_type="string"
                    ),
                    TableSchemaView.Property(
                        name="correlation", title="Correlation", data_type="number"
                    ),
                ],
            ),
            records=[["OTHERALPHA1", 0.5]],
            # total_count 不是 TableView 的直接字段
        )
        # 第一次返回未完成，需要重试
        # 第二次返回完成
        mock_wq_client.alpha_correlation_check.side_effect = [
            (False, 0.1, None),  # retry_after 0.1 秒
            (True, None, mock_result),
        ]

        with (
            patch(
                "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
            ),
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):  # 模拟 sleep
            await base_evaluator.correlation_check(CorrelationType.SELF)

        assert mock_wq_client.alpha_correlation_check.call_count == 2
        mock_sleep.assert_called_once_with(0.1)  # 验证 sleep 被调用且时间正确
        mock_check_record_dal.create.assert_called_once()
        mock_correlation_dal.bulk_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_correlation_check_api_error(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
    ) -> None:
        """测试相关性检查 API 调用出错。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        mock_correlation_dal = mock_db_session["correlation_dal"]

        mock_wq_client.alpha_correlation_check.side_effect = Exception("API Error")

        with patch(
            "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
        ):
            # 异常应该被捕获，不向上抛出，但会记录错误日志
            await base_evaluator.correlation_check(CorrelationType.SELF)

        mock_wq_client.alpha_correlation_check.assert_called_once()
        # 数据库操作不应被调用
        mock_check_record_dal.create.assert_not_called()
        mock_correlation_dal.bulk_upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_correlation_check_cancelled(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        sample_alpha: Alpha,
    ) -> None:
        """测试相关性检查在 sleep 时被取消。"""
        # 第一次返回未完成，需要重试
        mock_wq_client.alpha_correlation_check.return_value = (
            False,
            10.0,
            None,
        )  # retry_after 10 秒

        async def cancel_during_sleep(*args: Any, **kwargs: Any) -> None:
            """模拟取消操作的协程函数。"""
            # 模拟 sleep，并在执行时引发 CancelledError
            raise asyncio.CancelledError

        with (
            patch(
                "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
            ),
            patch("asyncio.sleep", side_effect=cancel_during_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await base_evaluator.correlation_check(CorrelationType.SELF)

        mock_wq_client.alpha_correlation_check.assert_called_once()

    @pytest.mark.asyncio
    async def test_self_correlation_check(self, base_evaluator: BaseEvaluator) -> None:
        """测试 self_correlation_check 方法。"""
        with patch.object(
            base_evaluator, "correlation_check", new_callable=AsyncMock
        ) as mock_corr_check:
            await base_evaluator.self_correlation_check()
            mock_corr_check.assert_called_once_with(CorrelationType.SELF)

    @pytest.mark.asyncio
    async def test_prod_correlation_check(self, base_evaluator: BaseEvaluator) -> None:
        """测试 prod_correlation_check 方法。"""
        with patch.object(
            base_evaluator, "correlation_check", new_callable=AsyncMock
        ) as mock_corr_check:
            await base_evaluator.prod_correlation_check()
            mock_corr_check.assert_called_once_with(CorrelationType.PROD)

    @pytest.mark.asyncio
    async def test_before_and_after_performance_check_finished(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
        sample_competition: Competition,
    ) -> None:
        """测试前后性能检查立即完成。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        competition_id = sample_competition.competition_id

        # 模拟 API 返回 - 修复 BeforeAndAfterPerformanceView 及其子对象结构
        mock_result = BeforeAndAfterPerformanceView(
            stats=BeforeAndAfterPerformanceView.Stats(
                before=StatsView(  # 使用 StatsView
                    book_size=100,
                    pnl=10.0,
                    long_count=5,
                    short_count=2,
                    drawdown=0.1,
                    turnover=0.2,
                    returns=0.05,
                    margin=0.3,
                    sharpe=1.5,
                    fitness=1.1,
                ),
                after=StatsView(  # 使用 StatsView
                    book_size=110,
                    pnl=11.0,
                    long_count=6,
                    short_count=3,
                    drawdown=0.09,
                    turnover=0.21,
                    returns=0.06,
                    margin=0.31,
                    sharpe=1.6,
                    fitness=1.2,
                ),
            ),
            yearly_stats=BeforeAndAfterPerformanceView.YearlyStats(
                before=TableView(
                    table_schema=TableSchemaView(  # 使用 TableSchemaView
                        name="yearly_stats_schema",
                        title="Yearly Stats",
                        properties=[
                            TableSchemaView.Property(
                                name="year", title="Year", data_type="integer"
                            ),
                            TableSchemaView.Property(
                                name="sharpe", title="Sharpe", data_type="number"
                            ),
                        ],
                    ),
                    records=[[2022, 1.4], [2023, 1.6]],
                ),
                after=TableView(
                    table_schema=TableSchemaView(  # 使用 TableSchemaView
                        name="yearly_stats_schema",
                        title="Yearly Stats",
                        properties=[
                            TableSchemaView.Property(
                                name="year", title="Year", data_type="integer"
                            ),
                            TableSchemaView.Property(
                                name="sharpe", title="Sharpe", data_type="number"
                            ),
                        ],
                    ),
                    records=[[2022, 1.5], [2023, 1.7]],
                ),
            ),
            pnl=TableView(
                table_schema=TableSchemaView(  # 使用 TableSchemaView
                    name="pnl_schema",
                    title="PnL Data",
                    properties=[
                        TableSchemaView.Property(
                            name="date", title="Date", data_type="string"
                        ),
                        TableSchemaView.Property(
                            name="pnl", title="PnL", data_type="number"
                        ),
                    ],
                ),
                records=[["2023-01-01", 1.0], ["2023-01-02", -0.5]],
            ),
            partition=["region", "sector"],
            competition=BeforeAndAfterPerformanceView.CompetitionRefView(
                id=competition_id,
                name="Test Comp",
                scoring=CompetitionScoring.PERFORMANCE,  # 修复：使用正确的枚举值
            ),
            score=BeforeAndAfterPerformanceView.ScoreView(before=1.5, after=1.6),
        )
        mock_wq_client.alpha_fetch_before_and_after_performance.return_value = (
            True,
            None,
            mock_result,
            None,
        )

        with patch(
            "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
        ):
            await base_evaluator.before_and_after_performance_check(competition_id)

        mock_wq_client.alpha_fetch_before_and_after_performance.assert_called_once_with(
            alpha_id=sample_alpha.alpha_id, competition_id=competition_id
        )
        mock_check_record_dal.create.assert_called_once()

        created_check_record: CheckRecord = mock_check_record_dal.create.call_args[0][0]
        assert created_check_record.alpha_id == sample_alpha.alpha_id
        assert (
            created_check_record.record_type
            == AlphaCheckType.BEFORE_AND_AFTER_PERFORMANCE
        )
        assert created_check_record.content == mock_result.model_dump(mode="python")

    @pytest.mark.asyncio
    async def test_before_and_after_performance_check_retry(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
        sample_competition: Competition,
    ) -> None:
        """测试前后性能检查需要重试。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        competition_id = sample_competition.competition_id

        mock_result = BeforeAndAfterPerformanceView(
            stats=BeforeAndAfterPerformanceView.Stats(
                before=StatsView(  # 使用 StatsView
                    book_size=100,
                    pnl=10.0,
                    long_count=5,
                    short_count=2,
                    drawdown=0.1,
                    turnover=0.2,
                    returns=0.05,
                    margin=0.3,
                    sharpe=1.5,
                    fitness=1.1,
                ),
                after=StatsView(  # 使用 StatsView
                    book_size=110,
                    pnl=11.0,
                    long_count=6,
                    short_count=3,
                    drawdown=0.09,
                    turnover=0.21,
                    returns=0.06,
                    margin=0.31,
                    sharpe=1.6,
                    fitness=1.2,
                ),
            ),
            yearly_stats=BeforeAndAfterPerformanceView.YearlyStats(
                before=TableView(
                    table_schema=TableSchemaView(  # 使用 TableSchemaView
                        name="yearly_stats_schema",
                        title="Yearly Stats",
                        properties=[
                            TableSchemaView.Property(
                                name="year", title="Year", data_type="integer"
                            ),
                            TableSchemaView.Property(
                                name="sharpe", title="Sharpe", data_type="number"
                            ),
                        ],
                    ),
                    records=[[2022, 1.4], [2023, 1.6]],
                ),
                after=TableView(
                    table_schema=TableSchemaView(  # 使用 TableSchemaView
                        name="yearly_stats_schema",
                        title="Yearly Stats",
                        properties=[
                            TableSchemaView.Property(
                                name="year", title="Year", data_type="integer"
                            ),
                            TableSchemaView.Property(
                                name="sharpe", title="Sharpe", data_type="number"
                            ),
                        ],
                    ),
                    records=[[2022, 1.5], [2023, 1.7]],
                ),
            ),
            pnl=TableView(
                table_schema=TableSchemaView(  # 使用 TableSchemaView
                    name="pnl_schema",
                    title="PnL Data",
                    properties=[
                        TableSchemaView.Property(
                            name="date", title="Date", data_type="string"
                        ),
                        TableSchemaView.Property(
                            name="pnl", title="PnL", data_type="number"
                        ),
                    ],
                ),
                records=[["2023-01-01", 1.0], ["2023-01-02", -0.5]],
            ),
            partition=["region", "sector"],
            # competition 和 score 在重试成功后才会有，第一次调用时为 None
        )
        mock_wq_client.alpha_fetch_before_and_after_performance.side_effect = [
            (False, 0.1, None, None),
            (True, None, mock_result, None),
        ]

        with (
            patch(
                "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
            ),
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            await base_evaluator.before_and_after_performance_check(competition_id)

        assert mock_wq_client.alpha_fetch_before_and_after_performance.call_count == 2
        mock_sleep.assert_called_once_with(0.1)
        mock_check_record_dal.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_before_and_after_performance_check_api_error(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
        sample_alpha: Alpha,
        sample_competition: Competition,
    ) -> None:
        """测试前后性能检查 API 调用出错。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]
        competition_id = sample_competition.competition_id

        mock_wq_client.alpha_fetch_before_and_after_performance.side_effect = Exception(
            "API Error"
        )

        with patch(
            "alphapower.engine.evaluate.base_evaluator.wq_client", mock_wq_client
        ):
            await base_evaluator.before_and_after_performance_check(competition_id)

        mock_wq_client.alpha_fetch_before_and_after_performance.assert_called_once()
        mock_check_record_dal.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_before_and_after_performance_check_no_competition_id(
        self,
        base_evaluator: BaseEvaluator,
        mock_wq_client: MagicMock,
        mock_db_session: dict,
    ) -> None:
        """测试前后性能检查 competition_id 为 None。"""
        mock_check_record_dal = mock_db_session["check_record_dal"]

        await base_evaluator.before_and_after_performance_check(None)  # type: ignore

        mock_wq_client.alpha_fetch_before_and_after_performance.assert_not_called()
        mock_check_record_dal.create.assert_not_called()
