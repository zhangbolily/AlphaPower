"""测试 BaseEvaluator 的单元测试模块。"""

import asyncio
from datetime import datetime  # 导入 datetime
from unittest.mock import ANY, AsyncMock, MagicMock, patch  # 导入 ANY

import pytest

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    TableSchemaView,
    TableView,
)
from alphapower.client.checks_view import StatsView
from alphapower.constants import (  # 导入所需的枚举
    AlphaType,
    CheckRecordType,
    Color,
    Grade,
    RefreshPolicy,
    Stage,
    Status,
)
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.engine.evaluate.alpha_fetcher_abc import AbstractAlphaFetcher
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.entity import Alpha
from alphapower.entity.evaluate import CheckRecord

# pylint: disable=W0212, W0613


@pytest.fixture(name="mock_fetcher")
def fixture_mock_fetcher() -> MagicMock:
    """提供一个 AbstractAlphaFetcher 的模拟对象。"""
    return MagicMock(spec=AbstractAlphaFetcher)


@pytest.fixture(name="mock_correlation_dal")
def fixture_mock_correlation_dal() -> AsyncMock:
    """提供一个 CorrelationDAL 的异步模拟对象。"""
    return AsyncMock(spec=CorrelationDAL)


@pytest.fixture(name="mock_check_record_dal")
def fixture_mock_check_record_dal() -> AsyncMock:
    """提供一个 CheckRecordDAL 的异步模拟对象。"""
    return AsyncMock(spec=CheckRecordDAL)


@pytest.fixture(name="mock_client")
def fixture_mock_client() -> MagicMock:
    """提供一个 WorldQuantClient 的模拟对象，包含异步上下文管理器。"""
    mock = MagicMock()
    # 模拟异步上下文管理器
    mock.__aenter__ = AsyncMock(return_value=mock)
    mock.__aexit__ = AsyncMock(return_value=None)
    # 模拟需要的方法
    mock.alpha_fetch_before_and_after_performance = AsyncMock()
    return mock


@pytest.fixture(name="test_alpha")
def fixture_test_alpha() -> Alpha:
    """提供一个测试用的 Alpha 实例。"""
    # 创建一个符合 Alpha 实体定义的实例
    return Alpha(
        id=1,  # 添加主键 ID
        alpha_id="test001",  # 使用 alpha_id 字段
        author="test_user",  # 使用 author 字段
        settings_id=1,  # 提供外键 ID
        regular_id=1,  # 提供外键 ID
        date_created=datetime.now(),  # 提供创建日期
        favorite=False,  # 提供布尔值
        hidden=False,  # 提供布尔值
        type=AlphaType.REGULAR,  # 使用 AlphaType 枚举
        color=Color.NONE,  # 使用 Color 枚举
        grade=Grade.GOOD,  # 使用 Grade 枚举
        stage=Stage.IS,  # 使用 Stage 枚举
        status=Status.ACTIVE,  # 使用 Status 枚举
        # 以下字段是可选的或通过关系加载，测试中可以省略或设为 None
        name="测试Alpha名称",
        category="PRICE_MOMENTUM",
        tags=["测试标签1", "测试标签2"],
        themes=None,
        pyramids=None,
        team=None,
        date_submitted=None,
        date_modified=None,
        settings=None,  # 关系字段在测试中通常不需要完整对象
        regular=None,  # 关系字段在测试中通常不需要完整对象
        in_sample_id=None,
        in_sample=None,
        out_sample_id=None,
        out_sample=None,
        train_id=None,
        train=None,
        test_id=None,
        test=None,
        prod_id=None,
        prod=None,
        classifications=[],
        competitions=[],
    )


@pytest.fixture(name="performance_view")
def fixture_performance_view() -> BeforeAndAfterPerformanceView:
    """提供一个测试用的 BeforeAndAfterPerformanceView 实例。"""
    # 创建符合 StatsView 结构的示例数据
    stats_data = StatsView(
        book_size=10000,
        pnl=150.5,
        long_count=50,
        short_count=45,
        drawdown=0.1,
        turnover=0.5,
        returns=0.05,
        margin=0.02,
        sharpe=1.2,
        fitness=0.8,
    )
    # 创建符合 TableView 结构的示例数据
    table_schema = TableSchemaView(
        name="test_table",
        title="测试表格",
        properties=[
            TableSchemaView.Property(name="col1", title="列1", data_type="integer"),
            TableSchemaView.Property(name="col2", title="列2", data_type="string"),
        ],
    )
    table_data = TableView(
        table_schema=table_schema, records=[[1, "a"], [2, "b"]], min=1.0, max=2.0
    )

    return BeforeAndAfterPerformanceView(
        stats=BeforeAndAfterPerformanceView.Stats(before=stats_data, after=stats_data),
        yearly_stats=BeforeAndAfterPerformanceView.YearlyStats(
            before=table_data, after=table_data
        ),
        pnl=table_data,
        partition=["year", "month"],
        # 可选字段，根据需要添加
        # competition=BeforeAndAfterPerformanceView.CompetitionRefView(...),
        # score=BeforeAndAfterPerformanceView.ScoreView(...),
    )


@pytest.fixture(name="base_evaluator")
def fixture_base_evaluator(
    mock_fetcher: MagicMock,
    mock_correlation_dal: AsyncMock,
    mock_check_record_dal: AsyncMock,
    mock_client: MagicMock,
) -> BaseEvaluator:
    """提供一个 BaseEvaluator 的实例，注入模拟依赖。"""
    return BaseEvaluator(
        fetcher=mock_fetcher,
        correlation_dal=mock_correlation_dal,
        check_record_dal=mock_check_record_dal,
        client=mock_client,
    )


@pytest.mark.asyncio
class TestBaseEvaluatorCheckAlphaPoolPerformanceDiff:
    """测试 BaseEvaluator._check_alpha_pool_performance_diff 方法的类。"""

    COMPETITION_ID: str = "test_comp_id"  # 保留 COMPETITION_ID 用于方法调用

    async def test_check_performance_diff_use_existing_found_passes(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 USE_EXISTING, 找到记录, 且检查通过的情况。"""
        # 安排 (Arrange)
        check_record = CheckRecord(
            id=1,  # 添加记录 ID
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        # 使用 patch.object 模拟内部方法
        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            # 设置模拟方法的返回值
            mock_determine_action.return_value = BaseEvaluator.CheckAction.USE_EXISTING
            mock_determine_pass.return_value = True  # 模拟检查通过

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.USE_EXISTING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once_with(
                policy=RefreshPolicy.USE_EXISTING,
                exist_check_record=check_record,
                alpha_id=test_alpha.alpha_id,
                check_type_name="因子池绩效差异",
            )
            mock_refresh.assert_not_awaited()  # 不应调用刷新
            # 验证解析和检查逻辑被调用
            mock_determine_pass.assert_awaited_once_with(
                alpha=test_alpha,
                perf_diff_view=performance_view,  # 验证传入的是解析后的对象
                competition_id=self.COMPETITION_ID,
            )
            assert result is True  # 结果应为 True

    async def test_check_performance_diff_use_existing_found_fails(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 USE_EXISTING, 找到记录, 但检查未通过的情况。"""
        # 安排 (Arrange)
        check_record = CheckRecord(
            id=1,
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.USE_EXISTING
            mock_determine_pass.return_value = False  # 模拟检查失败

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.USE_EXISTING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_awaited_once()  # 检查逻辑仍被调用
            assert result is False  # 结果应为 False

    async def test_check_performance_diff_use_existing_found_parse_error(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试策略 USE_EXISTING, 找到记录, 但内容解析失败的情况。"""
        # 安排 (Arrange) - 模拟一个内容格式错误的记录
        invalid_content = {"invalid_key": "some_value"}
        check_record = CheckRecord(
            id=1,
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=invalid_content,
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.USE_EXISTING

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.USE_EXISTING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_not_awaited()  # 解析失败，不应调用检查逻辑
            assert result is False  # 解析失败，结果应为 False

    async def test_check_performance_diff_use_existing_not_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试策略 USE_EXISTING 但未找到记录的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.FAIL_MISSING

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.USE_EXISTING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once_with(
                policy=RefreshPolicy.USE_EXISTING,
                exist_check_record=None,
                alpha_id=test_alpha.alpha_id,
                check_type_name="因子池绩效差异",
            )
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_not_awaited()
            assert result is False  # 因记录缺失而失败

    async def test_check_performance_diff_force_refresh_success(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 FORCE_REFRESH 且刷新成功的情况。"""
        # 安排 (Arrange)
        # 即使记录存在，也应该强制刷新
        check_record = CheckRecord(
            id=1,
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.return_value = performance_view  # 模拟刷新成功
            mock_determine_pass.return_value = True  # 模拟检查通过

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.FORCE_REFRESH,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once_with(
                alpha=test_alpha, competition_id=self.COMPETITION_ID
            )
            mock_determine_pass.assert_awaited_once_with(
                alpha=test_alpha,
                perf_diff_view=performance_view,
                competition_id=self.COMPETITION_ID,
            )
            assert result is True

    async def test_check_performance_diff_force_refresh_fails(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 FORCE_REFRESH 但刷新失败的情况 (返回 None)。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None  # 记录是否存在不影响

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.return_value = None  # 模拟刷新失败

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.FORCE_REFRESH,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine_pass.assert_not_awaited()  # 刷新失败，不应调用检查逻辑
            assert result is False  # 刷新失败，结果为 False

    async def test_check_performance_diff_refresh_async_if_missing_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 REFRESH_ASYNC_IF_MISSING 且找到记录的情况。"""
        # 安排 (Arrange)
        check_record = CheckRecord(
            id=1,
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.USE_EXISTING
            mock_determine_pass.return_value = True

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_awaited_once()
            assert result is True

    async def test_check_performance_diff_refresh_async_if_missing_not_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 REFRESH_ASYNC_IF_MISSING 但未找到记录的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.return_value = performance_view
            mock_determine_pass.return_value = True

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine_pass.assert_awaited_once()
            assert result is True

    async def test_check_performance_diff_skip_if_missing_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 SKIP_IF_MISSING 且找到记录的情况。"""
        # 安排 (Arrange)
        check_record = CheckRecord(
            id=1,
            alpha_id=test_alpha.alpha_id,
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.USE_EXISTING
            mock_determine_pass.return_value = True

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.SKIP_IF_MISSING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_awaited_once()
            assert result is True

    async def test_check_performance_diff_skip_if_missing_not_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试策略 SKIP_IF_MISSING 但未找到记录的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.SKIP

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.SKIP_IF_MISSING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine_pass.assert_not_awaited()
            assert result is False  # 跳过，结果为 False

    async def test_check_performance_diff_refresh_raises_runtime_error(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试刷新数据时发生 RuntimeError 的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        refresh_error = RuntimeError("API 错误")

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.side_effect = refresh_error  # 模拟刷新抛出异常

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,  # 触发刷新
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine_pass.assert_not_awaited()  # 刷新异常，不应调用检查逻辑
            # 异常被捕获，方法返回 False
            assert result is False

    async def test_check_performance_diff_refresh_raises_other_exception(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试刷新数据时发生其他未捕获异常的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        refresh_error = ValueError("其他错误")

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.side_effect = refresh_error  # 模拟刷新抛出异常

            # 行动 & 断言 (Act & Assert)
            with pytest.raises(ValueError, match="其他错误"):  # 验证异常被重新抛出
                await base_evaluator._check_alpha_pool_performance_diff(
                    alpha=test_alpha,
                    competition_id=self.COMPETITION_ID,
                    policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,  # 触发刷新
                )
            # 验证调用
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once()

    async def test_check_performance_diff_refresh_cancelled(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试刷新数据时任务被取消的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        cancel_error = asyncio.CancelledError("任务取消")

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
        ):
            mock_determine_action.return_value = BaseEvaluator.CheckAction.REFRESH
            mock_refresh.side_effect = cancel_error  # 模拟刷新抛出取消错误

            # 行动 & 断言 (Act & Assert)
            with pytest.raises(
                asyncio.CancelledError, match="任务取消"
            ):  # 验证取消错误被重新抛出
                await base_evaluator._check_alpha_pool_performance_diff(
                    alpha=test_alpha,
                    competition_id=self.COMPETITION_ID,
                    policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,  # 触发刷新
                )
            # 验证调用
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()
            mock_refresh.assert_awaited_once()

    async def test_check_performance_diff_invalid_policy_error_action(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试 _determine_check_action 返回 ERROR 的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None  # 记录是否存在不影响

        with (
            patch.object(
                base_evaluator, "_determine_check_action", new_callable=AsyncMock
            ) as mock_determine_action,
            patch.object(
                base_evaluator,
                "_refresh_alpha_pool_performance_diff",
                new_callable=AsyncMock,
            ) as mock_refresh,
            patch.object(
                base_evaluator,
                "_determine_performance_diff_pass_status",
                new_callable=AsyncMock,
            ) as mock_determine_pass,
        ):
            # 模拟 _determine_check_action 返回 ERROR
            mock_determine_action.return_value = BaseEvaluator.CheckAction.ERROR
            # 模拟一个无效的策略值传入，虽然 _determine_check_action 会处理它
            invalid_policy_value = "INVALID_POLICY"

            # 行动 (Act)
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=invalid_policy_value,  # type: ignore
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=ANY,  # 使用 ANY 匹配排序对象
            )
            mock_determine_action.assert_awaited_once()  # 验证被调用
            mock_refresh.assert_not_awaited()  # 不应调用刷新
            mock_determine_pass.assert_not_awaited()  # 不应调用检查逻辑
            assert result is False  # 返回 ERROR 时，结果为 False
