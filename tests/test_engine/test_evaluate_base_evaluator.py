"""测试 BaseEvaluator 的单元测试模块。"""

import asyncio
from datetime import datetime  # 导入 datetime
from unittest.mock import AsyncMock, MagicMock, patch

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

# type: ignore[attr-defined]
# pylint: disable=W0212


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

    async def test_check_performance_diff_use_existing_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 USE_EXISTING 且找到记录的情况。"""
        # 安排 (Arrange)
        check_record = CheckRecord(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,  # 方法调用仍需 competition_id
            policy=RefreshPolicy.USE_EXISTING,
        )

        # 断言 (Assert)
        # 注意：此断言现在可能会失败，因为 CheckRecordDAL 的 find_one_by
        # 可能需要 competition_id，但 CheckRecord 实体本身没有此字段。
        # 需要根据 DAL 的实际实现调整测试或实体。
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
        )
        # 确认没有调用刷新方法
        base_evaluator.client.alpha_fetch_before_and_after_performance.assert_not_awaited()
        # 确认没有调用创建记录方法
        mock_check_record_dal.create.assert_not_awaited()
        # 默认检查逻辑返回 True
        assert result is True

    async def test_check_performance_diff_use_existing_not_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试策略 USE_EXISTING 但未找到记录的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.USE_EXISTING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
        )
        # 确认没有调用刷新方法
        base_evaluator.client.alpha_fetch_before_and_after_performance.assert_not_awaited()
        # 确认没有调用创建记录方法
        mock_check_record_dal.create.assert_not_awaited()
        # 因为没有数据，无法执行检查，应返回 False
        assert result is False

    async def test_check_performance_diff_force_refresh(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        mock_client: MagicMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 FORCE_REFRESH 的情况。"""
        # 安排 (Arrange)
        # 即使记录存在，也应该强制刷新
        check_record = CheckRecord(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record
        # 模拟 API 返回成功
        mock_client.alpha_fetch_before_and_after_performance.return_value = (
            True,
            None,
            performance_view,
        )

        # 行动 (Act)
        # 使用 patch 模拟 _refresh_alpha_pool_performance_diff 内部的数据库写入
        with patch.object(
            base_evaluator.check_record_dal, "create", new_callable=AsyncMock
        ) as mock_create:
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.FORCE_REFRESH,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            )
            # 确认调用了刷新方法
            mock_client.alpha_fetch_before_and_after_performance.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                competition_id=self.COMPETITION_ID,  # 使用 alpha_id 属性
            )
            # 确认调用了创建记录方法 (在 _refresh... 内部)
            mock_create.assert_awaited_once()
            # 默认检查逻辑返回 True
            assert result is True

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
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
        )
        # 确认没有调用刷新方法
        base_evaluator.client.alpha_fetch_before_and_after_performance.assert_not_awaited()
        # 确认没有调用创建记录方法
        mock_check_record_dal.create.assert_not_awaited()
        # 默认检查逻辑返回 True
        assert result is True

    async def test_check_performance_diff_refresh_async_if_missing_not_found(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        mock_client: MagicMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试策略 REFRESH_ASYNC_IF_MISSING 但未找到记录的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        # 模拟 API 返回成功
        mock_client.alpha_fetch_before_and_after_performance.return_value = (
            True,
            None,
            performance_view,
        )

        # 行动 (Act)
        with patch.object(
            base_evaluator.check_record_dal, "create", new_callable=AsyncMock
        ) as mock_create:
            result: bool = await base_evaluator._check_alpha_pool_performance_diff(
                alpha=test_alpha,
                competition_id=self.COMPETITION_ID,
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
            )

            # 断言 (Assert)
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            )
            # 确认调用了刷新方法
            mock_client.alpha_fetch_before_and_after_performance.assert_awaited_once_with(
                alpha_id=test_alpha.alpha_id,
                competition_id=self.COMPETITION_ID,  # 使用 alpha_id 属性
            )
            # 确认调用了创建记录方法
            mock_create.assert_awaited_once()
            # 默认检查逻辑返回 True
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
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.SKIP_IF_MISSING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
        )
        # 确认没有调用刷新方法
        base_evaluator.client.alpha_fetch_before_and_after_performance.assert_not_awaited()
        # 确认没有调用创建记录方法
        mock_check_record_dal.create.assert_not_awaited()
        # 默认检查逻辑返回 True
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

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.SKIP_IF_MISSING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
        )
        # 确认没有调用刷新方法
        base_evaluator.client.alpha_fetch_before_and_after_performance.assert_not_awaited()
        # 确认没有调用创建记录方法
        mock_check_record_dal.create.assert_not_awaited()
        # 未找到记录且策略为跳过，应返回 False
        assert result is False

    async def test_check_performance_diff_refresh_raises_exception(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        mock_client: MagicMock,
        test_alpha: Alpha,
    ) -> None:
        """测试刷新数据时发生异常的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        mock_client.alpha_fetch_before_and_after_performance.side_effect = RuntimeError(
            "API 错误"
        )

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once()
        mock_client.alpha_fetch_before_and_after_performance.assert_awaited_once()
        # 发生异常，检查应视为失败
        assert result is False

    async def test_check_performance_diff_refresh_cancelled(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        mock_client: MagicMock,
        test_alpha: Alpha,
    ) -> None:
        """测试刷新数据时任务被取消的情况。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        mock_client.alpha_fetch_before_and_after_performance.side_effect = (
            asyncio.CancelledError("任务取消")
        )

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
        )

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once()
        mock_client.alpha_fetch_before_and_after_performance.assert_awaited_once()
        # 任务取消，检查应视为失败
        assert result is False

    async def test_check_performance_diff_invalid_policy_raises_error(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
    ) -> None:
        """测试传入无效策略时是否按预期处理（当前实现会抛 ValueError）。"""
        # 安排 (Arrange)
        mock_check_record_dal.find_one_by.return_value = None
        invalid_policy_value = "INVALID_POLICY"  # 模拟一个无效值

        # 行动 & 断言 (Act & Assert)
        with pytest.raises(
            ValueError, match=f"不支持的刷新策略 '{invalid_policy_value}' 或状态组合"
        ):
            # 使用 patch 绕过 RefreshPolicy 枚举的类型检查，模拟无效值传入
            with patch(
                "alphapower.engine.evaluate.base_evaluator.RefreshPolicy",
                MagicMock(),
            ):
                await base_evaluator._check_alpha_pool_performance_diff(
                    alpha=test_alpha,
                    competition_id=self.COMPETITION_ID,
                    policy=invalid_policy_value,  # type: ignore
                )

    # 注意：由于 _check_alpha_pool_performance_diff 内部的实际检查逻辑
    # (标记为 TODO 的部分) 尚未实现，且当前默认返回 True，
    # 因此无法直接测试检查逻辑失败的情况，除非修改该方法或添加更复杂的 mock。
    # 以下测试假设检查逻辑存在且可以被模拟或依赖于 performance_view 的内容。
    # 目前，这些测试将验证在获取数据后，默认逻辑是否返回 True。

    async def test_check_performance_diff_logic_passes_with_data(
        self,
        base_evaluator: BaseEvaluator,
        mock_check_record_dal: AsyncMock,
        test_alpha: Alpha,
        performance_view: BeforeAndAfterPerformanceView,
    ) -> None:
        """测试在获取到数据后，默认检查逻辑是否通过（当前为 True）。"""
        # 安排 (Arrange) - 使用现有数据
        check_record = CheckRecord(
            alpha_id=test_alpha.alpha_id,  # 使用 alpha_id 属性
            record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
            content=performance_view.model_dump(),
        )
        mock_check_record_dal.find_one_by.return_value = check_record

        # 行动 (Act)
        result: bool = await base_evaluator._check_alpha_pool_performance_diff(
            alpha=test_alpha,
            competition_id=self.COMPETITION_ID,
            policy=RefreshPolicy.USE_EXISTING,
        )

        # 断言 (Assert)
        # 确认检查逻辑（当前默认）返回 True
        assert result is True
