import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from alphapower.constants import RefreshPolicy
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.entity import Alpha

# mypy: disable-error-code="method-assign, attr-defined"


@pytest.fixture(name="mock_fetcher")
def fixture_mock_fetcher() -> MagicMock:
    """提供一个 AbstractAlphaFetcher 的模拟对象。"""
    return MagicMock()


@pytest.fixture(name="mock_check_record_dal")
def fixture_mock_check_record_dal() -> AsyncMock:
    """提供一个 CheckRecordDAL 的异步模拟对象。"""
    return AsyncMock()


@pytest.fixture(name="mock_correlation_dal")
def fixture_mock_correlation_dal() -> AsyncMock:
    """提供一个 CorrelationDAL 的异步模拟对象。"""
    return AsyncMock()


@pytest.fixture(name="mock_client")
def fixture_mock_client() -> AsyncMock:
    """提供一个 WorldQuantClient 的异步模拟对象。"""
    return AsyncMock()


@pytest.fixture(name="base_evaluator")
def fixture_base_evaluator(
    mock_fetcher: MagicMock,
    mock_check_record_dal: AsyncMock,
    mock_correlation_dal: AsyncMock,
    mock_client: AsyncMock,
) -> BaseEvaluator:
    """提供一个 BaseEvaluator 的实例，注入模拟依赖。"""
    return BaseEvaluator(
        fetcher=mock_fetcher,
        correlation_dal=mock_correlation_dal,
        check_record_dal=mock_check_record_dal,
        client=mock_client,
    )


@pytest.mark.asyncio
class TestEvaluateMany:
    """测试 BaseEvaluator.evaluate_many 方法的类。"""

    async def test_evaluate_many_success(
        self,
        base_evaluator: BaseEvaluator,
        mock_fetcher: MagicMock,
    ) -> None:
        """测试 evaluate_many 成功评估所有 Alpha 的情况。"""
        # 安排 (Arrange)
        mock_fetcher.fetch_alphas.return_value = [
            Alpha(alpha_id="alpha_1"),
            Alpha(alpha_id="alpha_2"),
        ]
        base_evaluator.evaluate_one = AsyncMock(return_value=True)
        base_evaluator.to_evaluate_alpha_count = AsyncMock(return_value=2)

        # 动作 (Act)
        results = []
        async for alpha in base_evaluator.evaluate_many(
            policy=RefreshPolicy.FORCE_REFRESH, concurrency=2
        ):
            results.append(alpha)

        # 断言 (Assert)
        assert len(results) == 2
        assert results[0].alpha_id == "alpha_1"
        assert results[1].alpha_id == "alpha_2"
        base_evaluator.evaluate_one.assert_awaited()

    async def test_evaluate_many_partial_failure(
        self,
        base_evaluator: BaseEvaluator,
        mock_fetcher: MagicMock,
    ) -> None:
        """测试 evaluate_many 部分 Alpha 评估失败的情况。"""
        # 安排 (Arrange)
        mock_fetcher.fetch_alphas.return_value = [
            Alpha(alpha_id="alpha_1"),
            Alpha(alpha_id="alpha_2"),
        ]
        base_evaluator.evaluate_one = AsyncMock(side_effect=[True, False])
        base_evaluator.to_evaluate_alpha_count = AsyncMock(return_value=2)

        # 动作 (Act)
        results = []
        async for alpha in base_evaluator.evaluate_many(
            policy=RefreshPolicy.FORCE_REFRESH, concurrency=2
        ):
            results.append(alpha)

        # 断言 (Assert)
        assert len(results) == 1
        assert results[0].alpha_id == "alpha_1"
        base_evaluator.evaluate_one.assert_awaited()

    async def test_evaluate_many_with_exception(
        self,
        base_evaluator: BaseEvaluator,
        mock_fetcher: MagicMock,
    ) -> None:
        """测试 evaluate_many 捕获 evaluate_one 异常并继续处理的情况。"""
        # 安排 (Arrange)
        mock_fetcher.fetch_alphas.return_value = [
            Alpha(alpha_id="alpha_1"),
            Alpha(alpha_id="alpha_2"),
        ]
        base_evaluator.evaluate_one = AsyncMock(side_effect=[True, Exception("Error")])
        base_evaluator.to_evaluate_alpha_count = AsyncMock(return_value=2)

        # 动作 (Act)
        results = []
        async for alpha in base_evaluator.evaluate_many(
            policy=RefreshPolicy.FORCE_REFRESH, concurrency=2
        ):
            results.append(alpha)

        # 断言 (Assert)
        assert len(results) == 1
        assert results[0].alpha_id == "alpha_1"
        base_evaluator.evaluate_one.assert_awaited()

    async def test_evaluate_many_cancelled(
        self,
        base_evaluator: BaseEvaluator,
        mock_fetcher: MagicMock,
    ) -> None:
        """测试 evaluate_many 捕获 CancelledError 并继续处理的情况。"""
        # 安排 (Arrange)
        mock_fetcher.fetch_alphas.return_value = [
            Alpha(alpha_id="alpha_1"),
            Alpha(alpha_id="alpha_2"),
        ]
        base_evaluator.evaluate_one = AsyncMock(
            side_effect=[asyncio.CancelledError("任务取消"), True]
        )
        base_evaluator.to_evaluate_alpha_count = AsyncMock(return_value=2)

        # 动作 (Act)
        results = []
        async for alpha in base_evaluator.evaluate_many(
            policy=RefreshPolicy.FORCE_REFRESH, concurrency=2
        ):
            results.append(alpha)

        # 断言 (Assert)
        assert len(results) == 1
        assert results[0].alpha_id == "alpha_2"
        base_evaluator.evaluate_one.assert_awaited()
