import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alphapower.constants import (
    AlphaType,
    CheckRecordType,
    Color,
    Grade,
    RefreshPolicy,
    Stage,
    Status,
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.entity import Alpha


@pytest.fixture(name="mock_alpha")
def fixture_mock_alpha() -> Alpha:
    """提供一个模拟的 Alpha 对象"""
    return Alpha(
        id=1,
        alpha_id="test_alpha_001",
        author="test_author",
        date_created=datetime.now(),
        settings_id=1,
        regular_id=1,
        favorite=False,
        hidden=False,
        type=AlphaType.REGULAR,
        color=Color.NONE,
        grade=Grade.AVERAGE,
        stage=Stage.IS,
        status=Status.ACTIVE,
        name="测试Alpha",
        category="测试类别",
        tags=["测试标签"],
        themes=None,
        pyramids=None,
        team=None,
        date_submitted=None,
        date_modified=None,
        settings=None,
        regular=None,
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


@pytest.fixture(name="evaluator")
def fixture_evaluator() -> BaseEvaluator:
    """提供一个 BaseEvaluator 的实例，注入模拟依赖"""
    fetcher = MagicMock()
    correlation_dal = AsyncMock()
    check_record_dal = AsyncMock()
    client = AsyncMock()

    evaluator_instance = BaseEvaluator(
        fetcher=fetcher,
        correlation_dal=correlation_dal,
        check_record_dal=check_record_dal,
        client=client,
    )
    return evaluator_instance


@pytest.mark.asyncio
class TestBaseEvaluatorEvaluateOne:
    """测试 BaseEvaluator.evaluate_one 方法"""

    async def test_evaluate_one_all_checks_passed(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试所有检查通过的情况"""
        checks_to_run = [CheckRecordType.CORRELATION_SELF, CheckRecordType.SUBMISSION]
        with (
            patch.object(
                evaluator,
                "_get_checks_to_run",
                AsyncMock(return_value=(checks_to_run, RefreshPolicy.USE_EXISTING)),
            ) as mock_get_checks,
            patch.object(
                evaluator,
                "_execute_checks",
                AsyncMock(return_value={check: True for check in checks_to_run}),
            ) as mock_execute_checks,
        ):
            result = await evaluator.evaluate_one(
                mock_alpha, RefreshPolicy.USE_EXISTING
            )

            assert result is True
            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )
            mock_execute_checks.assert_awaited_once_with(
                alpha=mock_alpha,
                checks=checks_to_run,
                policy=RefreshPolicy.USE_EXISTING,
            )

    async def test_evaluate_one_some_checks_failed(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试部分检查未通过的情况"""
        checks_to_run = [CheckRecordType.CORRELATION_SELF, CheckRecordType.SUBMISSION]
        with (
            patch.object(
                evaluator,
                "_get_checks_to_run",
                AsyncMock(return_value=(checks_to_run, RefreshPolicy.USE_EXISTING)),
            ) as mock_get_checks,
            patch.object(
                evaluator,
                "_execute_checks",
                AsyncMock(
                    return_value={
                        CheckRecordType.CORRELATION_SELF: True,
                        CheckRecordType.SUBMISSION: False,
                    }
                ),
            ) as mock_execute_checks,
        ):
            result = await evaluator.evaluate_one(
                mock_alpha, RefreshPolicy.USE_EXISTING
            )

            assert result is False
            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )
            mock_execute_checks.assert_awaited_once_with(
                alpha=mock_alpha,
                checks=checks_to_run,
                policy=RefreshPolicy.USE_EXISTING,
            )

    async def test_evaluate_one_no_checks_to_run(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试没有需要执行的检查时的情况"""
        with patch.object(
            evaluator,
            "_get_checks_to_run",
            AsyncMock(return_value=([], RefreshPolicy.USE_EXISTING)),
        ) as mock_get_checks:
            result = await evaluator.evaluate_one(
                mock_alpha, RefreshPolicy.USE_EXISTING
            )

            assert result is True
            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )

    async def test_evaluate_one_raises_not_implemented_error(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试子类未实现必要方法时抛出 NotImplementedError 的情况"""
        with patch.object(
            evaluator,
            "_get_checks_to_run",
            AsyncMock(side_effect=NotImplementedError("未实现的方法")),
        ) as mock_get_checks:
            with pytest.raises(NotImplementedError, match="未实现的方法"):
                await evaluator.evaluate_one(mock_alpha, RefreshPolicy.USE_EXISTING)

            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )

    async def test_evaluate_one_cancelled_error(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试任务被取消时的处理"""
        with patch.object(
            evaluator,
            "_get_checks_to_run",
            AsyncMock(side_effect=asyncio.CancelledError),
        ) as mock_get_checks:
            with pytest.raises(asyncio.CancelledError):
                await evaluator.evaluate_one(mock_alpha, RefreshPolicy.USE_EXISTING)

            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )

    async def test_evaluate_one_unexpected_exception(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
    ) -> None:
        """测试发生未预期异常时的情况"""
        with patch.object(
            evaluator,
            "_get_checks_to_run",
            AsyncMock(side_effect=ValueError("未知错误")),
        ) as mock_get_checks:
            with pytest.raises(ValueError, match="未知错误"):
                await evaluator.evaluate_one(mock_alpha, RefreshPolicy.USE_EXISTING)

            mock_get_checks.assert_awaited_once_with(
                alpha=mock_alpha, policy=RefreshPolicy.USE_EXISTING
            )
