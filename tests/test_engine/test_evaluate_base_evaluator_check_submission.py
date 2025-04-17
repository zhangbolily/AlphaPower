import asyncio
from datetime import datetime
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from alphapower.client import SubmissionCheckResultView

# 相对导入被测试的类和函数
from alphapower.constants import (
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

# pylint: disable=W0212, W0613, R0913
# mypy: disable-error-code="method-assign, attr-defined"


@pytest.fixture(name="mock_alpha")
def mock_alpha_fixture() -> Alpha:
    """提供一个模拟的 Alpha 对象"""
    # 根据 Alpha 实体定义创建模拟对象
    # 添加必要的非空字段和合理的默认值
    # 使用 constants.py 中定义的有效枚举值
    return Alpha(
        id=1,
        alpha_id="test_alpha_submission_001",
        author="tester_submission",
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
        name="测试提交Alpha",
        category="TEST",
        tags=["提交测试"],
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


@pytest.fixture(name="mock_fetcher")
def mock_fetcher_fixture() -> MagicMock:
    """提供一个模拟的 AbstractAlphaFetcher"""
    return MagicMock(spec=AbstractAlphaFetcher)


@pytest.fixture(name="mock_correlation_dal")
def mock_correlation_dal_fixture() -> AsyncMock:
    """提供一个模拟的 CorrelationDAL"""
    return AsyncMock(spec=CorrelationDAL)


@pytest.fixture(name="mock_check_record_dal")
def mock_check_record_dal_fixture() -> AsyncMock:
    """提供一个模拟的 CheckRecordDAL"""
    return AsyncMock(spec=CheckRecordDAL)


@pytest.fixture(name="mock_client")
def mock_client_fixture() -> AsyncMock:
    """提供一个模拟的 WorldQuantClient"""
    # 模拟 client 的异步上下文管理器
    mock = AsyncMock()
    mock.__aenter__.return_value = mock  # 进入上下文时返回自身
    mock.__aexit__.return_value = None  # 退出上下文
    return mock


@pytest.fixture(name="evaluator")
def evaluator_fixture(
    mock_fetcher: MagicMock,
    mock_correlation_dal: AsyncMock,
    mock_check_record_dal: AsyncMock,
    mock_client: AsyncMock,
) -> BaseEvaluator:
    """提供一个 BaseEvaluator 实例，注入模拟依赖"""
    # 因为 BaseEvaluator 是抽象类，我们需要一个具体的子类或者模拟它的抽象方法
    # 这里我们直接实例化 BaseEvaluator，并模拟抽象方法，以便测试非抽象方法
    evaluator_instance = BaseEvaluator(
        fetcher=mock_fetcher,
        correlation_dal=mock_correlation_dal,
        check_record_dal=mock_check_record_dal,
        client=mock_client,
    )
    # 模拟抽象方法和内部依赖的方法，避免 NotImplementedError 或实际调用
    evaluator_instance._get_checks_to_run = AsyncMock(
        return_value=([], RefreshPolicy.USE_EXISTING)
    )
    evaluator_instance._execute_checks = AsyncMock(return_value={})
    evaluator_instance._determine_performance_diff_pass_status = AsyncMock(
        return_value=True
    )
    # evaluator_instance._check_submission = AsyncMock(return_value=True) # 不要模拟被测试的方法
    evaluator_instance.evaluate_many = AsyncMock()
    evaluator_instance.evaluate_one = AsyncMock(return_value=True)
    evaluator_instance.to_evaluate_alpha_count = AsyncMock(return_value=0)

    # 模拟 _check_submission 内部调用的方法
    evaluator_instance._determine_check_action = AsyncMock()
    evaluator_instance._refresh_submission_check_data = AsyncMock()
    evaluator_instance._determine_submission_pass_status = AsyncMock()

    return evaluator_instance


@pytest.fixture(name="mock_passing_submission_view")
def mock_passing_submission_view_fixture() -> SubmissionCheckResultView:
    """提供一个模拟的表示通过的 SubmissionCheckResultView"""
    # 创建一个符合 SubmissionCheckResultView 结构的实例
    # 注意：这里的“通过”状态是由测试中模拟的 _determine_submission_pass_status 控制的，
    # 而不是由这个视图对象的内容直接决定的。
    # 因此，我们只需要提供一个结构有效的对象即可。
    # 这里我们创建一个包含空的 in_sample 数据的视图。
    return SubmissionCheckResultView(
        in_sample=SubmissionCheckResultView.Sample(
            checks=[],  # 假设有一些检查项，这里为空列表
            self_correlated=None,  # 假设没有自相关数据
            prod_correlated=None,  # 假设没有生产相关数据
        ),
        out_sample=None,  # 假设没有样本外数据
    )


@pytest.fixture(name="mock_failing_submission_view")
def mock_failing_submission_view_fixture() -> SubmissionCheckResultView:
    """提供一个模拟的表示失败的 SubmissionCheckResultView"""
    # 创建一个符合 SubmissionCheckResultView 结构的实例
    # 注意：这里的“失败”状态是由测试中模拟的 _determine_submission_pass_status 控制的，
    # 而不是由这个视图对象的内容直接决定的。
    # 因此，我们只需要提供一个结构有效的对象即可。
    # 这里我们创建一个包含空的 out_sample 数据的视图。
    return SubmissionCheckResultView(
        in_sample=None,  # 假设没有样本内数据
        out_sample=SubmissionCheckResultView.Sample(
            checks=[],  # 假设有一些检查项，这里为空列表
            self_correlated=None,
            prod_correlated=None,
        ),
    )


@pytest.fixture(name="mock_existing_check_record")
def mock_existing_check_record_fixture(
    mock_passing_submission_view: SubmissionCheckResultView,  # 使用修正后的 fixture
) -> CheckRecord:
    """提供一个模拟的包含有效提交检查结果的 CheckRecord"""
    # 使用修正后的 mock_passing_submission_view 来生成 content
    return CheckRecord(
        id=1,
        alpha_id="test_alpha_submission_001",
        record_type=CheckRecordType.SUBMISSION,
        # 使用 model_dump 将 Pydantic 模型序列化为字典，符合 CheckRecord.content 的预期
        content=mock_passing_submission_view.model_dump(mode="json"),
        created_at=datetime.now(),
    )


@pytest.fixture(name="mock_invalid_check_record")
def mock_invalid_check_record_fixture() -> CheckRecord:
    """提供一个模拟的包含无效内容的 CheckRecord"""
    # 这个 fixture 本身是用于模拟无效 content 的，其结构保持不变
    return CheckRecord(
        id=2,
        alpha_id="test_alpha_submission_001",
        record_type=CheckRecordType.SUBMISSION,
        content={"invalid_key": "some_value"},  # 保持无效结构用于测试解析失败场景
        created_at=datetime.now(),
    )


@pytest.mark.asyncio
class TestBaseEvaluatorCheckSubmission:
    """测试 BaseEvaluator._check_submission 方法的类"""

    async def test_refresh_policy_refresh_success_pass(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_passing_submission_view: SubmissionCheckResultView,
    ) -> None:
        """测试 REFRESH 策略，刷新成功且检查通过"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.FORCE_REFRESH
        mock_check_record_dal.find_one_by.return_value = None
        evaluator._determine_check_action.return_value = (
            BaseEvaluator.CheckAction.REFRESH
        )
        evaluator._refresh_submission_check_data.return_value = (
            mock_passing_submission_view
        )
        evaluator._determine_submission_pass_status.return_value = True

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is True
        mock_check_record_dal.find_one_by.assert_awaited_once_with(
            alpha_id=mock_alpha.alpha_id,
            record_type=CheckRecordType.SUBMISSION,
            order_by=ANY,
        )
        evaluator._determine_check_action.assert_awaited_once_with(
            policy=policy, exist_check_record=None, check_type_name=ANY, alpha_id=ANY
        )
        evaluator._refresh_submission_check_data.assert_awaited_once_with(
            alpha=mock_alpha
        )
        evaluator._determine_submission_pass_status.assert_awaited_once_with(
            submission_check_view=mock_passing_submission_view
        )

    async def test_refresh_policy_refresh_fail(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 REFRESH 策略，刷新失败"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.FORCE_REFRESH
        mock_check_record_dal.find_one_by.return_value = None
        evaluator._determine_check_action.return_value = (
            BaseEvaluator.CheckAction.REFRESH
        )
        evaluator._refresh_submission_check_data.return_value = None  # 模拟刷新失败

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is False
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once()
        evaluator._refresh_submission_check_data.assert_awaited_once_with(
            alpha=mock_alpha
        )
        evaluator._determine_submission_pass_status.assert_not_awaited()

    async def test_use_existing_policy_record_exists_parse_success_pass(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_existing_check_record: CheckRecord,
        mock_passing_submission_view: SubmissionCheckResultView,
    ) -> None:
        """测试 USE_EXISTING 策略，记录存在，解析成功且检查通过"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.USE_EXISTING
        mock_check_record_dal.find_one_by.return_value = mock_existing_check_record
        evaluator._determine_check_action.return_value = (
            BaseEvaluator.CheckAction.USE_EXISTING
        )
        evaluator._determine_submission_pass_status.return_value = True

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is True
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once_with(
            policy=policy,
            exist_check_record=mock_existing_check_record,
            check_type_name=ANY,
            alpha_id=ANY,
        )
        evaluator._refresh_submission_check_data.assert_not_awaited()
        # 验证传入 _determine_submission_pass_status 的对象是否与解析结果一致
        evaluator._determine_submission_pass_status.assert_awaited_once()
        _, call_kwargs = evaluator._determine_submission_pass_status.await_args
        assert "submission_check_view" in call_kwargs
        assert isinstance(
            call_kwargs["submission_check_view"], SubmissionCheckResultView
        )
        assert (
            call_kwargs["submission_check_view"].model_dump()
            == mock_passing_submission_view.model_dump()
        )

    async def test_use_existing_policy_record_exists_parse_fail(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_invalid_check_record: CheckRecord,
    ) -> None:
        """测试 USE_EXISTING 策略，记录存在但解析失败"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.USE_EXISTING
        mock_check_record_dal.find_one_by.return_value = mock_invalid_check_record
        evaluator._determine_check_action.return_value = (
            BaseEvaluator.CheckAction.USE_EXISTING
        )

        # 动作 (Act)
        _: bool = await evaluator._check_submission(alpha=mock_alpha, policy=policy)

        # 断言 (Assert)
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_awaited_once()

    async def test_skip_if_exists_policy_record_exists(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_existing_check_record: CheckRecord,
    ) -> None:
        """测试 SKIP_IF_EXISTS 策略，记录存在时跳过"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.SKIP_IF_MISSING
        mock_check_record_dal.find_one_by.return_value = mock_existing_check_record
        evaluator._determine_check_action.return_value = BaseEvaluator.CheckAction.SKIP

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is False
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_not_awaited()

    async def test_fail_if_missing_policy_record_missing(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 FAIL_IF_MISSING 策略，记录不存在时失败"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.USE_EXISTING
        mock_check_record_dal.find_one_by.return_value = None
        evaluator._determine_check_action.return_value = (
            BaseEvaluator.CheckAction.FAIL_MISSING
        )

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is False
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_not_awaited()

    async def test_general_exception_handling(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试发生未预期异常时的处理"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.FORCE_REFRESH
        test_exception = ValueError("数据库连接错误")
        mock_check_record_dal.find_one_by.side_effect = test_exception

        # 动作 & 断言 (Act & Assert)
        with pytest.raises(ValueError, match="数据库连接错误"):
            await evaluator._check_submission(alpha=mock_alpha, policy=policy)

        # 验证调用
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_not_awaited()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_not_awaited()

    async def test_cancellation_handling(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试任务被取消时的处理"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.FORCE_REFRESH
        mock_check_record_dal.find_one_by.side_effect = asyncio.CancelledError

        # 动作 & 断言 (Act & Assert)
        with pytest.raises(asyncio.CancelledError):
            await evaluator._check_submission(alpha=mock_alpha, policy=policy)

        # 验证调用
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_not_awaited()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_not_awaited()

    async def test_determine_check_action_error(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 _determine_check_action 返回 ERROR 时的处理"""
        # 安排 (Arrange)
        policy: RefreshPolicy = RefreshPolicy.FORCE_REFRESH  # 策略本身可能有效
        mock_check_record_dal.find_one_by.return_value = None
        # 模拟 _determine_check_action 内部逻辑判断后返回 ERROR
        evaluator._determine_check_action.return_value = BaseEvaluator.CheckAction.ERROR

        # 动作 (Act)
        result: bool = await evaluator._check_submission(
            alpha=mock_alpha, policy=policy
        )

        # 断言 (Assert)
        assert result is False
        mock_check_record_dal.find_one_by.assert_awaited_once()
        evaluator._determine_check_action.assert_awaited_once()
        evaluator._refresh_submission_check_data.assert_not_awaited()
        evaluator._determine_submission_pass_status.assert_not_awaited()
