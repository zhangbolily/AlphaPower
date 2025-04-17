import asyncio
from datetime import datetime
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from alphapower.client import TableView
from alphapower.client.common_view import TableSchemaView  # 导入 TableSchemaView
from alphapower.constants import AlphaType  # 导入 AlphaType
from alphapower.constants import Color  # 导入 Color
from alphapower.constants import Grade  # 导入 Grade
from alphapower.constants import Stage  # 导入 Stage
from alphapower.constants import Status  # 导入 Status
from alphapower.constants import (
    CheckRecordType,
    CorrelationType,
    RefreshPolicy,
)
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.engine.evaluate.alpha_fetcher_abc import AbstractAlphaFetcher
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.entity import Alpha
from alphapower.entity.evaluate import CheckRecord

# pylint: disable=W0212


@pytest.fixture(name="mock_alpha")
def mock_alpha_fixture() -> Alpha:
    """提供一个模拟的 Alpha 对象"""
    # 根据 Alpha 实体定义创建模拟对象
    # 添加必要的非空字段和合理的默认值
    # 使用 constants.py 中定义的有效枚举值
    return Alpha(
        alpha_id="test_alpha_001",
        author="tester",
        date_created=datetime.now(),
        settings_id=1,  # 假设的 settings ID
        regular_id=1,  # 假设的 regular ID
        favorite=False,
        hidden=False,
        type=AlphaType.REGULAR,  # 使用 AlphaType.REGULAR 替代不存在的 SIMULATION
        color=Color.NONE,  # Color.NONE 是有效的
        grade=Grade.AVERAGE,  # 使用 Grade.AVERAGE 替代不存在的 NEW
        stage=Stage.IS,  # Stage.IS 是有效的
        status=Status.ACTIVE,  # Status.ACTIVE 是有效的
        # 其他可选字段可以根据需要添加，或保持 None/默认值
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
    # 模拟抽象方法，避免 NotImplementedError
    evaluator_instance._get_checks_to_run = AsyncMock(return_value=([], RefreshPolicy.USE_EXISTING))  # type: ignore
    evaluator_instance._execute_checks = AsyncMock(return_value={})  # type: ignore
    evaluator_instance._determine_performance_diff_pass_status = AsyncMock(return_value=True)  # type: ignore
    evaluator_instance._check_submission = AsyncMock(return_value=True)  # type: ignore
    evaluator_instance.evaluate_many = AsyncMock()  # type: ignore
    evaluator_instance.evaluate_one = AsyncMock(return_value=True)  # type: ignore
    evaluator_instance.to_evaluate_alpha_count = AsyncMock(return_value=0)  # type: ignore
    return evaluator_instance


@pytest.fixture(name="mock_passing_correlation_view")
def mock_passing_correlation_view_fixture() -> TableView:
    """提供一个模拟的表示通过的 TableView (用于相关性)"""
    # 创建真实的 TableSchemaView 实例
    schema = TableSchemaView(
        name="correlation_schema",
        title="Correlation Schema",
        properties=[
            TableSchemaView.Property(
                name="correlation", title="Correlation", data_type="float"
            ),
            TableSchemaView.Property(
                name="alpha_id", title="Alpha ID", data_type="string"
            ),
        ],
    )
    return TableView(
        table_schema=schema,  # 使用真实的 schema
        records=[[0.1, "other_alpha"]],
        max=0.1,
        min=0.1,
    )


@pytest.fixture(name="mock_failing_correlation_view")
def mock_failing_correlation_view_fixture() -> TableView:
    """提供一个模拟的表示失败的 TableView (用于相关性)"""
    # 创建真实的 TableSchemaView 实例
    schema = TableSchemaView(
        name="correlation_schema",
        title="Correlation Schema",
        properties=[
            TableSchemaView.Property(
                name="correlation", title="Correlation", data_type="float"
            ),
            TableSchemaView.Property(
                name="alpha_id", title="Alpha ID", data_type="string"
            ),
        ],
    )
    return TableView(
        table_schema=schema,  # 使用真实的 schema
        records=[[0.9, "other_alpha"]],
        max=0.9,
        min=0.9,
    )


@pytest.fixture(name="mock_existing_check_record")
def mock_existing_check_record_fixture(
    mock_passing_correlation_view: TableView,
) -> CheckRecord:
    """提供一个模拟的现有 CheckRecord"""
    return CheckRecord(
        alpha_id="test_alpha_001",
        record_type=CheckRecordType.CORRELATION_SELF,
        content=mock_passing_correlation_view.model_dump(mode="python"),
    )


@pytest.mark.asyncio
class TestBaseEvaluatorCheckCorrelation:
    """测试 BaseEvaluator._check_correlation 方法"""

    async def test_force_refresh_succeeds_passes(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_passing_correlation_view: TableView,
    ) -> None:
        """测试 FORCE_REFRESH 策略，刷新成功且检查通过"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(return_value=mock_passing_correlation_view),
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )

            assert result is True
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=mock_alpha.alpha_id,
                record_type=CheckRecordType.CORRELATION_SELF,
                order_by=ANY,
            )
            mock_refresh.assert_awaited_once_with(mock_alpha, CorrelationType.SELF)
            mock_determine.assert_called_once_with(
                mock_passing_correlation_view, CorrelationType.SELF
            )

    async def test_force_refresh_succeeds_fails(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_failing_correlation_view: TableView,
    ) -> None:
        """测试 FORCE_REFRESH 策略，刷新成功但检查失败"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(return_value=mock_failing_correlation_view),
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=False),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )

            assert result is False
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once_with(mock_alpha, CorrelationType.SELF)
            mock_determine.assert_called_once_with(
                mock_failing_correlation_view, CorrelationType.SELF
            )

    async def test_force_refresh_fails(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 FORCE_REFRESH 策略，刷新失败"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock(return_value=None)
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )

            assert result is False
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once_with(mock_alpha, CorrelationType.SELF)
            mock_determine.assert_not_called()  # 刷新失败，不应调用判断逻辑

    async def test_refresh_if_missing_record_missing_succeeds_passes(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_passing_correlation_view: TableView,
    ) -> None:
        """测试 REFRESH_ASYNC_IF_MISSING 策略，记录不存在，刷新成功且通过"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(return_value=mock_passing_correlation_view),
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.REFRESH_ASYNC_IF_MISSING
            )

            assert result is True
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once_with(mock_alpha, CorrelationType.SELF)
            mock_determine.assert_called_once()

    async def test_refresh_if_missing_record_exists_passes(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_existing_check_record: CheckRecord,
    ) -> None:
        """测试 REFRESH_ASYNC_IF_MISSING 策略，记录存在，使用现有记录且通过"""
        mock_check_record_dal.find_one_by.return_value = mock_existing_check_record
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.REFRESH_ASYNC_IF_MISSING
            )

            assert result is True
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_not_awaited()  # 记录存在，不应刷新
            mock_determine.assert_called_once()  # 应使用现有记录进行判断

    async def test_use_existing_record_exists_passes(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_existing_check_record: CheckRecord,
    ) -> None:
        """测试 USE_EXISTING 策略，记录存在，使用现有记录且通过"""
        mock_check_record_dal.find_one_by.return_value = mock_existing_check_record
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.USE_EXISTING
            )

            assert result is True
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine.assert_called_once()

    async def test_use_existing_record_missing_fails(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 USE_EXISTING 策略，记录不存在，检查失败"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,  # 补全 with 语句块
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.USE_EXISTING
            )

            assert result is False
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine.assert_not_called()

    async def test_skip_if_missing_record_missing_skips(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试 SKIP_IF_MISSING 策略，记录不存在，跳过检查 (视为失败)"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(  # 修正重复的代码
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.SKIP_IF_MISSING
            )

            assert result is False  # 跳过视为失败
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine.assert_not_called()

    async def test_skip_if_missing_record_exists_passes(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_existing_check_record: CheckRecord,
    ) -> None:
        """测试 SKIP_IF_MISSING 策略，记录存在，使用现有记录且通过"""
        mock_check_record_dal.find_one_by.return_value = mock_existing_check_record
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.SKIP_IF_MISSING
            )

            assert result is True
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_not_awaited()
            mock_determine.assert_called_once()

    async def test_correlation_type_prod(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_passing_correlation_view: TableView,
    ) -> None:
        """测试 CorrelationType.PROD 类型"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(return_value=mock_passing_correlation_view),
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(return_value=True),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.PROD, RefreshPolicy.FORCE_REFRESH
            )

            assert result is True
            # 验证查找记录时使用了正确的 record_type
            mock_check_record_dal.find_one_by.assert_awaited_once_with(
                alpha_id=mock_alpha.alpha_id,
                record_type=CheckRecordType.CORRELATION_PROD,  # 确认类型
                order_by=ANY,
            )
            mock_refresh.assert_awaited_once_with(mock_alpha, CorrelationType.PROD)
            mock_determine.assert_called_once_with(
                mock_passing_correlation_view, CorrelationType.PROD
            )

    async def test_exception_during_refresh(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试刷新过程中发生异常"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(side_effect=ValueError("Refresh failed")),
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )

            assert result is False  # 异常视为失败
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine.assert_not_called()

    async def test_exception_during_determine_status(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
        mock_passing_correlation_view: TableView,
    ) -> None:
        """测试判断状态过程中发生异常"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(return_value=mock_passing_correlation_view),
            ) as mock_refresh,
            patch.object(
                evaluator,
                "_determine_correlation_pass_status",
                MagicMock(side_effect=ValueError("Determine failed")),
            ) as mock_determine,
        ):
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )

            assert result is False  # 异常视为失败
            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine.assert_called_once()  # 确认调用了判断函数

    async def test_cancelled_error_during_refresh(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试刷新过程中发生 CancelledError"""
        mock_check_record_dal.find_one_by.return_value = None
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator,
                "_refresh_correlation_data",
                AsyncMock(side_effect=asyncio.CancelledError),
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,
        ):

            # CancelledError 应该向上冒泡，但 _check_correlation 会捕获并返回 False
            result: bool = await evaluator._check_correlation(
                mock_alpha, CorrelationType.SELF, RefreshPolicy.FORCE_REFRESH
            )
            assert result is False  # 取消视为失败

            mock_check_record_dal.find_one_by.assert_awaited_once()
            mock_refresh.assert_awaited_once()
            mock_determine.assert_not_called()

    async def test_invalid_policy_error(
        self,
        evaluator: BaseEvaluator,
        mock_alpha: Alpha,
        mock_check_record_dal: AsyncMock,
    ) -> None:
        """测试传入无效的 RefreshPolicy"""
        mock_check_record_dal.find_one_by.return_value = None
        invalid_policy: int = 999  # 使用一个无效的枚举值或类型
        mock_refresh: AsyncMock
        mock_determine: MagicMock
        with (
            patch.object(
                evaluator, "_refresh_correlation_data", AsyncMock()
            ) as mock_refresh,
            patch.object(
                evaluator, "_determine_correlation_pass_status", MagicMock()
            ) as mock_determine,
        ):
            # 假设 _determine_check_action 会处理无效策略并返回 ERROR
            # 我们需要模拟 _determine_check_action 的行为
            mock_determine_action: AsyncMock
            with patch.object(
                evaluator,
                "_determine_check_action",
                AsyncMock(return_value=BaseEvaluator.CheckAction.ERROR),
            ) as mock_determine_action:

                result: bool = await evaluator._check_correlation(
                    mock_alpha, CorrelationType.SELF, invalid_policy  # type: ignore
                )

                assert result is False  # ERROR 状态应导致检查失败
                mock_check_record_dal.find_one_by.assert_awaited_once()
                # 确认调用了 _determine_check_action
                mock_determine_action.assert_awaited_once_with(
                    policy=invalid_policy,
                    exist_check_record=None,
                    alpha_id=mock_alpha.alpha_id,
                    check_type_name="相关性",
                )
                # 确认未调用刷新和判断逻辑
                mock_refresh.assert_not_awaited()
                mock_determine.assert_not_called()
