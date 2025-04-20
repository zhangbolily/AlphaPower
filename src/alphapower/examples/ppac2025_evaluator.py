from __future__ import annotations  # 解决类型前向引用问题

from typing import Any, Dict, List, Tuple

from alphapower.client import BeforeAndAfterPerformanceView
from alphapower.constants import (
    CheckRecordType,
    Database,
    RefreshPolicy,
    SampleCheckResult,
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.entity import Alpha
from alphapower.internal.logging import get_logger

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class PPAC2025Evaluator(BaseEvaluator):
    """
    ConsultantEvaluator 是 BaseEvaluator 的子类，
    专门用于实现顾问相关的 Alpha 评估逻辑。
    """

    async def _get_checks_to_run(
        self, alpha: Alpha, **kwargs: Any
    ) -> Tuple[List[CheckRecordType], Dict[str, Any], RefreshPolicy]:
        competitions, result = await self.matched_competitions(alpha=alpha)
        if result in (SampleCheckResult.PASS, SampleCheckResult.PENDING):
            for competition in competitions:
                if competition.id == "PPAC2025":
                    return (
                        [
                            CheckRecordType.CORRELATION_SELF,
                            CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                        ],
                        {"competition_id": competition.id},
                        RefreshPolicy.FORCE_REFRESH,
                    )

        return (
            [],
            {},
            RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
        )

    async def _determine_performance_diff_pass_status(
        self,
        alpha: Alpha,
        perf_diff_view: BeforeAndAfterPerformanceView,
        **kwargs: Any,
    ) -> bool:
        if perf_diff_view.score is None:
            return False

        if perf_diff_view.score.after < perf_diff_view.score.before:
            return False

        return True

    async def _handle_evaluate_success(
        self, alpha, checks_ctx, checks, **kwargs
    ) -> None:
        return await super()._handle_evaluate_success(
            alpha, checks_ctx, checks, **kwargs
        )

    async def _handle_evaluate_failure(
        self, alpha, checks_ctx, checks, **kwargs
    ) -> None:
        return await super()._handle_evaluate_failure(
            alpha, checks_ctx, checks, **kwargs
        )


if __name__ == "__main__":
    # 运行测试
    from alphapower.client import wq_client
    from alphapower.dal.alphas import AlphaDAL, SampleDAL, SettingDAL
    from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL, RecordSetDAL
    from alphapower.engine.evaluate.base_alpha_fetcher import BaseAlphaFetcher
    from alphapower.engine.evaluate.self_correlation_calculator import (
        SelfCorrelationCalculator,
    )
    from alphapower.internal.db_session import get_db_session

    async def test() -> None:
        async with get_db_session(Database.ALPHAS) as alpha_session:
            async with get_db_session(Database.EVALUATE) as evaluate_session:
                async with wq_client as client:
                    alpha_dal = AlphaDAL(alpha_session)
                    setting_dal = SettingDAL(alpha_session)
                    sample_dal = SampleDAL(alpha_session)

                    correlation_dal = CorrelationDAL(evaluate_session)
                    check_record_dal = CheckRecordDAL(evaluate_session)
                    record_set_dal = RecordSetDAL(evaluate_session)

                    correlation_calculator = SelfCorrelationCalculator(
                        client=client,
                        alpha_dal=alpha_dal,
                        record_set_dal=record_set_dal,
                        correlation_dal=correlation_dal,
                    )

                    fetcher = BaseAlphaFetcher(
                        alpha_dal=alpha_dal,
                        setting_dal=setting_dal,
                        sample_dal=sample_dal,
                    )
                    evaluator = PPAC2025Evaluator(
                        fetcher=fetcher,
                        correlation_dal=correlation_dal,
                        check_record_dal=check_record_dal,
                        client=client,
                        correlation_calculator=correlation_calculator,
                    )

                    async for alpha in evaluator.evaluate_many(
                        policy=RefreshPolicy.USE_EXISTING, concurrency=1
                    ):
                        print(alpha)

    # 运行异步测试函数
    import asyncio

    asyncio.run(test())
