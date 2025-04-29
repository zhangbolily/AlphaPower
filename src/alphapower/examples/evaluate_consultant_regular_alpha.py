from __future__ import annotations  # 解决类型前向引用问题

import asyncio
from typing import AsyncGenerator, Dict, List, Set

from alphapower.constants import (
    CorrelationType,
    Database,
    RefreshPolicy,
    Stage,
    Status,
    SubmissionCheckResult,
    SubmissionCheckType,
)
from alphapower.dal.base import DALFactory
from alphapower.dal.session_manager import session_manager
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import (
    Alpha,
)
from alphapower.internal.logging import get_logger

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)

if __name__ == "__main__":
    # 运行测试
    from datetime import datetime

    from alphapower.client import wq_client
    from alphapower.dal.alphas import AggregateDataDAL, AlphaDAL
    from alphapower.dal.evaluate import (
        CheckRecordDAL,
        CorrelationDAL,
        EvaluateRecordDAL,
        RecordSetDAL,
    )
    from alphapower.engine.evaluate.base_alpha_fetcher import BaseAlphaFetcher
    from alphapower.engine.evaluate.base_evaluate_stages import (
        CorrelationLocalEvaluateStage,
        CorrelationPlatformEvaluateStage,
        InSampleChecksEvaluateStage,
    )
    from alphapower.engine.evaluate.correlation_calculator import (
        CorrelationCalculator,
    )

    async def main() -> None:
        """
        测试 PPAC2025Evaluator 的功能。
        """
        alpha_dal: AlphaDAL = DALFactory.create_dal(dal_class=AlphaDAL)
        aggregate_data_dal: AggregateDataDAL = DALFactory.create_dal(
            dal_class=AggregateDataDAL,
        )
        correlation_dal: CorrelationDAL = DALFactory.create_dal(
            dal_class=CorrelationDAL
        )
        check_record_dal: CheckRecordDAL = DALFactory.create_dal(
            dal_class=CheckRecordDAL
        )
        record_set_dal: RecordSetDAL = DALFactory.create_dal(
            dal_class=RecordSetDAL,
        )
        evaluate_record_dal: EvaluateRecordDAL = DALFactory.create_dal(
            dal_class=EvaluateRecordDAL,
        )

        async with session_manager.get_session(Database.ALPHAS) as session:
            os_alphas: List[Alpha] = await alpha_dal.find_by_stage(
                session=session,
                stage=Stage.OS,
            )

        async def alpha_generator() -> AsyncGenerator[Alpha, None]:
            for alpha in os_alphas:
                yield alpha

        async with wq_client as client:
            correlation_calculator = CorrelationCalculator(
                client=client,
                alpha_stream=alpha_generator(),
                alpha_dal=alpha_dal,
                record_set_dal=record_set_dal,
                correlation_dal=correlation_dal,
                multiprocess=True,
            )
            await correlation_calculator.initialize()

            fetcher = BaseAlphaFetcher(
                alpha_dal=alpha_dal,
                aggregate_data_dal=aggregate_data_dal,
                start_time=datetime(2025, 3, 25),
                end_time=datetime(2025, 4, 28, 23, 59, 59),
                status=Status.UNSUBMITTED,
            )

            # 这几个检查是 WARNING 都要算不通过，没有办法提交生产相关性检查
            check_pass_result_map: Dict[
                SubmissionCheckType, Set[SubmissionCheckResult]
            ] = {
                SubmissionCheckType.SUB_UNIVERSE_SHARPE: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.IS_LADDER_SHARPE: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.LOW_2Y_SHARPE: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.CONCENTRATED_WEIGHT: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.PENDING,
                },
            }

            in_sample_stage: InSampleChecksEvaluateStage = InSampleChecksEvaluateStage(
                client=client,
                next_stage=None,
                check_pass_result_map=check_pass_result_map,
            )
            await in_sample_stage.initialize()

            local_correlation_stage: AbstractEvaluateStage = (
                CorrelationLocalEvaluateStage(
                    next_stage=None,
                    correlation_calculator=correlation_calculator,
                    threshold=0.7,
                )
            )
            platform_prod_correlation_stage: AbstractEvaluateStage = (
                CorrelationPlatformEvaluateStage(
                    next_stage=None,
                    correlation_type=CorrelationType.PROD,
                    check_record_dal=check_record_dal,
                    correlation_dal=correlation_dal,
                    client=client,
                )
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = platform_prod_correlation_stage
            evaluator = BaseEvaluator(
                name="consultant",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
                concurrency=50,
            ):
                print(alpha)

    asyncio.run(main())
