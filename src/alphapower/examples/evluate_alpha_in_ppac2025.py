from __future__ import annotations  # 解决类型前向引用问题

import asyncio
import os
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Set

from alphapower.client import wq_client
from alphapower.constants import (
    Database,
    Delay,
    RefreshPolicy,
    Region,
    Stage,
    Status,
    SubmissionCheckResult,
    SubmissionCheckType,
)
from alphapower.dal.alphas import AggregateDataDAL, AlphaDAL
from alphapower.dal.base import DALFactory
from alphapower.dal.evaluate import (
    CorrelationDAL,
    EvaluateRecordDAL,
    RecordSetDAL,
)
from alphapower.dal.session_manager import session_manager
from alphapower.engine.evaluate.base_alpha_fetcher import BaseAlphaFetcher
from alphapower.engine.evaluate.base_evaluate_stages import (
    InSampleChecksEvaluateStage,
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.correlation_calculator import (
    CorrelationCalculator,
)
from alphapower.engine.evaluate.scoring_evaluate_stage import ScoringEvaluateStage
from alphapower.entity import (
    Alpha,
)
from alphapower.internal.logging import get_logger
from alphapower.manager.record_sets_manager import RecordSetsManager

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


if __name__ == "__main__":
    # 运行测试

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
                for classification in alpha.classifications:
                    if classification.id == "POWER_POOL:POWER_POOL_ELIGIBLE":
                        await log.ainfo(
                            event="Alpha 策略符合 Power Pool 条件",
                            alpha_id=alpha.alpha_id,
                            classifications=alpha.classifications,
                            emoji="✅",
                        )
                        yield alpha

                await log.ainfo(
                    event="Alpha 策略不符合 Power Pool 条件",
                    alpha_id=alpha.alpha_id,
                    classifications=alpha.classifications,
                    emoji="❌",
                )

        async with wq_client as client:
            correlation_calculator = CorrelationCalculator(
                client=client,
                alpha_stream=alpha_generator(),
                alpha_dal=alpha_dal,
                record_set_dal=record_set_dal,
                correlation_dal=correlation_dal,
            )
            await correlation_calculator.initialize()

            fetcher = BaseAlphaFetcher(
                alpha_dal=alpha_dal,
                aggregate_data_dal=aggregate_data_dal,
                start_time=datetime(2025, 3, 16),
                end_time=datetime(2025, 4, 30, 23, 59, 59),
            )

            record_set_manager: RecordSetsManager = RecordSetsManager(
                client=client,
                record_set_dal=record_set_dal,
            )

            check_pass_result_map: Dict[
                SubmissionCheckType, Set[SubmissionCheckResult]
            ] = {
                SubmissionCheckType.MATCHES_COMPETITION: {
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

            scoring_stage: ScoringEvaluateStage = ScoringEvaluateStage(
                next_stage=None,
                record_sets_manager=record_set_manager,
            )

            in_sample_stage.next_stage = scoring_stage

            evaluator = BaseEvaluator(
                name="power_pool",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            evaluate_result_stream = evaluator.evaluate_many(
                policy=RefreshPolicy.FORCE_REFRESH,
                concurrency=48,
                status=Status.ACTIVE,
                region=Region.USA,
                delay=Delay.ONE,
            )

            async for alpha in evaluate_result_stream:
                print(alpha)

    os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(64)

    asyncio.run(main())
