from __future__ import annotations  # 解决类型前向引用问题

import asyncio
import os
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from alphapower.client import BeforeAndAfterPerformanceView, WorldQuantClient, wq_client
from alphapower.client.models import AlphaPropertiesPayload
from alphapower.constants import (
    CorrelationType,
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
    CheckRecordDAL,
    CorrelationDAL,
    EvaluateRecordDAL,
    RecordSetDAL,
)
from alphapower.dal.session_manager import session_manager
from alphapower.engine.evaluate.base_alpha_fetcher import BaseAlphaFetcher
from alphapower.engine.evaluate.base_evaluate_stages import (
    CorrelationLocalEvaluateStage,
    CorrelationPlatformEvaluateStage,
    InSampleChecksEvaluateStage,
    PerformanceDiffEvaluateStage,
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.correlation_calculator import (
    CorrelationCalculator,
)
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.engine.evaluate.scoring_evaluate_stage import ScoringEvaluateStage
from alphapower.entity import (
    Alpha,
    EvaluateRecord,
)
from alphapower.internal.logging import get_logger
from alphapower.manager.record_sets_manager import RecordSetsManager

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class PPAC2025PerfDiffEvaluateStage(PerformanceDiffEvaluateStage):

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ) -> None:
        """
        初始化 PPAC2025 评估阶段。

        参数:
            next_stage (Optional[AbstractEvaluateStage]): 下一个评估阶段。
            competition_id (Optional[str]): 竞赛 ID。
            check_record_dal (CheckRecordDAL): 检查记录数据访问层。
            client (WorldQuantClient): WorldQuant 客户端。
        """
        competition_id = "PPAC2025"
        super().__init__(next_stage, competition_id, check_record_dal, client)

    async def _determine_performance_diff_pass_status(
        self,
        alpha: Alpha,
        perf_diff_view: BeforeAndAfterPerformanceView,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        if alpha.regular is None or alpha.regular.operator_count is None:
            # 如果因子没有 regular 属性，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，因子没有有效的 regular 属性",
                alpha_id=alpha.alpha_id,
                regular=alpha.regular,
                emoji="❌",
            )
            return False

        if alpha.region != Region.USA or alpha.delay != Delay.ONE:
            # 如果因子不在美国市场或延迟不是 1，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，因子不在美国市场或延迟不是 1",
                alpha_id=alpha.alpha_id,
                region=alpha.region,
                delay=alpha.delay,
                emoji="❌",
            )
            return False

        if alpha.regular.operator_count > 8:
            # 如果因子操作数超过 8，评估失败
            await log.aerror(
                event="PPAC2025 评估失败",
                alpha_id=alpha.alpha_id,
                operator_count=alpha.regular.operator_count,
                emoji="❌",
            )
            return False

        if perf_diff_view.score is None:
            # 如果没有分数，无法比较竞赛业绩
            await log.aerror(
                event="PPAC2025 评估失败，没有分数",
                alpha_id=alpha.alpha_id,
                emoji="❌",
            )
            return False

        record.score_diff = perf_diff_view.score.after - perf_diff_view.score.before
        await log.ainfo(
            event="PPAC2025 评估成功",
            alpha_id=alpha.alpha_id,
            score_diff=record.score_diff,
            before_score=perf_diff_view.score.before,
            after_score=perf_diff_view.score.after,
            emoji="✅",
        )

        # 顺便更新一下评估记录的其他字段
        async with self.client as client:
            payload: AlphaPropertiesPayload = AlphaPropertiesPayload(
                regular=AlphaPropertiesPayload.Regular(
                    description=(
                        "Idea: Power Pool Alphas Competition 2025\n"
                        "Rationale for data used: Power Pool Alphas Competition 2025\n"
                        "Rationale for operators used: Power Pool Alphas Competition 2025"
                    ),
                ),
                tags=alpha.tags,
            )

            await client.alpha_update_properties(
                alpha_id=alpha.alpha_id, properties=payload
            )

            await self.log.ainfo(
                event="PPAC2025 评估成功，更新因子属性",
                alpha_id=alpha.alpha_id,
                properties=payload,
                emoji="✅",
            )

        return True


if __name__ == "__main__":

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
                end_time=datetime(2025, 5, 2, 23, 59, 59),
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

            local_correlation_stage: AbstractEvaluateStage = (
                CorrelationLocalEvaluateStage(
                    next_stage=None,
                    correlation_calculator=correlation_calculator,
                    threshold=0.5,
                )
            )
            platform_self_correlation_stage: AbstractEvaluateStage = (
                CorrelationPlatformEvaluateStage(
                    next_stage=None,
                    correlation_type=CorrelationType.SELF,
                    check_record_dal=check_record_dal,
                    correlation_dal=correlation_dal,
                    client=client,
                )
            )
            perf_diff_stage: AbstractEvaluateStage = PPAC2025PerfDiffEvaluateStage(
                next_stage=None,
                check_record_dal=check_record_dal,
                client=client,
            )
            scoring_stage: ScoringEvaluateStage = ScoringEvaluateStage(
                next_stage=None,
                record_sets_manager=record_set_manager,
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = (
                perf_diff_stage  # TODO: 自相关性计算直接用本地的数据，否则太慢了
            )
            platform_self_correlation_stage.next_stage = perf_diff_stage
            perf_diff_stage.next_stage = scoring_stage

            evaluator = BaseEvaluator(
                name="ppac2025",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.FORCE_REFRESH,
                concurrency=256,
                status=Status.UNSUBMITTED,
                region=Region.USA,
                delay=Delay.ONE,
            ):
                print(alpha)

    os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(256)

    asyncio.run(main())
