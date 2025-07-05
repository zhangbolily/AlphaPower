from __future__ import annotations  # 解决类型前向引用问题  # 解决类型前向引用问题

import asyncio
import os
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from alphapower.client import WorldQuantClient, wq_client
from alphapower.client.checks_view import SubmissionCheckResultView
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CorrelationType,
    Database,
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
    SubmissionEvaluateStage,
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.correlation_calculator import CorrelationCalculator
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import Alpha, EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.settings import settings
from alphapower.view.alpha import AlphaPropertiesPayload

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class ConsultantInSampleEvaluateStage(InSampleChecksEvaluateStage):

    def __init__(
        self,
        client: WorldQuantClient,
        brain_client: WorldQuantBrainClient,
        next_stage: Optional[AbstractEvaluateStage] = None,
        check_pass_result_map: Optional[
            Dict[SubmissionCheckType, Set[SubmissionCheckResult]]
        ] = None,
    ) -> None:
        """
        初始化 PPAC2025 评估阶段。

        参数:
            next_stage (Optional[AbstractEvaluateStage]): 下一个评估阶段。
            check_pass_result_map (Optional[Dict[SubmissionCheckType, Set[SubmissionCheckResult]]]): 检查通过结果映射。
            client (WorldQuantClient): WorldQuant 客户端。
        """
        super().__init__(
            client=client,
            next_stage=next_stage,
            check_pass_result_map=check_pass_result_map,
        )
        self.brain_client: WorldQuantBrainClient = brain_client

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        if alpha.in_sample and alpha.in_sample.checks:
            for check in alpha.in_sample.checks:
                if (
                    check.name == SubmissionCheckType.MATCHES_PYRAMID.value
                    and check.pyramids
                ):
                    for pyramid in check.pyramids:
                        if (
                            "MODEL" in pyramid.name or "ANALYST" in pyramid.name
                        ) and alpha.region == Region.USA:
                            # 如果因子在模型或分析中，评估失败
                            await log.aerror(
                                event="评估失败，因子在模型或分析中",
                                alpha_id=alpha.alpha_id,
                                pyramid=pyramid.name,
                                emoji="❌",
                            )
                            return False

        result: bool = await super()._evaluate_stage(alpha, policy, record, **kwargs)

        # 顺便更新一下评估记录的其他字段
        if result:
            payload: AlphaPropertiesPayload = AlphaPropertiesPayload(
                name=alpha.alpha_id,
                tags=alpha.tags,
                color=None,
            )

            await self.brain_client.update_alpha_properties(
                alpha_id=alpha.alpha_id,
                payload=payload,
            )

            await self.log.ainfo(
                event="Consultant 初筛通过",
                alpha_id=alpha.alpha_id,
                properties=payload,
                emoji="✅",
            )

        return result


class ConsultantSubmissionEvaluateStage(SubmissionEvaluateStage):
    async def _determine_submission_pass_status(
        self,
        submission_check_view: SubmissionCheckResultView,
        **kwargs: Any,
    ) -> bool:

        if submission_check_view.in_sample is None:
            return False
        if submission_check_view.in_sample.checks is None:
            return False
        if len(submission_check_view.in_sample.checks) == 0:
            return False

        for check in submission_check_view.in_sample.checks:
            if check.name in (
                SubmissionCheckType.POWER_POOL_CORRELATION.value,
                SubmissionCheckType.MATCHES_THEMES.value,
                SubmissionCheckType.MATCHES_COMPETITION.value,
            ):
                continue

            if check.result not in (
                SubmissionCheckResult.PASS,
                SubmissionCheckResult.PENDING,
                SubmissionCheckResult.ERROR,
            ):
                # 如果有任何一个检查不通过，直接返回 False
                await log.aerror(
                    event="提交检查评估失败",
                    check=check.name,
                    result=check.result,
                    emoji="❌",
                )
                return False

        return True


if __name__ == "__main__":
    # 运行测试

    async def main() -> None:
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

        wqb_client: WorldQuantBrainClient = WorldQuantBrainClient(
            username=settings.credential.username,
            password=settings.credential.password,
        )

        user_id: str = await wqb_client.get_user_id()

        await log.ainfo(
            event="评估开始",
            user_id=user_id,
            emoji="🚀",
        )

        async with (
            session_manager.get_session(Database.EVALUATE) as session,
            session.begin(),
        ):
            # 清理旧的评估记录
            deleted: int = await evaluate_record_dal.delete_by_filter(
                session=session,
                evaluator="consultant",
            )
            if deleted > 0:
                await log.ainfo(
                    event="清理旧的评估记录",
                    count=deleted,
                    emoji="🧹",
                )

        async with session_manager.get_session(Database.ALPHAS) as session:
            os_alphas: List[Alpha] = await alpha_dal.find_by(
                Alpha.stage == Stage.OS,
                Alpha.author == user_id,
                session=session,
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
                multiprocess=False,
            )
            await correlation_calculator.initialize()

            fetcher = BaseAlphaFetcher(
                alpha_dal=alpha_dal,
                aggregate_data_dal=aggregate_data_dal,
                start_time=datetime(2025, 3, 17),
                # end_time=datetime(2025, 6, 11, 23, 59, 59),
            )

            check_pass_result_map: Dict[
                SubmissionCheckType, Set[SubmissionCheckResult]
            ] = {
                SubmissionCheckType.UNITS: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.LOW_2Y_SHARPE: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.CONCENTRATED_WEIGHT: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.IS_LADDER_SHARPE: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.MATCHES_THEMES: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.MATCHES_COMPETITION: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
            }

            in_sample_stage: ConsultantInSampleEvaluateStage = (
                ConsultantInSampleEvaluateStage(
                    client=client,
                    brain_client=wqb_client,
                    next_stage=None,
                    check_pass_result_map=check_pass_result_map,
                )
            )
            await in_sample_stage.initialize()

            local_correlation_stage: AbstractEvaluateStage = (
                CorrelationLocalEvaluateStage(
                    next_stage=None,
                    correlation_calculator=correlation_calculator,
                    threshold=0.5,
                )
            )

            platform_prod_correlation_stage: AbstractEvaluateStage = (
                CorrelationPlatformEvaluateStage(
                    next_stage=None,
                    correlation_type=CorrelationType.PROD,
                    check_record_dal=check_record_dal,
                    correlation_dal=correlation_dal,
                    client=wqb_client,
                    threshold=CONSULTANT_MAX_PROD_CORRELATION,
                )
            )

            submission_stage: AbstractEvaluateStage = ConsultantSubmissionEvaluateStage(
                next_stage=None,
                check_record_dal=check_record_dal,
                client=client,
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = submission_stage
            submission_stage.next_stage = platform_prod_correlation_stage
            evaluator = BaseEvaluator(
                name="consultant",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
                concurrency=32,
                status=Status.UNSUBMITTED,
                author=user_id,
            ):
                print(alpha)

    os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(256)

    asyncio.run(main())
