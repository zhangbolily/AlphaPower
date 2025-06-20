from __future__ import annotations  # 解决类型前向引用问题

import asyncio
import os
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from alphapower.client import WorldQuantClient, wq_client
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import (
    AlphaType,
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
)
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.correlation_calculator import CorrelationCalculator
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.engine.evaluate.scoring_evaluate_stage import ScoringEvaluateStage
from alphapower.entity import Alpha, EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.manager.alpha_manager import AlphaManager
from alphapower.manager.alpha_manager_abc import AbstractAlphaManager
from alphapower.manager.record_sets_manager import RecordSetsManager
from alphapower.settings import settings
from alphapower.view.alpha import AlphaPropertiesPayload

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class PPAC2025InSampleEvaluateStage(InSampleChecksEvaluateStage):

    def __init__(
        self,
        client: WorldQuantClient,
        alpha_manager: AbstractAlphaManager,
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
        self.alpha_manager: AbstractAlphaManager = alpha_manager

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        if (
            alpha.regular
            and alpha.regular.operator_count is not None
            and alpha.regular.operator_count > 8
        ):
            # 如果因子使用的运算符超过 8 个，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，因子使用的运算符超过 8 个",
                alpha_id=alpha.alpha_id,
                operator_count=alpha.regular.operator_count,
                emoji="❌",
            )
            return False

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
                                event="PPAC2025 评估失败，因子在模型或分析中",
                                alpha_id=alpha.alpha_id,
                                pyramid=pyramid.name,
                                emoji="❌",
                            )
                            return False

                if check.name == SubmissionCheckType.LOW_2Y_SHARPE.value:
                    if (
                        check.result == SubmissionCheckResult.WARNING
                        and check.value is not None
                        and check.value < 0.5
                    ):
                        # 如果 2 年夏普率低于 0.5，评估失败
                        await log.aerror(
                            event="PPAC2025 评估失败，2 年夏普率低于 0.5",
                            alpha_id=alpha.alpha_id,
                            value=check.value,
                            emoji="❌",
                        )
                        return False

        result: bool = await super()._evaluate_stage(alpha, policy, record, **kwargs)

        # 顺便更新一下评估记录的其他字段
        ppac_description: str = (
            "Idea: Power Pool Alphas Competition 2025\n"
            "Rationale for data used: Power Pool Alphas Competition 2025\n"
            "Rationale for operators used: Power Pool Alphas Competition 2025"
        )

        if result and (
            (alpha.regular and alpha.regular.description != ppac_description)
            or alpha.name != alpha.alpha_id
        ):
            payload: AlphaPropertiesPayload = AlphaPropertiesPayload(
                name=alpha.alpha_id,
                regular=AlphaPropertiesPayload.Code(
                    description=ppac_description,
                ),
                tags=alpha.tags,
            )

            await self.alpha_manager.save_alpha_properties(
                alpha=alpha,
                properties=payload,
            )

            await self.log.ainfo(
                event="PPAC2025 初筛通过，更新 Power Pool 专用描述",
                alpha_id=alpha.alpha_id,
                properties=payload,
                emoji="✅",
            )

        return result


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
        brain_client: WorldQuantBrainClient = WorldQuantBrainClient(
            username=settings.credential.username,
            password=settings.credential.password,
        )
        alpha_manager: AbstractAlphaManager = AlphaManager(
            brain_client=brain_client,
        )

        user_id: str = await brain_client.get_user_id()
        await log.ainfo(
            event="PPAC2025 评估开始",
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
                evaluator="ppac2025",
                author=user_id,
            )
            if deleted > 0:
                await log.ainfo(
                    event="清理旧的评估记录",
                    count=deleted,
                    emoji="🧹",
                )

        async with session_manager.get_session(Database.ALPHAS) as session:

            os_alphas: List[Alpha] = await alpha_dal.find_by_stage(
                session=session,
                stage=Stage.OS,
            )

        async def alpha_generator() -> AsyncGenerator[Alpha, None]:
            for alpha in os_alphas:
                for classification in alpha.classifications:
                    if classification.id.startswith("POWER_POOL"):
                        # 仅处理 Power Pool 的因子
                        yield alpha
                        break
                else:
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
                start_time=datetime(2025, 3, 17, 0, 0),
                # end_time=datetime(2025, 6, 14, 23, 59, 59),
            )

            record_set_manager: RecordSetsManager = RecordSetsManager(
                brain_client=brain_client,
                record_set_dal=record_set_dal,
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
                SubmissionCheckType.MATCHES_COMPETITION: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
                SubmissionCheckType.MATCHES_THEMES: {
                    SubmissionCheckResult.PASS,
                    SubmissionCheckResult.WARNING,
                    SubmissionCheckResult.PENDING,
                },
            }

            in_sample_stage: PPAC2025InSampleEvaluateStage = (
                PPAC2025InSampleEvaluateStage(
                    client=client,
                    next_stage=None,
                    alpha_manager=alpha_manager,
                    check_pass_result_map=check_pass_result_map,
                )
            )
            await in_sample_stage.initialize()

            local_correlation_stage: AbstractEvaluateStage = (
                CorrelationLocalEvaluateStage(
                    next_stage=None,
                    correlation_calculator=correlation_calculator,
                    threshold=0.5,
                    same_region=True,
                    inner=False,
                )
            )
            platform_self_correlation_stage: AbstractEvaluateStage = (
                CorrelationPlatformEvaluateStage(
                    next_stage=None,
                    correlation_type=CorrelationType.POWER_POOL,
                    check_record_dal=check_record_dal,
                    correlation_dal=correlation_dal,
                    client=brain_client,
                    threshold=0.5,
                )
            )

            scoring_stage: ScoringEvaluateStage = ScoringEvaluateStage(
                next_stage=None,
                record_sets_manager=record_set_manager,
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = platform_self_correlation_stage
            platform_self_correlation_stage.next_stage = scoring_stage

            evaluator = BaseEvaluator(
                name="ppac2025",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
                concurrency=32,
                status=Status.UNSUBMITTED,
                type=AlphaType.REGULAR,
                author=user_id,
            ):
                print(alpha)

    os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(256)

    asyncio.run(main())
