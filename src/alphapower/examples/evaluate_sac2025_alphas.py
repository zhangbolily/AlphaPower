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
from alphapower.entity import Alpha, EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.manager.alpha_manager import AbstractAlphaManager, AlphaManager
from alphapower.settings import settings
from alphapower.view.alpha import AlphaPropertiesPayload

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class SACInSampleEvaluateStage(InSampleChecksEvaluateStage):

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
            alpha.selection
            and alpha.selection.code
            and "ACE2023" not in alpha.selection.code
        ):
            await self.log.awarning(
                event="SAC 评估阶段不支持 ACE2023 选择代码",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return False

        result: bool = await super()._evaluate_stage(alpha, policy, record, **kwargs)

        # 顺便更新一下评估记录的其他字段
        selection_description: str = (
            "Select Alpha with low correlation coefficient and fixed turnover rate to make factors have similar attributes, "
            "which is convenient for exploring the impact of other attributes on performance"
        )
        combo_description: str = (
            "This expression weights each Alpha by the maximum correlation with all other selected "
            "Alphas over a 2 year period, with more correlated Alphas receiving less weight."
        )

        if result and (
            (alpha.selection and alpha.selection.description != selection_description)
            or (alpha.combo and alpha.combo.description != combo_description)
            or alpha.name != alpha.alpha_id
        ):
            payload: AlphaPropertiesPayload = AlphaPropertiesPayload(
                name=alpha.alpha_id,
                selection=AlphaPropertiesPayload.Code(
                    description=selection_description,
                ),
                combo=AlphaPropertiesPayload.Code(
                    description=combo_description,
                ),
                tags=alpha.tags,
            )

            await self.alpha_manager.save_alpha_properties(
                alpha=alpha,
                properties=payload,
            )

            await self.log.ainfo(
                event="SAC 初筛通过，更新专用描述",
                alpha_id=alpha.alpha_id,
                properties=payload,
                emoji="✅",
            )

        return result


if __name__ == "__main__":

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
        brain_client: WorldQuantBrainClient = WorldQuantBrainClient(
            username=settings.credential.username,
            password=settings.credential.password,
        )
        alpha_manager: AbstractAlphaManager = AlphaManager(
            brain_client=brain_client,
        )

        user_id: str = await brain_client.get_user_id()
        async with (
            session_manager.get_session(Database.EVALUATE) as session,
            session.begin(),
        ):
            # 清理旧的评估记录
            deleted: int = await evaluate_record_dal.delete_by_filter(
                session=session,
                evaluator="sac",
                author=user_id,
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
                power_pool_eligible: bool = False
                for classification in alpha.classifications:
                    if classification.id.startswith("POWER_POOL"):
                        # 不处理 Power Pool 的因子
                        power_pool_eligible = True
                        break
                if not power_pool_eligible:
                    yield alpha
                else:
                    await log.ainfo(
                        event="跳过 Power Pool 因子",
                        alpha_id=alpha.alpha_id,
                        emoji="⏭️",
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
                start_time=datetime(2025, 6, 1, 0, 0),
            )

            check_pass_result_map: Dict[
                SubmissionCheckType, Set[SubmissionCheckResult]
            ] = {}

            in_sample_stage: SACInSampleEvaluateStage = SACInSampleEvaluateStage(
                client=client,
                next_stage=None,
                alpha_manager=alpha_manager,
                check_pass_result_map=check_pass_result_map,
            )
            await in_sample_stage.initialize()

            local_correlation_stage: AbstractEvaluateStage = (
                CorrelationLocalEvaluateStage(
                    next_stage=None,
                    correlation_calculator=correlation_calculator,
                    threshold=0.7,
                    same_region=True,
                    inner=False,
                )
            )
            platform_self_correlation_stage: AbstractEvaluateStage = (
                CorrelationPlatformEvaluateStage(
                    next_stage=None,
                    correlation_type=CorrelationType.PROD,
                    check_record_dal=check_record_dal,
                    correlation_dal=correlation_dal,
                    client=brain_client,
                    threshold=0.7,
                )
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = platform_self_correlation_stage

            evaluator = BaseEvaluator(
                name="sac",
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.REFRESH_ASYNC_IF_MISSING,
                concurrency=3,
                type=AlphaType.SUPER,
                status=Status.UNSUBMITTED,
                author=user_id,
            ):
                print(alpha)

    os.environ["PYTHONASYNCIO_MAX_WORKERS"] = str(256)

    asyncio.run(main())
