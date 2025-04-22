from __future__ import annotations  # 解决类型前向引用问题

from typing import Any, AsyncGenerator, Optional

from alphapower.client import BeforeAndAfterPerformanceView, WorldQuantClient
from alphapower.constants import (
    CorrelationType,
    Database,
    Delay,
    RefreshPolicy,
    Region,
    Stage,
)
from alphapower.engine.evaluate.base_evaluate_stages import PerformanceDiffEvaluateStage
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import Alpha, EvaluateRecord
from alphapower.internal.logging import get_logger

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class PPAC2025Evaluator(BaseEvaluator):
    """
    PPAC2025Evaluator 是 BaseEvaluator 的子类，
    专门用于实现 PPAC2025 相关的 Alpha 评估逻辑。
    """

    async def _handle_evaluate_success(
        self, alpha: Alpha, record: EvaluateRecord, **kwargs: Any
    ) -> None:
        """
        处理评估成功的逻辑。

        参数:
            alpha (Alpha): 被评估的因子对象。
            record (EvaluateRecord): 评估记录对象。
            kwargs (Any): 额外参数。
        """
        await self._log_evaluate_success(alpha, record)
        await self._create_evaluate_record(record)

    async def _log_evaluate_success(self, alpha: Alpha, record: EvaluateRecord) -> None:
        """
        记录评估成功的日志。

        参数:
            alpha (Alpha): 被评估的因子对象。
            record (EvaluateRecord): 评估记录对象。
        """
        await log.ainfo(
            event="因子评估成功",
            alpha_id=alpha.id,
            record_id=record.id,
            emoji="✅",
        )

    async def _create_evaluate_record(self, record: EvaluateRecord) -> None:
        """
        创建评估记录。

        参数:
            record (EvaluateRecord): 评估记录对象。
        """
        try:
            await self.evaluate_record_dal.create(record)
            # FIXME: 这里由于没有 commit 操作，所以可能会导致数据不一致
            await self.evaluate_record_dal.session.commit()
            await log.ainfo(
                event="因子评估记录创建成功",
                record_id=record.id,
                emoji="📄",
            )
        except Exception as e:
            await log.aerror(
                event="因子评估记录创建失败",
                record_id=record.id,
                error=str(e),
                emoji="❌",
            )
            raise e

    async def _handle_evaluate_failure(
        self, alpha: Alpha, record: EvaluateRecord, **kwargs: Any
    ) -> None:
        """
        处理评估失败的逻辑。

        参数:
            alpha (Alpha): 被评估的因子对象。
            record (EvaluateRecord): 评估记录对象。
            kwargs (Any): 额外参数。
        """
        await self.evaluate_record_dal.delete_by_filter(alpha_id=alpha.alpha_id)
        await self.evaluate_record_dal.session.commit()

        await log.ainfo(
            event="因子评估失败，评估记录已删除",
            alpha_id=alpha.alpha_id,
            record_id=record.id,
            emoji="❌",
        )


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

        if alpha.settings is None:
            # 如果因子没有设置，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，因子没有设置",
                alpha_id=alpha.alpha_id,
                settings=alpha.settings,
                emoji="❌",
            )
            return False

        if alpha.settings.region != Region.USA or alpha.settings.delay != Delay.ONE:
            # 如果因子不在美国市场或延迟不是 1，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，因子不在美国市场或延迟不是 1",
                alpha_id=alpha.alpha_id,
                region=alpha.settings.region,
                delay=alpha.settings.delay,
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

        if perf_diff_view.score.after < perf_diff_view.score.before:
            # 如果竞赛业绩下降，评估失败
            await log.aerror(
                event="PPAC2025 评估失败，竞赛业绩下降",
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

        return True


if __name__ == "__main__":
    # 运行测试
    from datetime import datetime
    from typing import Dict, Set

    from alphapower.client import wq_client
    from alphapower.constants import SubmissionCheckResult, SubmissionCheckType
    from alphapower.dal.alphas import AlphaDAL, SampleDAL, SettingDAL
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
    from alphapower.internal.db_session import get_db_session

    async def test() -> None:
        """
        测试 PPAC2025Evaluator 的功能。
        """
        async with get_db_session(Database.ALPHAS) as alpha_session:
            async with get_db_session(Database.EVALUATE) as evaluate_session:
                async with wq_client as client:
                    alpha_dal = AlphaDAL(alpha_session)
                    setting_dal = SettingDAL(alpha_session)
                    sample_dal = SampleDAL(alpha_session)

                    correlation_dal = CorrelationDAL(evaluate_session)
                    check_record_dal = CheckRecordDAL(evaluate_session)
                    record_set_dal = RecordSetDAL(evaluate_session)
                    evaluate_record_dal = EvaluateRecordDAL(evaluate_session)

                    async def alpha_generator() -> AsyncGenerator[Alpha, None]:
                        for alpha in await alpha_dal.find_by_stage(
                            stage=Stage.OS,
                        ):
                            for classification in alpha.classifications:
                                if (
                                    classification.classification_id
                                    == "POWER_POOL:POWER_POOL_ELIGIBLE"
                                ):
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

                    correlation_calculator = CorrelationCalculator(
                        client=client,
                        alpha_stream=alpha_generator(),
                        alpha_dal=alpha_dal,
                        record_set_dal=record_set_dal,
                        correlation_dal=correlation_dal,
                    )

                    fetcher = BaseAlphaFetcher(
                        alpha_dal=alpha_dal,
                        setting_dal=setting_dal,
                        sample_dal=sample_dal,
                        start_time=datetime(2025, 2, 21),
                        end_time=datetime(2025, 4, 14, 23, 59, 59),
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

                    in_sample_stage: AbstractEvaluateStage = (
                        InSampleChecksEvaluateStage(
                            next_stage=None,
                            check_pass_result_map=check_pass_result_map,
                        )
                    )

                    local_correlation_stage: AbstractEvaluateStage = (
                        CorrelationLocalEvaluateStage(
                            next_stage=None,
                            correlation_calculator=correlation_calculator,
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
                    perf_diff_stage: AbstractEvaluateStage = (
                        PPAC2025PerfDiffEvaluateStage(
                            next_stage=None,
                            check_record_dal=check_record_dal,
                            client=client,
                        )
                    )

                    in_sample_stage.next_stage = local_correlation_stage
                    local_correlation_stage.next_stage = perf_diff_stage  # TODO: 自相关性计算直接用本地的数据，否则太慢了
                    platform_self_correlation_stage.next_stage = perf_diff_stage

                    evaluator = PPAC2025Evaluator(
                        fetcher=fetcher,
                        evaluate_stage_chain=in_sample_stage,
                        evaluate_record_dal=evaluate_record_dal,
                    )

                    async for alpha in evaluator.evaluate_many(
                        policy=RefreshPolicy.FORCE_REFRESH, concurrency=1
                    ):
                        print(alpha)

    # 运行异步测试函数
    import asyncio

    asyncio.run(test())
