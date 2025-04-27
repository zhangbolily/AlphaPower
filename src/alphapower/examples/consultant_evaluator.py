from __future__ import annotations  # 解决类型前向引用问题

import asyncio
from typing import Any, AsyncGenerator, List

from alphapower.constants import (
    CorrelationType,
    Database,
    RefreshPolicy,
    Stage,
)
from alphapower.dal.base import DALFactory
from alphapower.dal.session_manager import session_manager
from alphapower.engine.evaluate.base_evaluator import BaseEvaluator
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import (
    Alpha,
    EvaluateRecord,
)
from alphapower.internal.logging import get_logger

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class ConsultantEvaluator(BaseEvaluator):
    """
    ConsultantEvaluator 是 BaseEvaluator 的子类，
    专门用于实现 Consultant 相关的 Alpha 评估逻辑。
    """

    _db_lock: asyncio.Lock = asyncio.Lock()

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
            async with (
                session_manager.get_session(Database.EVALUATE) as session,
                session.begin(),
            ):
                await self.evaluate_record_dal.create(session=session, entity=record)
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
        async with (
            session_manager.get_session(Database.EVALUATE) as session,
            session.begin(),
        ):
            await self.evaluate_record_dal.delete_by_filter(
                session=session, alpha_id=alpha.alpha_id
            )

        await log.ainfo(
            event="因子评估失败，评估记录已删除",
            alpha_id=alpha.alpha_id,
            record_id=record.id,
            emoji="❌",
        )


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
            )
            await correlation_calculator.initialize()

            fetcher = BaseAlphaFetcher(
                alpha_dal=alpha_dal,
                aggregate_data_dal=aggregate_data_dal,
                start_time=datetime(2025, 3, 17),
                end_time=datetime(2025, 4, 24, 23, 59, 59),
            )

            in_sample_stage: InSampleChecksEvaluateStage = InSampleChecksEvaluateStage(
                client=client,
                next_stage=None,
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
                    client=client,
                )
            )

            in_sample_stage.next_stage = local_correlation_stage
            local_correlation_stage.next_stage = platform_prod_correlation_stage
            evaluator = BaseEvaluator(
                fetcher=fetcher,
                evaluate_stage_chain=in_sample_stage,
                evaluate_record_dal=evaluate_record_dal,
            )

            async for alpha in evaluator.evaluate_many(
                policy=RefreshPolicy.FORCE_REFRESH, concurrency=1
            ):
                print(alpha)

    asyncio.run(main())
