from datetime import timedelta
from typing import Any, Dict

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.constants import RecordSetType, RefreshPolicy
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.manager.record_sets_manager import RecordSetsManager

from .evaluate_stage_abc import AbstractEvaluateStage

# 不需要自动补全文档


class ScoringEvaluateStage(AbstractEvaluateStage):
    def __init__(
        self,
        next_stage: AbstractEvaluateStage,
        record_sets_manager: RecordSetsManager,
    ) -> None:
        super().__init__(next_stage=next_stage)
        self.record_sets_manager = record_sets_manager
        self.log: BoundLogger = get_logger(
            module_name=f"{__name__}.{self.__class__.__name__}"
        )

    async def return_stability_score(
        self,
        alpha: Alpha,
        yearly_stats_df: pd.DataFrame,
    ) -> Dict[int, float]:

        try:
            # 检查 yearly_stats_df 是否包含 pnl 和 returns 字段，并且这两个字段有非空值
            if (
                "pnl" not in yearly_stats_df.columns
                or "returns" not in yearly_stats_df.columns
            ):
                await self.log.aerror(
                    "ScoringEvaluateStage: 缺少 pnl 或 returns 字段",
                    alpha=alpha,
                    columns=list(yearly_stats_df.columns),
                    emoji="❌",
                )
                raise ValueError("yearly_stats_df 缺少 pnl 或 returns 字段")

            if (
                yearly_stats_df["pnl"].isnull().all()
                or yearly_stats_df["returns"].isnull().all()
            ):
                await self.log.aerror(
                    "ScoringEvaluateStage: pnl 或 returns 字段全为空值",
                    alpha=alpha,
                    pnl_null=yearly_stats_df["pnl"].isnull().sum(),
                    returns_null=yearly_stats_df["returns"].isnull().sum(),
                    emoji="⚠️",
                )
                raise ValueError("pnl 或 returns 字段全为空值")

        except Exception as e:
            await self.log.aerror(
                "ScoringEvaluateStage: Error in getting record sets",
                alpha=alpha,
                error=str(e),
            )
            raise e

        return {}

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:

        try:
            yearly_stats_df: pd.DataFrame = (
                await self.record_sets_manager.get_record_sets(
                    alpha=alpha,
                    record_type=RecordSetType.YEARLY_STATS,
                    allow_local=True,
                    local_expire_time=timedelta(days=30),
                )
            )

            if yearly_stats_df.empty:
                await self.log.aerror(
                    "ScoringEvaluateStage: yearly_stats_df is empty",
                    alpha=alpha,
                )
                raise ValueError("yearly_stats_df is empty")

            # 将 year 字段转换为 datetime 类型，并设置为索引，只保留 pnl 和 returns 字段
            yearly_stats_df["year"] = pd.to_datetime(
                yearly_stats_df["year"], format="%Y"
            )
            yearly_stats_df = yearly_stats_df.set_index("year")

            return False
        except Exception as e:
            await self.log.aerror(
                "ScoringEvaluateStage: Error in getting record sets",
                alpha=alpha,
                error=str(e),
            )
            raise e
