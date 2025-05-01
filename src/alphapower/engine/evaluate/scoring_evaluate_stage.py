from datetime import timedelta
from typing import Any, List, Optional, Tuple  # 增加 Tuple 类型用于类型注解

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.constants import RecordSetType, RefreshPolicy
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.manager.record_sets_manager import RecordSetsManager
from alphapower.view.evaluate import ScoreResult, ScoreResultListAdapter

from .evaluate_stage_abc import AbstractEvaluateStage

# 不需要自动补全文档


class ScoringEvaluateStage(AbstractEvaluateStage):
    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
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
        sharpe_df: pd.DataFrame,
        max_years: int = 5,
    ) -> List[ScoreResult]:
        """
        以年为单位，向过去扩展时间窗口，仅基于 pnl_daily_df 计算每个窗口的稳定性得分。
        只计算 pnl_daily_df 的时间轴，返回每个窗口的起止日期和得分。
        """
        try:
            await self.log.adebug(
                "return_stability_score: 入参（仅使用 pnl_daily_df）",
                alpha=alpha,
                pnl_daily_df_shape=sharpe_df.shape,
                max_years=max_years,
                emoji="🧮",
            )

            await self._validate_stability_score_inputs(alpha, sharpe_df)
            sharpe_df = await self._truncate_nan_and_zero(sharpe_df, alpha)
            complete_years, max_years = await self._get_complete_years_and_max(
                sharpe_df, alpha, max_years
            )
            await self._validate_date_range(sharpe_df, alpha)

            results: List[ScoreResult] = await self._calculate_stability_scores(
                alpha, sharpe_df, complete_years, max_years
            )

            await self.log.adebug(
                "所有窗口得分计算完成",
                result_count=len(results),
                emoji="🎯",
            )
            return results

        except Exception as e:
            await self.log.aerror(
                "return_stability_score: 计算窗口得分异常",
                alpha=alpha,
                error=str(e),
                emoji="💥",
            )
            raise e

    async def _validate_stability_score_inputs(
        self, alpha: Alpha, daily_pnl_df: pd.DataFrame
    ) -> None:
        """
        校验输入参数，确保数据完整性。
        """
        if "sharpe" not in daily_pnl_df.columns:
            await self.log.aerror(
                "pnl_daily_df 缺少必要字段 sharpe",
                alpha=alpha,
                columns_pnl_daily=list(daily_pnl_df.columns),
                emoji="❌",
            )
            raise ValueError("pnl_daily_df 缺少必要字段 sharpe")
        if daily_pnl_df["sharpe"].isnull().all():
            await self.log.aerror(
                "pnl_daily_df 的 sharpe 字段全为 null",
                alpha=alpha,
                emoji="❌",
            )
            raise ValueError("pnl_daily_df 的 sharpe 字段全为 null")
        if not alpha or not alpha.in_sample:
            await self.log.aerror(
                "alpha 对象无效或 in_sample 字段为空",
                alpha=alpha,
                emoji="❌",
            )
            raise ValueError("alpha 对象无效或 in_sample 字段为空")
        if not alpha.in_sample.book_size:
            await self.log.aerror(
                "alpha 对象的 book_size 字段为空",
                alpha=alpha,
                emoji="❌",
            )
            raise ValueError("alpha 对象的 book_size 字段为空")

    async def _truncate_nan_and_zero(
        self, data: pd.DataFrame, alpha: Alpha
    ) -> pd.DataFrame:
        """
        截断前置连续为 0 或 nan 的数据（truncate leading zeros and NaN rows，前置连续为 0 或 nan 的行全部去除）

        参数:
            data (pd.DataFrame): 输入数据，要求包含 'sharpe' 字段
            alpha (Alpha): Alpha 实体对象

        返回值:
            pd.DataFrame: 截断后的 DataFrame

        异常:
            ValueError: 如果全部为 0 或 nan，抛出异常

        说明:
            只保留第一个非 0 且非 nan 之后（含该行）的数据，前置连续为 0 或 nan 的行全部去除。
        """
        sharpe_arr: np.ndarray = data["sharpe"].to_numpy()
        # 判断 nan 或 0（np.isnan/sharpe_arr==0）
        valid_mask: np.ndarray = (~np.isnan(sharpe_arr)) & (sharpe_arr != 0)
        valid_indices: np.ndarray = np.flatnonzero(valid_mask)
        await self.log.adebug(
            "截断前置 nan 或 0 数据，入参",
            alpha=alpha,
            data_shape=data.shape,
            sharpe_arr_preview=sharpe_arr[:10].tolist(),
            emoji="🔍",
        )
        if valid_indices.size > 0:
            first_valid_idx: int = valid_indices[0]
            new_df: pd.DataFrame = data.iloc[first_valid_idx:]
            await self.log.adebug(
                "已截断前置连续为 nan 或 0 的 sharpe 数据",
                first_valid_idx=first_valid_idx,
                new_shape=new_df.shape,
                emoji="✂️",
            )
            return new_df
        else:
            await self.log.awarning(
                "pnl_daily_df 全部为 nan 或 0，无法进行有效评分",
                alpha=alpha,
                emoji="⚠️",
            )
            raise ValueError("pnl_daily_df 全部为 nan 或 0，无法进行有效评分")

    async def _get_complete_years_and_max(
        self, daily_pnl_df: pd.DataFrame, alpha: Alpha, max_years: int
    ) -> Tuple[List[int], int]:
        """
        获取包含完整交易日的自然年列表和最大年数。

        返回值:
            Tuple[List[int], int]: 完整自然年列表和最大年数
        """
        # 索引转为 pd.DatetimeIndex，确保类型一致
        dates: pd.DatetimeIndex = pd.DatetimeIndex(daily_pnl_df.index)
        years: List[int] = sorted(dates.year.unique())  # pylint: disable=E1101
        if len(years) < 1:
            await self.log.aerror(
                "pnl_daily_df 没有有效年份",
                alpha=alpha,
                years=years,
                emoji="❌",
            )
            raise ValueError("pnl_daily_df 没有有效年份")
        complete_years: List[int] = []
        # 统计每年交易日数量，判断是否为完整年
        year_counts: dict[int, int] = {
            year: (dates.year == year).sum() for year in years  # pylint: disable=E1101
        }
        # 以所有年份中最大交易日数为基准，允许 3 天以内的缺失（如节假日等）
        max_trading_days: int = max(year_counts.values())
        for year in years:
            trading_days: int = year_counts[year]
            # 允许最多缺失 3 个交易日
            if trading_days >= max_trading_days - 3:
                complete_years.append(year)
        if complete_years:
            max_years = len(complete_years)
            await self.log.adebug(
                "已根据完整自然年（交易日充足）设置 max_years",
                complete_years=complete_years,
                max_years=max_years,
                max_trading_days=max_trading_days,
                emoji="📅",
            )
        else:
            await self.log.awarning(
                "没有完整自然年（交易日不足），max_years 保持默认",
                years=years,
                emoji="⚠️",
            )
        return complete_years, max_years

    async def _validate_date_range(
        self, daily_pnl_df: pd.DataFrame, alpha: Alpha
    ) -> None:
        """
        校验时间区间有效性。
        """
        start_date: pd.Timestamp = daily_pnl_df.index.min()
        end_date: pd.Timestamp = daily_pnl_df.index.max()
        if start_date > end_date:
            await self.log.aerror(
                "pnl_daily_df 没有有效时间区间",
                alpha=alpha,
                start_date=str(start_date),
                end_date=str(end_date),
                emoji="❌",
            )
            raise ValueError("pnl_daily_df 没有有效时间区间")

    async def _calculate_stability_scores(
        self,
        alpha: Alpha,
        sharpe_df: pd.DataFrame,
        complete_years: list[int],
        max_years: int,
    ) -> List[ScoreResult]:
        """
        按完整自然年滑动窗口计算稳定性得分。

        参数:
            alpha (Alpha): Alpha 实体对象
            sharpe_df (pd.DataFrame): 仅包含 pnl 字段的 DataFrame
            complete_years (list[int]): 完整自然年列表
            max_years (int): 最大年数

        返回值:
            List[ScoreResult]: 每个窗口的稳定性得分结果列表
        """
        results: List[ScoreResult] = []
        try:
            await self.log.adebug(
                "开始计算稳定性得分窗口",
                alpha=alpha,
                complete_years=complete_years,
                max_years=max_years,
                sharpe_df_shape=sharpe_df.shape,
                emoji="🧮",
            )
            for i in range(1, max_years + 1):
                if len(complete_years) < i:
                    await self.log.awarning(
                        "完整自然年数量不足，跳过该窗口",
                        complete_years=complete_years,
                        i=i,
                        emoji="⚠️",
                    )
                    continue
                window_years: List[int] = complete_years[-i:]
                window_start: pd.Timestamp = pd.Timestamp(f"{window_years[0]}-01-01")
                window_end: pd.Timestamp = pd.Timestamp(f"{window_years[-1]}-12-31")
                data_window: pd.DataFrame = sharpe_df.loc[window_start:window_end]

                if data_window.empty:
                    await self.log.awarning(
                        "窗口内数据为空，跳过该窗口",
                        window_start=str(window_start),
                        window_end=str(window_end),
                        window_years=window_years,
                        emoji="⚠️",
                    )
                    continue

                # 计算标准差（standard deviation，标准差越小，数据越稳定）
                data_std: float = float(np.std(data_window["sharpe"].to_numpy()))
                stability_score: float = 1 / (data_std + 1e-6)

                results.append(
                    ScoreResult(
                        start_date=window_start,
                        end_date=window_end,
                        score=stability_score,
                    )
                )
                await self.log.adebug(
                    "窗口得分已计算",
                    window_start=str(window_start),
                    window_end=str(window_end),
                    window_years=window_years,
                    pnl_std=data_std,
                    stability_score=stability_score,
                    emoji="✅",
                )
            await self.log.adebug(
                "全部窗口稳定性得分计算完成",
                result_count=len(results),
                emoji="🎯",
            )
            return results
        except Exception as exc:
            await self.log.aerror(
                "稳定性得分窗口计算异常",
                alpha=alpha,
                error=str(exc),
                emoji="💥",
            )
            raise RuntimeError("稳定性得分窗口计算异常") from exc

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        评估阶段：计算稳定性得分（stability score），并将结果写入 record。
        """
        try:
            await self.log.adebug(
                "ScoringEvaluateStage: _evaluate_stage 入参",
                alpha=alpha,
                policy=str(policy),
                record=record,
                kwargs=kwargs,
                emoji="🧮",
            )

            sharpe_df: pd.DataFrame = await self.record_sets_manager.get_record_sets(
                alpha=alpha,
                set_type=RecordSetType.SHARPE,
                allow_local=True,
                local_expire_time=timedelta(days=30),
            )

            if sharpe_df.empty:
                await self.log.aerror(
                    "ScoringEvaluateStage: sharpe_df 为空",
                    alpha=alpha,
                    emoji="❌",
                )
                raise ValueError("sharpe_df 为空")

            sharpe_df["date"] = pd.to_datetime(sharpe_df["date"], format="%Y-%m-%d")
            sharpe_df = sharpe_df.set_index("date")
            # 只保留 pnl 字段
            if "sharpe" not in sharpe_df.columns:
                await self.log.aerror(
                    "ScoringEvaluateStage: sharpe_df 缺少 sharpe 字段",
                    alpha=alpha,
                    columns=list(sharpe_df.columns),
                    emoji="❌",
                )
                raise ValueError("sharpe_df 缺少 sharpe 字段")
            sharpe_df = sharpe_df[["sharpe"]]

            # 计算稳定性得分
            score_results: List[ScoreResult] = await self.return_stability_score(
                alpha=alpha,
                sharpe_df=sharpe_df,
            )

            await self.log.adebug(
                "ScoringEvaluateStage: 稳定性得分计算完成",
                score_results=[
                    {
                        "start_date": str(r.start_date),
                        "end_date": str(r.end_date),
                        "score": r.score,
                    }
                    for r in score_results
                ],
                emoji="🏅",
            )

            record.score_results = ScoreResultListAdapter.dump_python(
                score_results,
                mode="json",
            )
            await self.log.ainfo(
                "ScoringEvaluateStage: 评估阶段完成",
                alpha=alpha,
                emoji="✅",
            )
            return True
        except Exception as e:
            await self.log.aerror(
                "ScoringEvaluateStage: 获取或计算记录集异常",
                alpha=alpha,
                error=str(e),
                emoji="💥",
            )
            raise e
