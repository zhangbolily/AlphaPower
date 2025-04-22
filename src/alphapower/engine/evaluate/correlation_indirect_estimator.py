from math import sqrt
from typing import AsyncGenerator, Dict, List, Optional

from alphapower.constants import CorrelationType, Database, Stage
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL
from alphapower.engine.evaluate.correlation_calculator import (
    CorrelationCalculator,
)
from alphapower.entity import Alpha, Correlation
from alphapower.internal.logging import get_logger

log = get_logger(__name__)


class CorrelationIndirectEstimator:
    def __init__(
        self,
        alpha_dal: AlphaDAL,
        correlation_dal: CorrelationDAL,
        prod_alpha_stream: AsyncGenerator[Alpha],
        corr_calculator: CorrelationCalculator,
    ) -> None:
        """
        初始化 CorrelationIndirectEstimator

        :param alpha_dal: Alpha 数据访问层实例
        :param correlation_dal: Correlation 数据访问层实例
        :param self_corr_calculator: 自相关性计算器实例
        """
        self.alpha_dal = alpha_dal
        self.correlation_dal = correlation_dal
        self.corr_calculator = corr_calculator
        # 强制使用生产环境 Alpha 流计算相关性
        self.prod_alpha_stream = prod_alpha_stream
        self.corr_calculator.alpha_stream = prod_alpha_stream

    async def get_prod_correlations(self) -> Dict[str, float]:
        """
        获取生产环境中所有 Alpha 的 PROD 属性。

        :return: Alpha ID 到其 PROD 属性的映射
        """
        await log.ainfo(event="查询生产环境 Alpha 的 PROD 属性", emoji="🔍")
        prod_alpha_ids: List[str] = [
            alpha.alpha_id async for alpha in self.prod_alpha_stream
        ]

        prod_correlations: List[Correlation] = await self.correlation_dal.find_by(
            in_={
                "alpha_id_a": prod_alpha_ids,
            },
            correlation_type=CorrelationType.PROD,
        )

        prod_correlations.extend(
            await self.correlation_dal.find_by(
                in_={
                    "alpha_id_b": prod_alpha_ids,
                },
                correlation_type=CorrelationType.PROD,
            )
        )

        prod_map: Dict[str, float] = {}
        for corr in prod_correlations:
            prod_map[corr.alpha_id_a] = max(
                prod_map.get(corr.alpha_id_a, 0.0), corr.correlation
            )
            prod_map[corr.alpha_id_b] = max(
                prod_map.get(corr.alpha_id_b, 0.0), corr.correlation
            )
        await log.ainfo(
            event="完成生产环境 Alpha 的相关性查询", count=len(prod_map), emoji="✅"
        )
        return prod_map

    async def estimate_correlation(self, alpha: Alpha) -> Optional[float]:
        """
        预测指定 Alpha 的生产环境相关系数。

        :param alpha: 要预测的 Alpha 实例
        :return: 预测的生产环境相关系数
        """
        await log.ainfo(
            event="开始预测 Alpha 的生产环境相关系数",
            alpha_id=alpha.alpha_id,
            emoji="🔄",
        )

        # 获取生产环境中所有 Alpha 的 PROD 属性
        prod_map = await self.get_prod_correlations()
        if not prod_map:
            await log.awarning(
                event="生产环境中没有 Alpha 的相关性记录",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        # 计算待估计 Alpha 与生产环境 Alpha 的相关性
        pairwise_correlation = await self.corr_calculator.calculate_correlation(alpha)
        if not pairwise_correlation:
            await log.awarning(
                event="未能计算 Alpha 与生产环境 Alpha 的相关性",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        # 使用严格上界公式估算目标 Alpha 的生产环境相关系数
        estimated_prod_corr = max(
            self._calculate_upper_bound(
                pairwise_correlation.get(prod_alpha_id, 0.0),
                prod_map[prod_alpha_id],
            )
            for prod_alpha_id in prod_map.keys()
            if prod_alpha_id in pairwise_correlation
        )

        await log.ainfo(
            event="完成 Alpha 的生产环境相关系数预测",
            alpha_id=alpha.alpha_id,
            estimated_prod_corr=estimated_prod_corr,
            emoji="✅",
        )
        return estimated_prod_corr

    def _calculate_upper_bound(self, rho_ab: float, rho_bc: float) -> float:
        """
        根据严格上界公式计算相关系数的上界。

        :param rho_ab: Alpha 与生产环境 Alpha 的相关系数
        :param rho_bc: 生产环境 Alpha 的 PROD 属性
        :return: 相关系数的上界
        """
        # 上界公式：|rho_ac| <= sqrt((1 - rho_ab^2)(1 - rho_bc^2)) + |rho_ab * rho_bc|
        upper_bound = sqrt((1 - rho_ab**2) * (1 - rho_bc**2)) + abs(rho_ab * rho_bc)
        return upper_bound


if __name__ == "__main__":
    # 运行测试
    from datetime import datetime
    from typing import Dict, Set

    from alphapower.client import wq_client
    from alphapower.constants import SubmissionCheckResult, SubmissionCheckType
    from alphapower.dal.alphas import AggregateDataDAL, AlphaDAL, SettingDAL
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

                    correlation_dal = CorrelationDAL(evaluate_session)
                    record_set_dal = RecordSetDAL(evaluate_session)

                    async def alpha_generator() -> AsyncGenerator[Alpha]:
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

                    alpha_id: str = "7LQXXJZ"
                    alpha: Optional[Alpha] = await alpha_dal.find_by_alpha_id(
                        alpha_id=alpha_id,
                    )

                    if not alpha:
                        await log.ainfo(
                            event="Alpha 策略不存在",
                            alpha_id=alpha_id,
                            emoji="❌",
                        )
                        return

                    correlation_calculator = CorrelationCalculator(
                        client=client,
                        alpha_stream=alpha_generator(),
                        alpha_dal=alpha_dal,
                        record_set_dal=record_set_dal,
                        correlation_dal=correlation_dal,
                    )

                    correlation_estimator = CorrelationIndirectEstimator(
                        alpha_dal=alpha_dal,
                        correlation_dal=correlation_dal,
                        prod_alpha_stream=alpha_generator(),
                        corr_calculator=correlation_calculator,
                    )

    # 运行异步测试函数
    import asyncio

    asyncio.run(test())
