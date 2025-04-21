from typing import Dict, List, Optional

from alphapower.constants import CorrelationType
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL
from alphapower.engine.evaluate.self_correlation_calculator import (
    SelfCorrelationCalculator,
)
from alphapower.entity import Alpha, Correlation
from alphapower.internal.logging import get_logger

log = get_logger(__name__)


class ProdCorrelationEstimator:
    def __init__(
        self,
        alpha_dal: AlphaDAL,
        correlation_dal: CorrelationDAL,
        self_corr_calculator: SelfCorrelationCalculator,
    ) -> None:
        """
        初始化 ProdCorrelationEstimator。

        :param alpha_dal: Alpha 数据访问层实例
        :param correlation_dal: Correlation 数据访问层实例
        :param self_corr_calculator: 自相关性计算器实例
        """
        self.alpha_dal = alpha_dal
        self.correlation_dal = correlation_dal
        self.self_corr_calculator = self_corr_calculator

    async def get_prod_attributes(self) -> Dict[str, float]:
        """
        获取生产环境中所有 Alpha 的 PROD 属性。

        :return: Alpha ID 到其 PROD 属性的映射
        """
        await log.ainfo(event="查询生产环境 Alpha 的 PROD 属性", emoji="🔍")
        prod_correlations: List[Correlation] = await self.correlation_dal.find_by(
            correlation_type=CorrelationType.PROD
        )
        prod_map: Dict[str, float] = {}
        for corr in prod_correlations:
            prod_map[corr.alpha_id_a] = max(
                prod_map.get(corr.alpha_id_a, -1.0), corr.correlation
            )
            prod_map[corr.alpha_id_b] = max(
                prod_map.get(corr.alpha_id_b, -1.0), corr.correlation
            )
        await log.ainfo(
            event="完成生产环境 Alpha 的 PROD 属性查询", count=len(prod_map), emoji="✅"
        )
        return prod_map

    async def estimate_prod_correlation(self, alpha: Alpha) -> Optional[float]:
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
        prod_map = await self.get_prod_attributes()
        if not prod_map:
            await log.awarning(
                event="生产环境中没有 Alpha 的 PROD 属性记录",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        # 计算待估计 Alpha 与生产环境 Alpha 的相关性
        pairwise_correlation = (
            await self.self_corr_calculator.calculate_self_correlation(alpha)
        )
        if not pairwise_correlation:
            await log.awarning(
                event="未能计算 Alpha 与生产环境 Alpha 的相关性",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        # 根据生产环境 Alpha 的 PROD 属性和相关性估算目标 Alpha 的 PROD 属性
        estimated_prod_corr = max(
            pairwise_correlation.get(prod_alpha_id, -1.0) * prod_map[prod_alpha_id]
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
