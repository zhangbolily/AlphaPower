from math import sqrt
from typing import AsyncGenerator, Dict, List, Optional

from structlog.stdlib import BoundLogger

from alphapower.constants import CorrelationType, Database, Stage
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL
from alphapower.engine.evaluate.correlation_calculator import (
    CorrelationCalculator,
)
from alphapower.entity import Alpha, Correlation
from alphapower.internal.logging import get_logger


class CorrelationIndirectEstimator:
    def __init__(
        self,
        alpha_dal: AlphaDAL,
        correlation_dal: CorrelationDAL,
        prod_alpha_stream: AsyncGenerator[Alpha, None],
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
        self.log: BoundLogger = get_logger(self.__class__.__name__)

    async def get_prod_correlations(self) -> Dict[str, float]:
        """
        获取生产环境中所有 Alpha 的 PROD 属性。

        :return: Alpha ID 到其 PROD 属性的映射
        """
        await self.log.ainfo(event="查询生产环境 Alpha 的 PROD 属性", emoji="🔍")
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
        await self.log.ainfo(
            event="完成生产环境 Alpha 的相关性查询", count=len(prod_map), emoji="✅"
        )
        return prod_map

    async def estimate_correlation(self, alpha: Alpha) -> Optional[float]:
        """
        预测指定 Alpha 的生产环境相关系数。

        :param alpha: 要预测的 Alpha 实例
        :return: 预测的生产环境相关系数
        """
        await self.log.ainfo(
            event="开始预测 Alpha 的生产环境相关系数",
            alpha_id=alpha.alpha_id,
            emoji="🔄",
        )

        # 获取生产环境中所有 Alpha 的 PROD 属性
        prod_map = await self.get_prod_correlations()
        if not prod_map:
            await self.log.awarning(
                event="生产环境中没有 Alpha 的相关性记录",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        # 计算待估计 Alpha 与生产环境 Alpha 的相关性
        alpha_corr_map = await self.corr_calculator.calculate_correlation(alpha)
        if not alpha_corr_map:
            await self.log.awarning(
                event="未能计算 Alpha 与生产环境 Alpha 的相关性",
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return None

        alpha_id_corr_map: Dict[str, float] = {
            alpha.alpha_id: corr for alpha, corr in alpha_corr_map.items()
        }

        # 使用严格上界公式估算目标 Alpha 的生产环境相关系数
        estimated_prod_corr = max(
            self._calculate_upper_bound(
                alpha_id_corr_map.get(prod_alpha_id, 0.0),
                prod_map[prod_alpha_id],
            )
            for prod_alpha_id in prod_map.keys()
            if prod_alpha_id in alpha_id_corr_map
        )

        await self.log.ainfo(
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
        # 兜底系数绝对值略微大于 1 的问题
        rho_ab = min(max(rho_ab, -1.0), 1.0)
        rho_bc = min(max(rho_bc, -1.0), 1.0)

        # 上界公式：rho_ac <= |rho_ab * rho_bc| + sqrt((1 - rho_ab^2)(1 - rho_bc^2))
        upper_bound = abs(rho_ab * rho_bc) + sqrt((1 - rho_ab**2) * (1 - rho_bc**2))
        return upper_bound

    def _calculate_lower_bound(self, rho_ab: float, rho_bc: float) -> float:
        """
        根据严格下界公式计算相关系数的下界。

        :param rho_ab: Alpha 与生产环境 Alpha 的相关系数
        :param rho_bc: 生产环境 Alpha 的 PROD 属性
        :return: 相关系数的下界
        """
        # 兜底系数绝对值略微大于 1 的问题
        rho_ab = min(max(rho_ab, -1.0), 1.0)
        rho_bc = min(max(rho_bc, -1.0), 1.0)

        # 下界公式：rho_ac >= |rho_ab * rho_bc| - sqrt((1 - rho_ab^2)(1 - rho_bc^2))
        lower_bound = abs(rho_ab * rho_bc) - sqrt((1 - rho_ab**2) * (1 - rho_bc**2))
        return lower_bound


if __name__ == "__main__":
    # 运行测试
    from alphapower.client import wq_client
    from alphapower.dal.evaluate import (
        RecordSetDAL,
    )
    from alphapower.dal.session_manager import session_manager

    log: BoundLogger = get_logger(
        "alphapower.engine.evaluate.correlation_indirect_estimator.test"
    )

    async def test() -> None:
        """
        测试 PPAC2025Evaluator 的功能。
        """
        async with wq_client as client:
            alpha_dal = AlphaDAL()
            correlation_dal = CorrelationDAL()
            record_set_dal = RecordSetDAL()

            async def alpha_generator() -> AsyncGenerator[Alpha, None]:
                async with session_manager.get_session(Database.ALPHAS) as session:
                    for alpha in await alpha_dal.find_by_stage(
                        session=session,
                        stage=Stage.OS,
                    ):
                        for classification in alpha.classifications:
                            if classification.id == "POWER_POOL:POWER_POOL_ELIGIBLE":
                                await log.ainfo(
                                    event="Alpha 策略符合 Power Pool 条件",
                                    alpha_id=alpha.alpha_id,
                                    classifications=alpha.classifications,
                                    emoji="✅",
                                )
                                yield alpha
                            else:
                                await log.ainfo(
                                    event="Alpha 策略不符合 Power Pool 条件",
                                    alpha_id=alpha.alpha_id,
                                    classifications=alpha.classifications,
                                    emoji="❌",
                                )

            alpha_id: str = "7LQXXJZ"
            async with session_manager.get_session(Database.ALPHAS) as session:
                alpha_a: Optional[Alpha] = await alpha_dal.find_by_alpha_id(
                    session=session,
                    alpha_id=alpha_id,
                )

            if not alpha_a:
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
            await correlation_calculator.initialize()

            correlation_estimator = CorrelationIndirectEstimator(
                alpha_dal=alpha_dal,
                correlation_dal=correlation_dal,
                prod_alpha_stream=alpha_generator(),
                corr_calculator=correlation_calculator,
            )

            alpha_a_corrs: Dict[Alpha, float] = (
                await correlation_calculator.calculate_correlation(alpha_a)
            )

            for alpha_b, rho_ab in alpha_a_corrs.items():
                if rho_ab > 0.7:
                    await log.ainfo(
                        event="Alpha 与其他 Alpha 的相关性",
                        alpha_id_a=alpha_a.alpha_id,
                        alpha_id_b=alpha_b.alpha_id,
                        correlation=rho_ab,
                        emoji="🔄",
                    )
                    continue

                alpha_b_corrs: Dict[Alpha, float] = (
                    await correlation_calculator.calculate_correlation(
                        alpha_b,
                    )
                )

                for alpha_c, rho_bc in alpha_b_corrs.items():
                    if rho_bc > 0.7:
                        await log.ainfo(
                            event="Alpha 与其他 Alpha 的相关性",
                            alpha_id_a=alpha_b.alpha_id,
                            alpha_id_b=alpha_c.alpha_id,
                            correlation=rho_bc,
                            emoji="🔄",
                        )
                        continue

                    real_p_ac: float = alpha_a_corrs.get(alpha_c, 0.0)
                    estimated_p_ac_upper: float = (
                        correlation_estimator._calculate_upper_bound(
                            rho_ab=rho_ab,
                            rho_bc=rho_bc,
                        )
                    )
                    estimated_p_ac_lower: float = (
                        correlation_estimator._calculate_lower_bound(
                            rho_ab=rho_ab,
                            rho_bc=rho_bc,
                        )
                    )

                    if real_p_ac > estimated_p_ac_upper:
                        await log.awarning(
                            event="Alpha 实际相关性超出上界",
                            alpha_id_a=alpha_a.alpha_id,
                            alpha_id_b=alpha_c.alpha_id,
                            rho_ab=rho_ab,
                            rho_bc=rho_bc,
                            real_p_ac=real_p_ac,
                            estimated_p_ac_upper=estimated_p_ac_upper,
                            estimated_p_ac_lower=estimated_p_ac_lower,
                            emoji="⚠️",
                        )
                    elif real_p_ac < estimated_p_ac_lower:
                        await log.awarning(
                            event="Alpha 实际相关性低于下界",
                            alpha_id_a=alpha_a.alpha_id,
                            alpha_id_b=alpha_c.alpha_id,
                            rho_ab=rho_ab,
                            rho_bc=rho_bc,
                            real_p_ac=real_p_ac,
                            estimated_p_ac_upper=estimated_p_ac_upper,
                            estimated_p_ac_lower=estimated_p_ac_lower,
                            emoji="✅",
                        )
                    else:
                        await log.ainfo(
                            event="Alpha 实际相关性在上界和下界之间",
                            alpha_id_a=alpha_a.alpha_id,
                            alpha_id_b=alpha_c.alpha_id,
                            rho_ab=rho_ab,
                            rho_bc=rho_bc,
                            real_p_ac=real_p_ac,
                            estimated_p_ac_upper=estimated_p_ac_upper,
                            estimated_p_ac_lower=estimated_p_ac_lower,
                            emoji="✅",
                        )

    # 运行异步测试函数
    import asyncio

    asyncio.run(test())
