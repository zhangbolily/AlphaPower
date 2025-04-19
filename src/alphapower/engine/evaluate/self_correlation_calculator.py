from typing import List, Optional  # 用于可选类型注解

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import TableView, WorldQuantClient
from alphapower.constants import CorrelationCalcType, RecordSetType, Status
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL, RecordSetDAL
from alphapower.entity import Alpha, Correlation, RecordSet
from alphapower.internal.logging import get_logger

log: BoundLogger = get_logger(__name__)


class SelfCorrelationCalculator:
    def __init__(
        self,
        client: WorldQuantClient,
        alpha_dal: AlphaDAL,
        record_set_dal: RecordSetDAL,
        correlation_dal: CorrelationDAL,
    ) -> None:
        """
        初始化 SelfCorrelationCalculator。

        :param client: WorldQuant 客户端实例
        :param record_set_dal: RecordSet 数据访问层实例
        """
        self.client: WorldQuantClient = client
        self.alpha_dal: AlphaDAL = alpha_dal
        self.record_set_dal: RecordSetDAL = record_set_dal
        self.correlation_dal: CorrelationDAL = correlation_dal
        self._active_alpha_ids: List[str] = []
        self._initialized: bool = False

    async def initialize(self) -> None:
        """
        初始化方法，加载活动的 Alpha 策略的 pnl 数据。

        :return: None
        """
        await log.ainfo(
            event="开始初始化自相关性计算器",
            emoji="🔄",
            module=__name__,
        )

        active_alphas: List[Alpha] = await self.alpha_dal.find_by_status(
            status=Status.ACTIVE,
        )
        missing_pnl_alpha_ids: List[str] = []

        for alpha in active_alphas:
            self._active_alpha_ids.append(alpha.alpha_id)

            record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
                alpha_id=alpha.alpha_id,
                set_type=RecordSetType.PNL,
            )

            if record_set is None:
                missing_pnl_alpha_ids.append(alpha.alpha_id)
                await log.awarning(
                    event="Alpha 策略缺少 pnl 数据",
                    alpha_id=alpha.alpha_id,
                    emoji="⚠️",
                    module=__name__,
                )
                continue

            if record_set.content is None:
                missing_pnl_alpha_ids.append(alpha.alpha_id)
                await log.awarning(
                    event="Alpha 策略的 pnl 数据为空",
                    alpha_id=alpha.alpha_id,
                    emoji="⚠️",
                    module=__name__,
                )
                continue

        if missing_pnl_alpha_ids:
            await log.awarning(
                event="缺少 pnl 数据的 Alpha 策略",
                missing_pnl_alpha_ids=missing_pnl_alpha_ids,
                emoji="⚠️",
                module=__name__,
            )
            for alpha_id in missing_pnl_alpha_ids:
                try:
                    await self._load_pnl_from_platform(alpha_id)
                except ValueError as ve:
                    await log.aerror(
                        event="加载 Alpha 策略的 pnl 数据失败 - 数据错误",
                        alpha_id=alpha_id,
                        error=str(ve),
                        emoji="❌",
                        module=__name__,
                        exc_info=True,  # 记录完整堆栈信息
                    )
                    raise  # 重新抛出数据错误异常
                except ConnectionError as ce:
                    await log.aerror(
                        event="加载 Alpha 策略的 pnl 数据失败 - 网络错误",
                        alpha_id=alpha_id,
                        error=str(ce),
                        emoji="❌",
                        module=__name__,
                        exc_info=True,
                    )
                except Exception as e:
                    await log.acritical(
                        event="加载 Alpha 策略的 pnl 数据失败 - 未知错误",
                        alpha_id=alpha_id,
                        error=str(e),
                        emoji="💥",
                        module=__name__,
                        exc_info=True,
                    )
                    raise  # 重新抛出未知异常

        self._initialized = True
        await log.ainfo(
            event="自相关性计算器初始化完成",
            active_alpha_count=len(self._active_alpha_ids),
            emoji="✅",
            module=__name__,
        )

    async def fetch_active_alpha_pnl_from_platform(self) -> None:
        """
        从平台加载活动的 Alpha 策略的 pnl 数据。
        """
        active_alphas: List[Alpha] = await self.alpha_dal.find_by_status(
            status=Status.ACTIVE,
        )

        if not active_alphas:
            await log.ainfo(
                event="没有找到任何活动的 Alpha 策略",
                emoji="🔍",
                module=__name__,
            )
            return

        await log.ainfo(
            event="开始从平台加载活动的 Alpha 策略的 pnl 数据",
            active_alpha_count=len(active_alphas),
            emoji="📊",
            module=__name__,
        )

        for alpha in active_alphas:
            try:
                await self._load_pnl_from_platform(alpha.alpha_id)
            except Exception as e:
                await log.aerror(
                    event="加载 Alpha 策略的 pnl 数据失败",
                    alpha_id=alpha.alpha_id,
                    error=str(e),
                    emoji="❌",
                    module=__name__,
                )
                continue

        await log.ainfo(
            event="完成从平台加载活动的 Alpha 策略的 pnl 数据",
            active_alpha_count=len(active_alphas),
            emoji="✅",
            module=__name__,
        )

    async def _load_pnl_from_platform(self, alpha_id: str) -> pd.DataFrame:
        """
        从平台加载指定 Alpha 的 pnl 数据。
        """
        try:
            async with self.client as client:
                pnl_table_view: Optional[TableView]
                pnl_table_view, _ = await client.alpha_fetch_record_set_pnl(
                    alpha_id=alpha_id
                )

            if pnl_table_view is None:
                raise ValueError("Alpha 的 pnl 数据为 None")

            record_set_pnl: RecordSet = RecordSet(
                alpha_id=alpha_id,
                set_type=RecordSetType.PNL,
                content=pnl_table_view.model_dump(),
            )

            existing_record_set: Optional[RecordSet] = (
                await self.record_set_dal.find_one_by(
                    alpha_id=alpha_id,
                    set_type=RecordSetType.PNL,
                )
            )

            if existing_record_set is None:
                await self.record_set_dal.create(record_set_pnl)
            else:
                record_set_pnl.id = existing_record_set.id
                await self.record_set_dal.update(
                    record_set_pnl,
                )

            pnl_series_df: Optional[pd.DataFrame] = pnl_table_view.to_dataframe()
            if pnl_series_df is None:
                raise ValueError("Alpha 的 pnl 数据转换为 DataFrame 失败")

            await log.adebug(
                event="成功从平台加载 Alpha 的 pnl 数据",
                alpha_id=alpha_id,
                emoji="✅",
                module=__name__,
            )
            return pnl_series_df

        except ValueError as ve:
            await log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 数据错误",
                alpha_id=alpha_id,
                error=str(ve),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except ConnectionError as ce:
            await log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 网络错误",
                alpha_id=alpha_id,
                error=str(ce),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except Exception as e:
            await log.acritical(
                event="加载 Alpha 策略的 pnl 数据失败 - 未知错误",
                alpha_id=alpha_id,
                error=str(e),
                emoji="💥",
                module=__name__,
                exc_info=True,
            )
            raise

    async def _load_pnl_from_local(self, alpha_id: str) -> pd.DataFrame:
        pnl_record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
            alpha_id=alpha_id,
            set_type=RecordSetType.PNL,
        )

        if pnl_record_set is None:
            await log.aerror(
                event="Alpha 的 pnl 数据为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                alpha_id=alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha in_sample 为 None")

        if pnl_record_set.content is None:
            await log.aerror(
                event="Alpha 的 pnl 数据为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                alpha_id=alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 的 pnl 数据为 None")

        pnl_series_table: TableView = TableView.model_validate(pnl_record_set.content)
        pnl_series_df: Optional[pd.DataFrame] = pnl_series_table.to_dataframe()

        if pnl_series_df is None:
            await log.aerror(
                event="Alpha 的 pnl 数据转换为 DataFrame 失败, 无法计算自相关性, 请检查 Alpha 的配置",
                alpha_id=alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 的 pnl 数据转换为 DataFrame 失败")

        return pnl_series_df

    async def _load_pnl_dataframe(
        self, alpha_id: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        # 调试日志记录函数入参
        pnl_series_df: Optional[pd.DataFrame]
        if force_refresh:
            pnl_series_df = await self._load_pnl_from_platform(alpha_id)
            if pnl_series_df is None:
                await log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id.id,
                    emoji="❌",
                )
                raise ValueError("Alpha in_sample 为 None")
            return pnl_series_df

        pnl_series_df = await self._load_pnl_from_local(alpha_id)
        if pnl_series_df is None:
            pnl_series_df = await self._load_pnl_from_platform(alpha_id)
            if pnl_series_df is None:
                await log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha in_sample 为 None")
        # 调试日志记录返回值
        await log.adebug(
            event="成功获取 Alpha 的 pnl 数据",
            alpha_id=alpha_id,
            emoji="✅",
        )

        return pnl_series_df

    async def calculate_max_correlation_with_active_alphas(self, alpha: Alpha) -> float:
        """
        计算自相关性。

        :param alpha: Alpha 实例
        :return: 自相关系数
        """
        # 方法进入日志
        await log.ainfo(
            event="开始计算 Alpha 的自相关性",
            alpha_id=alpha.id,
            emoji="🔄",
        )

        if not self._initialized:
            await log.awarning(
                event="SelfCorrelationCalculator 尚未初始化, 正在初始化",
                emoji="⚠️",
            )
            await self.initialize()
            self._initialized = True

        x_pnl_series_df: pd.DataFrame = await self._load_pnl_dataframe(
            alpha_id=alpha.alpha_id,
            force_refresh=False,
        )

        if x_pnl_series_df is None:
            await log.aerror(
                event="Alpha 的 pnl 数据为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                alpha_id=alpha.alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 的 pnl 数据为 None")

        x_pnl_series_df = x_pnl_series_df[["date", "pnl"]]
        x_pnl_series_df.set_index("date", inplace=True)
        x_pnl_series_df.ffill(inplace=True)
        x_pnl_diff_series: pd.DataFrame = (
            x_pnl_series_df - x_pnl_series_df.shift(1)
        ).ffill()

        max_corr: float = -1.0
        min_corr: float = 1.0
        for alpha_id in self._active_alpha_ids:
            if alpha_id == alpha.alpha_id:
                continue

            y_pnl_series_df: pd.DataFrame = await self._load_pnl_from_local(
                alpha_id=alpha_id,
            )

            if y_pnl_series_df is None:
                await log.aerror(
                    event="Alpha 的 pnl 数据为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha 的 pnl 数据为 None")

            y_pnl_series_df = y_pnl_series_df[["date", "pnl"]]
            y_pnl_series_df.set_index("date", inplace=True)
            y_pnl_series_df.ffill(inplace=True)
            y_pnl_diff_series: pd.DataFrame = (
                y_pnl_series_df - y_pnl_series_df.shift(1)
            ).ffill()

            corr: float = x_pnl_diff_series.corrwith(
                y_pnl_diff_series,
                axis=0,
            ).iloc[0]
            max_corr = max(max_corr, corr)
            min_corr = min(min_corr, corr)
            self_correlation: Correlation = Correlation(
                alpha_id_a=alpha.alpha_id,
                alpha_id_b=alpha_id,
                correlation=corr,
                calc_type=CorrelationCalcType.LOCAL,
            )

            await self.correlation_dal.create(self_correlation)

        return max_corr


if __name__ == "__main__":
    from alphapower.client import wq_client
    from alphapower.constants import Database
    from alphapower.internal.db_session import get_db_session

    async def main() -> None:
        async with wq_client as client:
            async with get_db_session(Database.ALPHAS) as alpha_session:
                async with get_db_session(Database.EVALUATE) as evaluate_session:
                    dal = AlphaDAL(session=alpha_session)
                    record_set_dal = RecordSetDAL(session=evaluate_session)
                    correlation_dal = CorrelationDAL(session=evaluate_session)

                    calculator = SelfCorrelationCalculator(
                        client=client,
                        alpha_dal=dal,
                        record_set_dal=record_set_dal,
                        correlation_dal=correlation_dal,
                    )
                    await calculator.initialize()

                    alpha: Optional[Alpha] = await dal.find_one_by(
                        alpha_id="d1n2w6w",
                    )

                    if alpha is None:
                        await log.aerror(
                            event="Alpha 策略不存在",
                            alpha_id="alpha_id_example",
                            emoji="❌",
                        )
                        return
                    max_corr: float = (
                        await calculator.calculate_max_correlation_with_active_alphas(
                            alpha=alpha,
                        )
                    )
                    await log.ainfo(
                        event="计算完成",
                        alpha_id=alpha.alpha_id,
                        max_corr=max_corr,
                        emoji="✅",
                    )

    import asyncio

    asyncio.run(main())
