import asyncio
from datetime import datetime
from typing import Dict, List, Optional  # 用于可选类型注解

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import TableView, WorldQuantClient
from alphapower.constants import (
    CorrelationCalcType,
    RecordSetType,
    Region,
    Stage,
    Status,
)
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
        self._initialized: bool = False
        self._os_alpha_map: Dict[Region, List[str]] = {}

    async def _handle_missing_pnl(self, alpha_id: str) -> None:
        """
        处理缺失的 pnl 数据。

        :param alpha_id: Alpha 策略 ID
        """
        try:
            await self._load_pnl_from_platform(alpha_id)
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

    async def _validate_pnl_dataframe(
        self, pnl_df: Optional[pd.DataFrame], alpha_id: str
    ) -> pd.DataFrame:
        """
        验证 pnl 数据框是否有效。

        :param pnl_df: pnl 数据框
        :param alpha_id: Alpha 策略 ID
        :return: 验证后的 pnl 数据框
        """
        if pnl_df is None:
            await log.aerror(
                event="Alpha 的 pnl 数据为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                alpha_id=alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 的 pnl 数据为 None")
        return pnl_df

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

        os_alphas: List[Alpha] = await self.alpha_dal.find_by_stage(
            stage=Stage.OS,
        )
        missing_pnl_alpha_ids: List[str] = []

        for alpha in os_alphas:
            try:
                region: Region = alpha.settings.region
            except AttributeError:
                await log.aerror(
                    event="Alpha 策略缺少 region 设置",
                    alpha_id=alpha.alpha_id,
                    emoji="❌",
                    module=__name__,
                )
                continue

            self._os_alpha_map.setdefault(region, []).append(alpha.alpha_id)

            record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
                alpha_id=alpha.alpha_id,
                set_type=RecordSetType.PNL,
            )

            if record_set is None or record_set.content is None:
                missing_pnl_alpha_ids.append(alpha.alpha_id)
                await log.awarning(
                    event="Alpha 策略缺少或为空的 pnl 数据",
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
                await self._handle_missing_pnl(alpha_id)

        self._initialized = True
        await log.ainfo(
            event="自相关性计算器初始化完成",
            os_stage_alpha_ids=self._os_alpha_map,
            emoji="✅",
            module=__name__,
        )

    async def load_active_alpha_pnl(self) -> None:
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
                finished: bool = False
                retry_after: float = 0.0
                while not finished:
                    finished, pnl_table_view, retry_after, _ = (
                        await client.alpha_fetch_record_set_pnl(alpha_id=alpha_id)
                    )

                    if not finished:
                        await log.ainfo(
                            event="Alpha 策略的 pnl 数据加载中, 等待重试",
                            alpha_id=alpha_id,
                            retry_after=retry_after,
                            emoji="⏳",
                            module=__name__,
                        )
                        await asyncio.sleep(retry_after)

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

    async def _load_pnl_from_local(self, alpha_id: str) -> Optional[pd.DataFrame]:
        pnl_record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
            alpha_id=alpha_id,
            set_type=RecordSetType.PNL,
        )

        if pnl_record_set is None:
            await log.awarning(
                event="Alpha 策略缺少 pnl 数据",
                alpha_id=alpha_id,
                emoji="⚠️",
            )
            return None

        if pnl_record_set.content is None:
            await log.awarning(
                event="Alpha 策略的 pnl 数据为空",
                alpha_id=alpha_id,
                emoji="⚠️",
            )
            return None

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

    async def _retrieve_pnl_dataframe(
        self, alpha_id: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        # 调试日志记录函数入参
        pnl_series_df: Optional[pd.DataFrame]
        if force_refresh:
            pnl_series_df = await self._load_pnl_from_platform(alpha_id)
            if pnl_series_df is None:
                await log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
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

    async def _process_pnl_dataframe(self, pnl_df: pd.DataFrame) -> pd.DataFrame:
        """
        处理 pnl 数据框，包括日期转换、过滤、设置索引和填充缺失值。

        :param pnl_df: 原始 pnl 数据框
        :return: 处理后的 pnl 数据框
        """
        try:
            pnl_df["date"] = pd.to_datetime(pnl_df["date"])  # 转换为 datetime 类型
        except Exception as e:
            await log.aerror(
                event="日期转换失败",
                error=str(e),
                emoji="❌",
            )
            raise ValueError("日期转换失败") from e

        four_years_ago = pnl_df["date"].max() - pd.DateOffset(years=4)
        pnl_df = pnl_df[pnl_df["date"] >= four_years_ago]
        pnl_df.set_index("date", inplace=True)
        pnl_df.ffill(inplace=True)
        await log.adebug(
            event="成功处理 pnl 数据框",
            rows=len(pnl_df),
            columns=list(pnl_df.columns),
            emoji="✅",
        )
        return pnl_df

    async def calculate_self_correlation(self, alpha: Alpha) -> Dict[str, float]:
        """
        计算自相关性。

        :param alpha: Alpha 实例
        :return: 自相关系数
        """
        await log.ainfo(
            event="开始计算 Alpha 的自相关性",
            alpha_id=alpha.alpha_id,
            emoji="🔄",
        )

        if not self._initialized:
            await log.awarning(
                event="SelfCorrelationCalculator 尚未初始化, 正在初始化",
                emoji="⚠️",
            )
            await self.initialize()

        start_time: datetime = datetime.now()

        try:
            region: Region = alpha.settings.region
        except AttributeError as e:
            await log.aerror(
                event="Alpha 策略缺少 region 设置",
                alpha_id=alpha.alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 策略缺少 region 设置") from e

        matched_region_alpha_ids: List[str] = self._os_alpha_map.get(region, [])

        if not matched_region_alpha_ids:
            await log.awarning(
                event="没有找到同区域匹配的 OS 阶段 Alpha 策略",
                region=region,
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return {}

        x_pnl_series_df: pd.DataFrame = await self._retrieve_pnl_dataframe(
            alpha_id=alpha.alpha_id,
            force_refresh=False,
        )
        x_pnl_series_df = await self._validate_pnl_dataframe(
            x_pnl_series_df, alpha.alpha_id
        )
        x_pnl_series_df = await self._process_pnl_dataframe(x_pnl_series_df)
        x_pnl_diff_series: pd.DataFrame = (
            x_pnl_series_df - x_pnl_series_df.shift(1)
        ).ffill()

        max_corr: float = -1.0
        min_corr: float = 1.0
        pairwise_correlation: Dict[str, float] = {}

        for alpha_id in matched_region_alpha_ids:
            if alpha_id == alpha.alpha_id:
                continue

            y_pnl_series_df: Optional[pd.DataFrame] = await self._load_pnl_from_local(
                alpha_id=alpha_id
            )
            y_pnl_series_df = await self._validate_pnl_dataframe(
                y_pnl_series_df, alpha_id
            )
            y_pnl_series_df = await self._process_pnl_dataframe(y_pnl_series_df)
            y_pnl_diff_series: pd.DataFrame = (
                y_pnl_series_df - y_pnl_series_df.shift(1)
            ).ffill()

            corr: float = x_pnl_diff_series.corrwith(y_pnl_diff_series, axis=0).iloc[0]
            if pd.isna(corr):
                await log.awarning(
                    event="相关性计算结果为 NaN",
                    alpha_id_a=alpha.alpha_id,
                    alpha_id_b=alpha_id,
                    emoji="⚠️",
                )
                continue

            self_correlation: Correlation = Correlation(
                alpha_id_a=alpha.alpha_id,
                alpha_id_b=alpha_id,
                correlation=corr,
                calc_type=CorrelationCalcType.LOCAL,
            )
            await self.correlation_dal.create(self_correlation)
            pairwise_correlation[alpha_id] = corr

            max_corr = max(max_corr, corr)
            min_corr = min(min_corr, corr)

        end_time: datetime = datetime.now()
        elapsed_time: float = (end_time - start_time).total_seconds()

        await log.ainfo(
            event="完成自相关性计算",
            alpha_id=alpha.alpha_id,
            max_corr=max_corr,
            min_corr=min_corr,
            elapsed_time="{:.2f} 秒".format(elapsed_time),
            emoji="✅",
        )
        return pairwise_correlation


if __name__ == "__main__":
    from alphapower.client import wq_client
    from alphapower.constants import Database
    from alphapower.internal.db_session import get_db_session

    async def main() -> None:
        async with wq_client as client:
            async with get_db_session(Database.ALPHAS) as alpha_session:
                async with get_db_session(Database.EVALUATE) as evaluate_session:
                    alpha_dal = AlphaDAL(session=alpha_session)
                    record_set_dal = RecordSetDAL(session=evaluate_session)
                    correlation_dal = CorrelationDAL(session=evaluate_session)

                    calculator = SelfCorrelationCalculator(
                        client=client,
                        alpha_dal=alpha_dal,
                        record_set_dal=record_set_dal,
                        correlation_dal=correlation_dal,
                    )
                    await calculator.initialize()

                    alpha: Optional[Alpha] = await alpha_dal.find_one_by(
                        alpha_id="d1n2w6w",
                    )

                    if alpha is None:
                        await log.aerror(
                            event="Alpha 策略不存在",
                            alpha_id="alpha_id_example",
                            emoji="❌",
                        )
                        return
                    corr: Dict[str, float] = (
                        await calculator.calculate_self_correlation(
                            alpha=alpha,
                        )
                    )
                    await log.ainfo(
                        event="计算完成",
                        alpha_id=alpha.alpha_id,
                        corr=corr,
                        emoji="✅",
                    )

    import asyncio

    asyncio.run(main())
