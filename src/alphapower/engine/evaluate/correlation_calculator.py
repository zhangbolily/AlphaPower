import asyncio
from datetime import datetime
from multiprocessing import Manager, Process
from typing import AsyncGenerator, Dict, List, Optional  # 用于可选类型注解

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import TableView, WorldQuantClient
from alphapower.constants import (
    Database,
    RecordSetType,
    Region,
    Stage,
)
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL, RecordSetDAL
from alphapower.dal.session_manager import session_manager
from alphapower.entity import Alpha, RecordSet
from alphapower.internal.logging import get_logger

log: BoundLogger = get_logger(__name__)


class CorrelationCalculator:
    def __init__(
        self,
        client: WorldQuantClient,
        alpha_stream: Optional[AsyncGenerator[Alpha, None]],
        alpha_dal: AlphaDAL,
        record_set_dal: RecordSetDAL,
        correlation_dal: CorrelationDAL,
        multiprocess: bool = False,
    ) -> None:
        """
        初始化 CorrelationCalculator

        :param client: WorldQuant 客户端实例
        :param alpha_stream: Alpha 策略流生成器
        :param alpha_dal: Alpha 数据访问层实例
        :param record_set_dal: RecordSet 数据访问层实例
        :param correlation_dal: Correlation 数据访问层实例
        """
        self.client: WorldQuantClient = client
        self.alpha_stream: Optional[AsyncGenerator[Alpha, None]] = (
            alpha_stream  # 修改变量名
        )
        self.alpha_dal: AlphaDAL = alpha_dal
        self.record_set_dal: RecordSetDAL = record_set_dal
        self.correlation_dal: CorrelationDAL = correlation_dal
        self._is_initialized: bool = (
            False  # 建议将 _initialized 改为 _is_initialized，更符合布尔变量的命名习惯
        )
        self._region_to_alpha_map: Dict[Region, List[str]] = (
            {}
        )  # 建议改为 _region_to_alpha_map，更清晰表达区域到 Alpha 的映射关系
        self.multiprocess: bool = multiprocess

    async def _load_missing_pnl(self, alpha_id: str) -> None:
        """
        处理缺失的 pnl 数据。

        :param alpha_id: Alpha 策略 ID
        """
        try:
            await self._retrieve_pnl_from_platform(alpha_id)
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

        missing_pnl_alpha_ids: List[str] = []

        if not self.alpha_stream:
            await log.aerror(
                event="Alpha 策略加载器未初始化",
                emoji="❌",
                module=__name__,
            )
            raise RuntimeError("Alpha 策略加载器未初始化")

        async for alpha in self.alpha_stream:
            try:
                region: Region = alpha.region
            except AttributeError:
                await log.aerror(
                    event="Alpha 策略缺少 region 设置",
                    alpha_id=alpha.alpha_id,
                    emoji="❌",
                    module=__name__,
                )
                continue

            self._region_to_alpha_map.setdefault(region, []).append(alpha.alpha_id)

            # FIXME: 数据库连接池测试
            async with session_manager.get_session(
                Database.EVALUATE, readonly=True
            ) as session:
                self.record_set_dal.session = session
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
                await self._load_missing_pnl(alpha_id)

        self._is_initialized = True
        await log.ainfo(
            event="自相关性计算器初始化完成",
            os_stage_alpha_ids=self._region_to_alpha_map,
            emoji="✅",
            module=__name__,
        )

    async def _retrieve_pnl_from_platform(self, alpha_id: str) -> pd.DataFrame:
        """
        从平台加载指定 Alpha 的 pnl 数据。
        """
        try:
            async with self.client as client:
                pnl_table_view: Optional[TableView]
                finished: bool = False
                retry_after: float = 0.0
                timeout: float = 30.0  # 设置超时时间为 30 秒
                start_time: float = asyncio.get_event_loop().time()

                while not finished:
                    # 检查是否超时
                    elapsed_time = asyncio.get_event_loop().time() - start_time
                    if elapsed_time > timeout:
                        await log.aerror(
                            event="加载 Alpha 策略的 pnl 数据超时",
                            alpha_id=alpha_id,
                            timeout=timeout,
                            emoji="⏰",
                            module=__name__,
                        )
                        raise TimeoutError(
                            f"加载 Alpha 策略 {alpha_id} 的 pnl 数据超时"
                        )

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

            # FIXME: 数据库连接池测试
            async with (
                session_manager.get_session(Database.EVALUATE) as session,
                session.begin(),
            ):
                self.record_set_dal.session = session
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

    async def _retrieve_pnl_from_local(self, alpha_id: str) -> Optional[pd.DataFrame]:
        # FIXME: 数据库连接池测试
        async with session_manager.get_session(
            Database.EVALUATE, readonly=True
        ) as session:
            self.record_set_dal.session = session
            # 从数据库中获取 pnl 数据
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

    async def _get_pnl_dataframe(
        self, alpha_id: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        # 调试日志记录函数入参
        pnl_series_df: Optional[pd.DataFrame]
        if force_refresh:
            pnl_series_df = await self._retrieve_pnl_from_platform(alpha_id)
            if pnl_series_df is None:
                await log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha in_sample 为 None")
            return pnl_series_df

        pnl_series_df = await self._retrieve_pnl_from_local(alpha_id)
        if pnl_series_df is None:
            pnl_series_df = await self._retrieve_pnl_from_platform(alpha_id)
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

    async def _prepare_pnl_dataframe(self, pnl_df: pd.DataFrame) -> pd.DataFrame:
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
        pnl_df = pnl_df.set_index("date").ffill()
        await log.adebug(
            event="成功处理 pnl 数据框",
            rows=len(pnl_df),
            columns=list(pnl_df.columns),
            emoji="✅",
        )
        return pnl_df

    async def calculate_correlation(self, alpha: Alpha) -> Dict[str, float]:
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

        if not self._is_initialized:
            await log.aerror(
                event="自相关性计算器未初始化",
                emoji="❌",
                module=__name__,
            )
            raise RuntimeError("自相关性计算器未初始化")

        start_time: datetime = datetime.now()

        try:
            region: Region = alpha.region
        except AttributeError as e:
            await log.aerror(
                event="Alpha 策略缺少 region 设置",
                alpha_id=alpha.alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 策略缺少 region 设置") from e

        matched_region_alpha_ids: List[str] = self._region_to_alpha_map.get(region, [])

        if not matched_region_alpha_ids:
            await log.awarning(
                event="没有找到同区域匹配的 OS 阶段 Alpha 策略",
                region=region,
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return {}

        x_pnl_series_df: pd.DataFrame = await self._get_pnl_dataframe(
            alpha_id=alpha.alpha_id,
            force_refresh=False,
        )
        x_pnl_series_df = await self._validate_pnl_dataframe(
            x_pnl_series_df, alpha.alpha_id
        )
        x_pnl_series_df = await self._prepare_pnl_dataframe(x_pnl_series_df)
        x_pnl_diff_series: pd.DataFrame = (
            x_pnl_series_df - x_pnl_series_df.shift(1)
        ).ffill()

        shared_y_pnl_dict: Dict[str, pd.DataFrame] = {}

        for alpha_id in matched_region_alpha_ids:
            if alpha_id == alpha.alpha_id:
                continue

            y_pnl_series_df: Optional[pd.DataFrame] = (
                await self._retrieve_pnl_from_local(alpha_id=alpha_id)
            )
            y_pnl_series_df = await self._validate_pnl_dataframe(
                y_pnl_series_df, alpha_id
            )
            y_pnl_series_df = await self._prepare_pnl_dataframe(y_pnl_series_df)
            y_pnl_diff_series: pd.DataFrame = (
                y_pnl_series_df - y_pnl_series_df.shift(1)
            ).ffill()

            shared_y_pnl_dict[alpha_id] = y_pnl_diff_series

        def compute_correlation_in_subprocess(
            shared_corr_val: Dict[str, float],
            shared_y_pnl_data: Dict[str, pd.DataFrame],
        ) -> None:
            """
            子进程中计算相关性并存储到共享变量中。

            :param shared_corr_val: 用于存储相关性值的共享字典
            """
            for alpha_id, y_pnl_diff_series in shared_y_pnl_data.items():
                corr: float = x_pnl_diff_series.corrwith(
                    y_pnl_diff_series, axis=0
                ).iloc[0]
                if pd.isna(corr):
                    log.warning(
                        event="相关性计算结果为 NaN",
                        alpha_id_a=alpha.alpha_id,
                        alpha_id_b=alpha_id,
                        emoji="⚠️",
                    )

                    continue
                shared_corr_val[alpha_id] = corr

        pairwise_correlation: Dict[str, float] = {}

        # 使用 Manager 创建共享字典
        if self.multiprocess:
            with Manager() as manager:
                pairwise_corr_val = manager.dict()

                # 创建子进程并传递共享字典
                sub_process: Process = Process(
                    target=compute_correlation_in_subprocess,
                    args=(pairwise_corr_val, shared_y_pnl_dict),
                )
                sub_process.start()
                sub_process.join()

                # 将共享字典转换为普通字典
                pairwise_correlation = dict(pairwise_corr_val)
        else:
            for alpha_id, y_pnl_diff_series in shared_y_pnl_dict.items():
                corr: float = x_pnl_diff_series.corrwith(
                    y_pnl_diff_series, axis=0
                ).iloc[0]
                if pd.isna(corr):
                    await log.awarning(
                        event="相关性计算结果为 NaN",
                        alpha_id_a=alpha.alpha_id,
                        alpha_id_b=alpha_id,
                        emoji="⚠️",
                    )
                    continue
                pairwise_correlation[alpha_id] = corr

        end_time: datetime = datetime.now()
        elapsed_time: float = (end_time - start_time).total_seconds()

        max_corr: float = max(pairwise_correlation.values(), default=0.0)
        min_corr: float = min(pairwise_correlation.values(), default=0.0)

        await log.ainfo(
            event="完成自相关性计算",
            alpha_id=alpha.alpha_id,
            max_corr=max_corr,
            min_corr=min_corr,
            elapsed_time=f"{elapsed_time:.2f}秒",
            emoji="✅",
        )
        return pairwise_correlation


if __name__ == "__main__":
    from alphapower.client import wq_client
    from alphapower.dal.base import DALFactory

    async def main() -> None:
        async with wq_client as client:
            alpha_dal: AlphaDAL = DALFactory.create_dal(dal_class=AlphaDAL)
            record_set_dal: RecordSetDAL = DALFactory.create_dal(
                dal_class=RecordSetDAL,
            )
            correlation_dal: CorrelationDAL = DALFactory.create_dal(
                dal_class=CorrelationDAL,
            )

            async def alpha_generator() -> AsyncGenerator[Alpha, None]:
                for alpha in await alpha_dal.find_by_stage(
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

                    await log.ainfo(
                        event="Alpha 策略不符合 Power Pool 条件",
                        alpha_id=alpha.alpha_id,
                        classifications=alpha.classifications,
                        emoji="❌",
                    )

            calculator = CorrelationCalculator(
                client=client,
                alpha_stream=alpha_generator(),
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
            corr: Dict[str, float] = await calculator.calculate_correlation(
                alpha=alpha,
            )
            await log.ainfo(
                event="计算完成",
                alpha_id=alpha.alpha_id,
                corr=corr,
                emoji="✅",
            )

    asyncio.run(main())
