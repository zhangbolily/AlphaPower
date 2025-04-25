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
        self._is_initialized: bool = False
        self._region_to_alpha_map: Dict[Region, List[Alpha]] = {}
        self.multiprocess: bool = multiprocess
        self.log: BoundLogger = get_logger(module_name=self.__class__.__name__)

    async def _load_missing_pnl(self, alpha_id: str) -> None:
        """
        处理缺失的 pnl 数据。

        :param alpha_id: Alpha 策略 ID
        """
        try:
            await self._retrieve_pnl_from_platform(alpha_id)
        except ValueError as ve:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 数据错误",
                alpha_id=alpha_id,
                error=str(ve),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except ConnectionError as ce:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 网络错误",
                alpha_id=alpha_id,
                error=str(ce),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
        except Exception as e:
            await self.log.acritical(
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
            await self.log.aerror(
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
        await self.log.ainfo(
            event="开始初始化自相关性计算器",
            emoji="🔄",
            module=__name__,
        )

        missing_pnl_alpha_ids: List[str] = []

        if not self.alpha_stream:
            await self.log.aerror(
                event="Alpha 策略加载器未初始化",
                emoji="❌",
                module=__name__,
            )
            raise RuntimeError("Alpha 策略加载器未初始化")

        async for alpha in self.alpha_stream:
            try:
                region: Region = alpha.region
            except AttributeError:
                await self.log.aerror(
                    event="Alpha 策略缺少 region 设置",
                    alpha_id=alpha.alpha_id,
                    emoji="❌",
                    module=__name__,
                )
                continue

            self._region_to_alpha_map.setdefault(region, []).append(alpha)

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
                await self.log.awarning(
                    event="Alpha 策略缺少或为空的 pnl 数据",
                    alpha_id=alpha.alpha_id,
                    emoji="⚠️",
                    module=__name__,
                )
                continue

        if missing_pnl_alpha_ids:
            await self.log.awarning(
                event="缺少 pnl 数据的 Alpha 策略",
                missing_pnl_alpha_ids=missing_pnl_alpha_ids,
                emoji="⚠️",
                module=__name__,
            )
            for alpha_id in missing_pnl_alpha_ids:
                await self._load_missing_pnl(alpha_id)

        self._is_initialized = True
        await self.log.ainfo(
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
                        await self.log.aerror(
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
                        await self.log.ainfo(
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

            await self.log.adebug(
                event="成功从平台加载 Alpha 的 pnl 数据",
                alpha_id=alpha_id,
                emoji="✅",
                module=__name__,
            )
            return pnl_series_df

        except ValueError as ve:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 数据错误",
                alpha_id=alpha_id,
                error=str(ve),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except ConnectionError as ce:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 网络错误",
                alpha_id=alpha_id,
                error=str(ce),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except Exception as e:
            await self.log.acritical(
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
            await self.log.awarning(
                event="Alpha 策略缺少 pnl 数据",
                alpha_id=alpha_id,
                emoji="⚠️",
            )
            return None

        if pnl_record_set.content is None:
            await self.log.awarning(
                event="Alpha 策略的 pnl 数据为空",
                alpha_id=alpha_id,
                emoji="⚠️",
            )
            return None

        pnl_series_table: TableView = TableView.model_validate(pnl_record_set.content)
        pnl_series_df: Optional[pd.DataFrame] = pnl_series_table.to_dataframe()

        if pnl_series_df is None:
            await self.log.aerror(
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
        pnl_df: Optional[pd.DataFrame]
        if force_refresh:
            pnl_df = await self._retrieve_pnl_from_platform(alpha_id)
            if pnl_df is None:
                await self.log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha in_sample 为 None")
            return pnl_df

        pnl_df = await self._retrieve_pnl_from_local(alpha_id)
        if pnl_df is None:
            pnl_df = await self._retrieve_pnl_from_platform(alpha_id)
            if pnl_df is None:
                await self.log.aerror(
                    event="Alpha in_sample 为 None, 无法计算自相关性, 请检查 Alpha 的配置",
                    alpha_id=alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha in_sample 为 None")

        pnl_df = await self._validate_pnl_dataframe(
            pnl_df,
            alpha_id,
        )
        pnl_diff_df = await self._prepare_pnl_dataframe(pnl_df)

        # 调试日志记录返回值
        await self.log.adebug(
            event="成功获取 Alpha 的 pnl 数据",
            alpha_id=alpha_id,
            emoji="✅",
        )

        return pnl_diff_df

    async def _prepare_pnl_dataframe(self, pnl_df: pd.DataFrame) -> pd.DataFrame:
        """
        处理 pnl 数据框，包括日期转换、过滤、设置索引和填充缺失值。

        :param pnl_df: 原始 pnl 数据框
        :return: 处理后的 pnl 数据框
        """
        try:
            pnl_df["date"] = pd.to_datetime(pnl_df["date"])  # 转换为 datetime 类型
        except Exception as e:
            await self.log.aerror(
                event="日期转换失败",
                error=str(e),
                emoji="❌",
            )
            raise ValueError("日期转换失败") from e

        four_years_ago = pnl_df["date"].max() - pd.DateOffset(years=4)
        pnl_df = pnl_df[pnl_df["date"] >= four_years_ago]
        pnl_df = pnl_df.set_index("date").ffill()
        pnl_df = pnl_df[["pnl"]].ffill()

        pnl_diff_df: pd.DataFrame = (pnl_df - pnl_df.shift(1)).ffill()

        await self.log.adebug(
            event="成功处理 pnl 数据框",
            rows=len(pnl_diff_df),
            columns=list(pnl_diff_df.columns),
            emoji="✅",
        )
        return pnl_diff_df

    @staticmethod
    def _do_calculation(
        target: pd.DataFrame,
        others: Dict[Alpha, pd.DataFrame],
        log: BoundLogger,
    ) -> Dict[Alpha, float]:
        correlation_map: Dict[Alpha, float] = {}

        if target is None:
            log.error(
                event="Alpha 策略的 pnl 数据为 None, 无法计算自相关性",
                emoji="❌",
            )
            return {}

        start: datetime = datetime.now()
        for other_alpha, other_pnl_df in others.items():
            if other_alpha is None or other_pnl_df is None:
                log.error(
                    event="Alpha 策略的 pnl 数据为 None, 无法计算自相关性",
                    alpha_id=other_alpha.alpha_id,
                    emoji="❌",
                )
                raise ValueError("Alpha 策略的 pnl 数据为 None, 无法计算自相关性")

            correlation: float = target.corrwith(other_pnl_df, axis=0).iloc[0]
            if pd.isna(correlation):
                log.warning(
                    event="相关性计算结果为 NaN",
                    alpha_id=other_alpha.alpha_id,
                    emoji="⚠️",
                )
                continue
            correlation_map[other_alpha] = correlation

        elapsed_time: float = (datetime.now() - start).total_seconds()
        log.info(
            event="相关性计算任务耗时",
            elapsed_time=f"{elapsed_time:.2f}秒",
            emoji="✅",
        )

        return correlation_map

    async def calculate_correlation(self, alpha: Alpha) -> Dict[Alpha, float]:
        """
        计算自相关性。

        :param alpha: Alpha 实例
        :return: 自相关系数
        """
        await self.log.ainfo(
            event="开始计算 Alpha 的自相关性",
            alpha_id=alpha.alpha_id,
            emoji="🔄",
        )

        if not self._is_initialized:
            await self.log.aerror(
                event="自相关性计算器未初始化",
                emoji="❌",
                module=__name__,
            )
            raise RuntimeError("自相关性计算器未初始化")

        start_time: datetime = datetime.now()

        try:
            region: Region = alpha.region
        except AttributeError as e:
            await self.log.aerror(
                event="Alpha 策略缺少 region 设置",
                alpha_id=alpha.alpha_id,
                emoji="❌",
            )
            raise ValueError("Alpha 策略缺少 region 设置") from e

        matched_region_alphas: List[Alpha] = self._region_to_alpha_map.get(region, [])

        if not matched_region_alphas:
            await self.log.awarning(
                event="没有找到同区域匹配的 OS 阶段 Alpha 策略",
                region=region,
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
            )
            return {}

        target_pnl_diff_df: pd.DataFrame = await self._get_pnl_dataframe(
            alpha_id=alpha.alpha_id,
            force_refresh=False,
        )

        shared_others_pnl_diff_dict: Dict[Alpha, pd.DataFrame] = {}

        for alpha in matched_region_alphas:
            if alpha == alpha.alpha_id:
                continue

            other_pnl_series_df: pd.DataFrame = await self._get_pnl_dataframe(
                alpha_id=alpha.alpha_id,
                force_refresh=False,
            )

            shared_others_pnl_diff_dict[alpha] = other_pnl_series_df

        def compute_correlation_in_subprocess(
            shared_corr_val: Dict[Alpha, float],
            shared_y_pnl_data: Dict[Alpha, pd.DataFrame],
            log: BoundLogger,
        ) -> None:
            """
            子进程中计算相关性并存储到共享变量中。

            :param shared_corr_val: 用于存储相关性值的共享字典
            """
            shared_corr_val = self._do_calculation(
                target=target_pnl_diff_df,
                others=shared_y_pnl_data,
                log=self.log,
            )

            log.info(
                event="子进程计算完成",
                alpha_id=alpha.alpha_id,
                shared_corr_val=shared_corr_val,
                emoji="✅",
            )

        pairwise_correlation: Dict[Alpha, float] = {}

        # 使用 Manager 创建共享字典
        if self.multiprocess:
            start_subprocess_time: datetime = datetime.now()
            with Manager() as manager:
                pairwise_corr_val = manager.dict()

                # 创建子进程并传递共享字典
                sub_process: Process = Process(
                    target=compute_correlation_in_subprocess,
                    args=(pairwise_corr_val, shared_others_pnl_diff_dict, self.log),
                )
                sub_process.start()
                sub_process.join()

                # 将共享字典转换为普通字典
                pairwise_correlation = dict(pairwise_corr_val)

            subprocess_elapsed_time: float = (
                datetime.now() - start_subprocess_time
            ).total_seconds()
            await self.log.ainfo(
                event="子进程计算完成",
                elapsed_time=f"{subprocess_elapsed_time:.2f}秒",
                emoji="✅",
            )
        else:
            pairwise_correlation = self._do_calculation(
                target=target_pnl_diff_df,
                others=shared_others_pnl_diff_dict,
                log=self.log,
            )

        end_time: datetime = datetime.now()
        elapsed_time: float = (end_time - start_time).total_seconds()

        max_corr: float = max(pairwise_correlation.values(), default=0.0)
        min_corr: float = min(pairwise_correlation.values(), default=0.0)

        await self.log.ainfo(
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

    log1: BoundLogger = get_logger(module_name=__name__)

    async def main() -> None:
        async with wq_client as client:
            alpha_dal: AlphaDAL = DALFactory.create_dal(dal_class=AlphaDAL)
            record_set_dal: RecordSetDAL = DALFactory.create_dal(
                dal_class=RecordSetDAL,
            )
            correlation_dal: CorrelationDAL = DALFactory.create_dal(
                dal_class=CorrelationDAL,
            )

            async with session_manager.get_session(Database.ALPHAS) as session:
                alpha_list: List[Alpha] = await alpha_dal.find_by_stage(
                    session=session,
                    stage=Stage.OS,
                )

            async def alpha_generator() -> AsyncGenerator[Alpha, None]:
                for alpha in alpha_list:
                    for classification in alpha.classifications:
                        if classification.id == "POWER_POOL:POWER_POOL_ELIGIBLE":
                            await log1.ainfo(
                                event="Alpha 策略符合 Power Pool 条件",
                                alpha_id=alpha.alpha_id,
                                classifications=alpha.classifications,
                                emoji="✅",
                            )
                            yield alpha

                    await log1.ainfo(
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
                multiprocess=False,
            )
            await calculator.initialize()

            async with session_manager.get_session(
                Database.ALPHAS, readonly=True
            ) as session:
                alpha: Optional[Alpha] = await alpha_dal.find_one_by(
                    session=session,
                    alpha_id="d1n2w6w",
                )

            if alpha is None:
                await log1.aerror(
                    event="Alpha 策略不存在",
                    alpha_id="alpha_id_example",
                    emoji="❌",
                )
                return
            corr: Dict[Alpha, float] = await calculator.calculate_correlation(
                alpha=alpha,
            )
            await log1.ainfo(
                event="计算完成",
                alpha_id=alpha.alpha_id,
                corr=corr,
                emoji="✅",
            )

    asyncio.run(main())
