import asyncio
from datetime import datetime
from multiprocessing import Manager, Process
from multiprocessing.managers import DictProxy
from typing import AsyncGenerator, Dict, List, Optional

import numpy as np
import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import TableView, WorldQuantClient
from alphapower.constants import (
    CORRELATION_CALCULATION_YEARS,
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


def process_target_calc_func(
    shared_corr_val: DictProxy,
    target_dict: dict,
    others_dict: dict,
    inner: bool,
) -> None:
    # 子进程内重新获取 logger
    from alphapower.internal.logging import (  # pylint: disable=W0621,W0404,C0415
        get_logger,
    )

    log = get_logger(module_name="alphapower.engine.evaluate.correlation_calculator")
    try:
        import pandas as pd  # pylint: disable=W0621,W0404,C0415

        target_df = pd.DataFrame.from_dict(target_dict)
        others_df = {k: pd.DataFrame.from_dict(v) for k, v in others_dict.items()}
        corr_dict = CorrelationCalculator._do_calculation(  # pylint: disable=W0212
            target=target_df,
            others=others_df,
            log=log,
            inner=inner,
        )
        shared_corr_val.update(corr_dict)
        preview_corr_val = dict(list(shared_corr_val.items())[:10])
        log.info(
            event="子进程计算完成",
            shared_corr_val_preview=preview_corr_val,
            total_count=len(shared_corr_val),
            emoji="✅",
        )
    except Exception as e:
        log.error(
            event="子进程相关性计算异常",
            error=str(e),
            emoji="💥",
            exc_info=True,
        )
        shared_corr_val["__error__"] = str(e)


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
        self.other_alphas_pnl_cache: Dict[str, pd.DataFrame] = {}
        self.log: BoundLogger = get_logger(
            module_name=f"{__name__}.{self.__class__.__name__}"
        )

    async def _load_missing_pnl(self, alpha: Alpha) -> None:
        """
        处理缺失的 pnl 数据。

        :param alpha: Alpha 策略实例
        """
        try:
            pnl_data: Optional[pd.DataFrame] = await self._retrieve_pnl_from_platform(
                alpha.alpha_id
            )
            if pnl_data is None or pnl_data.empty:
                await self.log.aerror(
                    event="Alpha 策略缺少 pnl 数据",
                    alpha_id=alpha.alpha_id,
                    emoji="⚠️",
                    module=__name__,
                )
                raise ValueError("Alpha 策略缺少 pnl 数据")

            pnl_data = await self._validate_pnl_dataframe(
                pnl_data,
                alpha.alpha_id,
            )
            pnl_data = await self._prepare_pnl_dataframe(pnl_data)

            # 缓存 pnl 数据
            self.other_alphas_pnl_cache[alpha.alpha_id] = pnl_data
            await self.log.ainfo(
                event="成功加载 Alpha 策略的 pnl 数据",
                alpha_id=alpha.alpha_id,
                rows=len(pnl_data),
                columns=list(pnl_data.columns),
                emoji="✅",
            )
        except ValueError as ve:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 数据错误",
                alpha_id=alpha.alpha_id,
                error=str(ve),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise
        except ConnectionError as ce:
            await self.log.aerror(
                event="加载 Alpha 策略的 pnl 数据失败 - 网络错误",
                alpha_id=alpha.alpha_id,
                error=str(ce),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
        except Exception as e:
            await self.log.acritical(
                event="加载 Alpha 策略的 pnl 数据失败 - 未知错误",
                alpha_id=alpha.alpha_id,
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

        missing_pnl_alphas: List[Alpha] = []

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

            pnl_data: Optional[pd.DataFrame] = await self._retrieve_pnl_from_local(
                alpha_id=alpha.alpha_id
            )
            if pnl_data is None or pnl_data.empty:
                missing_pnl_alphas.append(alpha)
                await self.log.awarning(
                    event="Alpha 策略缺少 pnl 数据",
                    alpha_id=alpha.alpha_id,
                    emoji="⚠️",
                    module=__name__,
                )
                continue

            pnl_data = await self._validate_pnl_dataframe(
                pnl_data,
                alpha.alpha_id,
            )
            pnl_data = await self._prepare_pnl_dataframe(pnl_data)

            # 缓存 pnl 数据
            self.other_alphas_pnl_cache[alpha.alpha_id] = pnl_data
            await self.log.ainfo(
                event="成功加载 Alpha 策略的 pnl 数据",
                alpha_id=alpha.alpha_id,
                rows=len(pnl_data),
                columns=list(pnl_data.columns),
                emoji="✅",
            )

        if missing_pnl_alphas:
            missing_pnl_alpha_ids: List[str] = [
                alpha.alpha_id for alpha in missing_pnl_alphas
            ]

            await self.log.awarning(
                event="缺少 pnl 数据的 Alpha 策略",
                missing_pnl_alpha_ids=missing_pnl_alpha_ids,
                emoji="⚠️",
                module=__name__,
            )
            for alpha in missing_pnl_alphas:
                await self._load_missing_pnl(alpha)

        self._is_initialized = True
        await self.log.ainfo(
            event="自相关性计算器初始化完成",
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
                content=pnl_table_view,
            )

            async with (
                session_manager.get_session(Database.EVALUATE) as session,
                session.begin(),
            ):
                existing_record_set: Optional[RecordSet] = (
                    await self.record_set_dal.find_one_by(
                        alpha_id=alpha_id,
                        set_type=RecordSetType.PNL,
                        session=session,
                    )
                )

                if existing_record_set is None:
                    await self.record_set_dal.create(
                        record_set_pnl,
                        session=session,
                    )
                else:
                    record_set_pnl.id = existing_record_set.id
                    await self.record_set_dal.update(
                        record_set_pnl,
                        session=session,
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
        async with session_manager.get_session(
            Database.EVALUATE, readonly=True
        ) as session:
            # 从数据库中获取 pnl 数据
            pnl_record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
                session=session,
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
        self,
        alpha_id: str,
        force_refresh: bool = False,
        inner: bool = False,
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
        else:
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
        pnl_diff_df: pd.DataFrame = await self._prepare_pnl_dataframe(
            pnl_df, inner=inner
        )

        # 调试日志记录返回值
        await self.log.adebug(
            event="成功获取 Alpha 的 pnl 数据",
            alpha_id=alpha_id,
            emoji="✅",
        )

        return pnl_diff_df

    async def _prepare_pnl_dataframe(
        self,
        pnl_df: pd.DataFrame,
        inner: bool = False,
    ) -> pd.DataFrame:
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

        pnl_df = pnl_df.set_index("date").ffill()
        pnl_df = pnl_df[["pnl"]].ffill()

        if not inner:
            # 不是 inner 相关性，pnl 取固定的回溯周期计算
            four_years_ago = pnl_df.index.max() - pd.DateOffset(
                years=CORRELATION_CALCULATION_YEARS
            )
            pnl_df = pnl_df[pnl_df.index > four_years_ago]

        pnl_diff_df: pd.DataFrame = pnl_df - pnl_df.shift(1)
        pnl_diff_df = pnl_diff_df.ffill().fillna(0)
        pnl_diff_df = pnl_diff_df.sort_index(ascending=True)

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
        others: Dict[str, pd.DataFrame],
        log: BoundLogger,
        inner: bool = False,
    ) -> Dict[str, float]:
        """
        使用 numpy 实现相关性（correlation，相关系数）计算逻辑。

        :param target: 目标 Alpha 的 pnl 差分数据
        :param others: 其他 Alpha 的 pnl 差分数据，key 为 alpha_id
        :param log: 日志对象 BoundLogger
        :param inner: 是否为内相关性（inner correlation，内相关性）
        :return: 相关性字典，key 为 alpha_id，value 为相关系数
        """
        correlation_map: Dict[str, float] = {}

        if target is None:
            log.error(
                event="目标 Alpha 策略的 pnl 数据为 None，无法计算相关性",
                emoji="❌",
            )
            return {}

        start: datetime = datetime.now()
        target_values: np.ndarray = target.values.squeeze()
        target_index = target.index

        log.debug(
            event="开始相关性批量计算",
            target_shape=target.shape,
            others_count=len(others),
            inner=inner,
            emoji="🧮",
        )

        for other_alpha_id, other_pnl_df in others.items():
            if other_alpha_id is None or other_pnl_df is None:
                log.error(
                    event="其他 Alpha 策略的 pnl 数据为 None，无法计算相关性",
                    alpha_id=other_alpha_id,
                    emoji="❌",
                )
                continue

            # 对齐索引，保证数据长度一致
            if inner:
                # 内相关性：取交集索引
                common_index = target_index.intersection(other_pnl_df.index)
                if common_index.empty:
                    log.warning(
                        event="内相关性计算时，目标与其他 Alpha 策略的 pnl 索引无交集",
                        alpha_id=other_alpha_id,
                        emoji="⚠️",
                    )
                    continue
                target_arr = target.loc[common_index].values.squeeze()
                other_arr = other_pnl_df.loc[common_index].values.squeeze()
            else:
                # 外相关性：直接对齐索引
                target_arr = target_values
                other_arr = other_pnl_df.values.squeeze()
                # 若长度不一致，取最短长度
                min_len = min(len(target_arr), len(other_arr))
                target_arr = target_arr[-min_len:]
                other_arr = other_arr[-min_len:]

            # 检查有效性
            if target_arr.size == 0 or other_arr.size == 0:
                log.warning(
                    event="相关性计算时，目标或其他 Alpha 策略的 pnl 数据为空",
                    alpha_id=other_alpha_id,
                    emoji="⚠️",
                )
                continue

            # 计算皮尔逊相关系数（Pearson correlation coefficient，皮尔逊相关性）
            try:
                corr_matrix: np.ndarray = np.corrcoef(
                    target_arr,
                    other_arr,
                    rowvar=False,
                )
                corr: float = corr_matrix[0, 1]
            except Exception as e:
                log.error(
                    event="相关性计算异常",
                    alpha_id=other_alpha_id,
                    error=str(e),
                    emoji="💥",
                    exc_info=True,
                )
                continue

            if np.isnan(corr):
                log.warning(
                    event="相关性计算结果为 NaN",
                    alpha_id=other_alpha_id,
                    emoji="⚠️",
                )
                continue

            correlation_map[other_alpha_id] = corr

            log.debug(
                event="单个 Alpha 相关性计算完成",
                alpha_id=other_alpha_id,
                correlation=corr,
                emoji="🔗",
            )

        elapsed_time: float = (datetime.now() - start).total_seconds()
        log.info(
            event="相关性批量计算完成",
            total=len(correlation_map),
            elapsed_time=f"{elapsed_time:.2f}秒",
            emoji="✅",
        )

        return correlation_map

    @staticmethod
    def _do_calculation_in_subprocess(
        target: pd.DataFrame,
        others: Dict[str, pd.DataFrame],  # 用 alpha_id 作为 key
        inner: bool = False,
    ) -> Dict[str, float]:
        """
        在子进程中计算相关性，支持异常处理。
        只传递可序列化对象，避免传递 self、log、复杂对象。
        """
        local_log: BoundLogger = get_logger(
            module_name="alphapower.engine.evaluate.correlation_calculator"
        )

        # 只传递 dict
        target_dict = target.to_dict()
        others_dict = {k: v.to_dict() for k, v in others.items()}

        with Manager() as manager:
            pairwise_corr_val: DictProxy = manager.dict()
            sub_process = Process(
                target=process_target_calc_func,
                args=(pairwise_corr_val, target_dict, others_dict, inner),
                name="CorrelationCalculator",
            )
            sub_process.start()

            local_log.info(
                event="子进程计算开始",
                emoji="🔄",
                module=__name__,
                pid=sub_process.pid,
            )

            sub_process.join(timeout=30)
            if "__error__" in pairwise_corr_val:
                error_msg = pairwise_corr_val["__error__"]
                local_log.error(
                    event="子进程相关性计算失败",
                    pid=sub_process.pid,
                    error=error_msg,
                    emoji="💥",
                )
                raise RuntimeError(f"子进程相关性计算失败: {error_msg}")

            if sub_process.is_alive():
                sub_process.terminate()
                local_log.error(
                    event="子进程相关性计算超时",
                    pid=sub_process.pid,
                    emoji="⏰",
                )
                raise TimeoutError("子进程相关性计算超时")

            sub_process.close()
            pairwise_correlation = dict(pairwise_corr_val)
        return pairwise_correlation

    async def calculate_correlation(
        self,
        alpha: Alpha,
        force_refresh: bool = False,
        inner: bool = False,
    ) -> Dict[Alpha, float]:
        """
        计算自相关性（correlation，自相关系数）。

        :param alpha: Alpha 实例
        :param force_refresh: 是否强制刷新 pnl 数据
        :param inner: 是否为内相关性（inner correlation，内相关性）
        :return: 自相关系数字典，key 为 Alpha 实例，value 为相关系数
        """
        await self.log.ainfo(
            event="开始计算 Alpha 的自相关性",
            alpha_id=alpha.alpha_id,
            force_refresh=force_refresh,
            inner=inner,
            emoji="🔄",
        )

        if not self._is_initialized:
            await self.log.aerror(
                event="自相关性计算器未初始化",
                alpha_id=alpha.alpha_id,
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
                error=str(e),
                emoji="❌",
                module=__name__,
                exc_info=True,
            )
            raise ValueError("Alpha 策略缺少 region 设置") from e

        matched_region_alphas: List[Alpha] = self._region_to_alpha_map.get(region, [])
        matched_alpha_map: Dict[str, Alpha] = {
            a.alpha_id: a for a in matched_region_alphas
        }

        if not matched_region_alphas:
            await self.log.awarning(
                event="没有找到同区域匹配的 OS 阶段 Alpha 策略",
                region=str(region),
                alpha_id=alpha.alpha_id,
                emoji="⚠️",
                module=__name__,
            )
            return {}

        try:
            target_pnl_diff_df: pd.DataFrame = await self._get_pnl_dataframe(
                alpha_id=alpha.alpha_id,
                force_refresh=force_refresh,
                inner=inner,
            )
        except Exception as e:
            await self.log.aerror(
                event="获取目标 Alpha 的 pnl 数据失败",
                alpha_id=alpha.alpha_id,
                error=str(e),
                emoji="💥",
                module=__name__,
                exc_info=True,
            )
            raise

        pairwise_correlation: Dict[str, float] = {}

        try:
            if self.multiprocess:
                start_subprocess_time: datetime = datetime.now()
                # 使用异步线程池调用子进程计算
                task = asyncio.to_thread(
                    self._do_calculation_in_subprocess,
                    target_pnl_diff_df,
                    self.other_alphas_pnl_cache,
                    inner,
                )
                pairwise_correlation = await task

                subprocess_elapsed_time: float = (
                    datetime.now() - start_subprocess_time
                ).total_seconds()
                await self.log.ainfo(
                    event="子进程相关性计算完成",
                    alpha_id=alpha.alpha_id,
                    elapsed_time=f"{subprocess_elapsed_time:.2f}秒",
                    emoji="✅",
                    module=__name__,
                )
            else:
                pairwise_correlation = self._do_calculation(
                    target=target_pnl_diff_df,
                    others=self.other_alphas_pnl_cache,
                    log=self.log,
                    inner=inner,
                )
        except TimeoutError as te:
            await self.log.aerror(
                event="相关性计算超时",
                alpha_id=alpha.alpha_id,
                error=str(te),
                emoji="⏰",
                module=__name__,
                exc_info=True,
            )
            raise
        except Exception as e:
            await self.log.acritical(
                event="相关性计算发生严重异常",
                alpha_id=alpha.alpha_id,
                error=str(e),
                emoji="💥",
                module=__name__,
                exc_info=True,
            )
            raise

        end_time: datetime = datetime.now()
        elapsed_time: float = (end_time - start_time).total_seconds()

        max_corr: float = max(pairwise_correlation.values(), default=0.0)
        min_corr: float = min(pairwise_correlation.values(), default=0.0)

        await self.log.ainfo(
            event="完成 Alpha 的自相关性计算",
            alpha_id=alpha.alpha_id,
            max_corr=max_corr,
            min_corr=min_corr,
            result_count=len(pairwise_correlation),
            elapsed_time=f"{elapsed_time:.2f}秒",
            emoji="✅",
            module=__name__,
        )

        result: Dict[Alpha, float] = {
            matched_alpha_map[alpha_id]: correlation
            for alpha_id, correlation in pairwise_correlation.items()
            if alpha_id in matched_alpha_map
        }

        await self.log.adebug(
            event="自相关性计算结果详情",
            alpha_id=alpha.alpha_id,
            result_preview=dict(list(result.items())[:5]),
            emoji="📊",
            module=__name__,
        )

        return result


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
                    alpha_id="8NqbaZv",
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
