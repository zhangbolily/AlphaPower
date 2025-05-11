import asyncio
from concurrent.futures import ProcessPoolExecutor
from itertools import combinations
from math import ceil
from typing import Any, Dict, List, Optional, Set, Tuple, TypeVar

import numpy as np
import pandas as pd

from alphapower.client.common_view import TableView
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import (
    CorrelationCalcType,
    CorrelationType,
    Database,
    LoggingEmoji,
)
from alphapower.dal import correlation_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.evaluate import Correlation
from alphapower.internal.decorator import async_exception_handler, async_timed
from alphapower.internal.multiprocessing import BaseProcessSafeClass
from alphapower.view.alpha import ProdCorrelationView, SelfCorrelationView

T = TypeVar("T")  # 泛型类型，代表 others 的 key 类型


class CorrelationManager(BaseProcessSafeClass):
    # 相关性矩阵管理器，负责相关性计算、平台/本地数据交互、约束下矩阵优化等
    def __init__(
        self,
        brain_client: AbstractWorldQuantBrainClient,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.brain_client = brain_client

    @async_timed
    async def calculate_correlations_with(
        self,
        target_series: List[float],  # 目标序列
        others_series_dict: Dict[T, List[float]],  # 其他序列字典
    ) -> Dict[T, float]:
        await self.log.ainfo(
            event="进入 calculate_correlations_with 方法",
            target_series_length=len(target_series),
            others_series_count=len(others_series_dict),
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            "开始批量计算目标序列相关系数",
            target_series=target_series,
            others_series_dict=others_series_dict,
            emoji="🔢",
        )

        correlation_results: Dict[T, float] = {}

        for series_key, series_values in others_series_dict.items():
            await self.log.adebug(
                "处理单个对比序列",
                series_key=series_key,
                series_values=series_values,
                emoji="🔍",
            )
            try:
                correlation = await self._calculate_pairwise_correlation(
                    target_series, series_values
                )
            except ValueError as ve:
                await self.log.awarning(
                    "输入数据异常，相关系数计算失败",
                    series_key=series_key,
                    error=str(ve),
                    emoji="⚠️",
                )
                correlation_results[series_key] = float("nan")
                continue
            except Exception as exc:
                await self.log.aerror(
                    "相关系数计算出现未预期异常",
                    series_key=series_key,
                    error=str(exc),
                    emoji="❌",
                    exc_info=True,
                )
                correlation_results[series_key] = float("nan")
                continue

            correlation_results[series_key] = correlation
            await self.log.adebug(
                "单个序列相关系数计算完成",
                series_key=series_key,
                correlation=correlation,
                emoji="✅",
            )

        await self.log.ainfo(
            "全部相关系数计算完成",
            correlation_results=correlation_results,
            emoji="🎉",
        )
        await self.log.ainfo(
            event="退出 calculate_correlations_with 方法",
            result_count=len(correlation_results),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return correlation_results

    @async_timed
    async def compute_pearson_correlation_matrix(
        self,
        sequences_dict: Dict[T, List[float]],  # 多组数值序列
    ) -> Dict[T, Dict[T, float]]:
        await self.log.ainfo(
            event="进入 compute_pearson_correlation_matrix 方法",
            sequences_count=len(sequences_dict),
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            "开始计算皮尔逊相关系数矩阵（批量模式）",
            sequences_dict=sequences_dict,
            emoji="🧮",
        )
        sequence_keys: List[T] = list(sequences_dict.keys())
        try:
            # 构建二维数组，每行为一组序列，rowvar=True 表示每行是一个变量（序列，variable/sequence）
            data_matrix = np.array([sequences_dict[key] for key in sequence_keys])
            if data_matrix.ndim != 2:
                await self.log.aerror(
                    "输入数据无法组成二维数组",
                    data_matrix_shape=data_matrix.shape,
                    emoji="❌",
                )
                raise ValueError("输入数据无法组成二维数组")
            if np.any(np.isnan(data_matrix)):
                await self.log.aerror(
                    "输入数据包含 NaN（非数字）",
                    data_matrix_has_nan=bool(np.any(np.isnan(data_matrix))),
                    emoji="❌",
                )
                raise ValueError("输入数据包含 NaN（非数字）")
            # np.corrcoef 默认 rowvar=True，sequence_keys 顺序与行顺序一致
            pearson_matrix = np.corrcoef(data_matrix)
        except Exception as exc:
            await self.log.aerror(
                "批量计算皮尔逊相关系数矩阵失败",
                error=str(exc),
                emoji="❌",
                exc_info=True,
            )
            raise

        correlation_matrix: Dict[T, Dict[T, float]] = {}
        for i, key_i in enumerate(sequence_keys):
            correlation_matrix[key_i] = {}
            for j, key_j in enumerate(sequence_keys):
                correlation_matrix[key_i][key_j] = float(pearson_matrix[i, j])
            await self.log.adebug(
                "已完成一行皮尔逊相关系数计算",
                row_key=key_i,
                row_values=correlation_matrix[key_i],
                emoji="✅",
            )
        await self.log.ainfo(
            "皮尔逊相关系数矩阵计算完成",
            emoji="🎉",
        )
        await self.log.ainfo(
            event="退出 compute_pearson_correlation_matrix 方法",
            matrix_size=len(correlation_matrix),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return correlation_matrix

    async def _calculate_pairwise_correlation(
        self,
        series_a: List[float],
        series_b: List[float],
    ) -> float:
        await self.log.ainfo(
            event="进入 _calculate_pairwise_correlation 方法",
            series_a_length=len(series_a),
            series_b_length=len(series_b),
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            "开始计算单对序列相关系数",
            series_a=series_a,
            series_b=series_b,
            emoji="📊",
        )

        if len(series_a) != len(series_b):
            await self.log.aerror(
                "序列长度不一致",
                series_a_len=len(series_a),
                series_b_len=len(series_b),
                emoji="❌",
            )
            raise ValueError("序列长度不一致")

        arr_a = np.array(series_a)
        arr_b = np.array(series_b)
        if arr_a.size == 0 or arr_b.size == 0:
            await self.log.aerror(
                "输入序列为空",
                arr_a_size=arr_a.size,
                arr_b_size=arr_b.size,
                emoji="❌",
            )
            raise ValueError("输入序列为空")
        if np.isnan(arr_a).any() or np.isnan(arr_b).any():
            await self.log.aerror(
                "参与计算的序列中包含 NaN（非数字）",
                arr_a_has_nan=bool(np.isnan(arr_a).any()),
                arr_b_has_nan=bool(np.isnan(arr_b).any()),
                emoji="❌",
            )
            raise ValueError("参与计算的序列中包含 NaN（非数字）")
        correlation = float(np.corrcoef(arr_a, arr_b)[0, 1])
        await self.log.adebug(
            "单对序列相关系数计算完成",
            correlation=correlation,
            emoji="📈",
        )
        await self.log.ainfo(
            event="退出 _calculate_pairwise_correlation 方法",
            correlation=correlation,
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return correlation

    async def get_correlation_local(
        self,
        target_alpha_id: str,
        others_alpha_ids: Optional[List[str]],
    ) -> Dict[str, float]:
        await self.log.ainfo(
            event="进入 get_correlation_local 方法",
            target_alpha_id=target_alpha_id,
            others_alpha_ids_count=len(others_alpha_ids) if others_alpha_ids else 0,
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            "开始获取本地相关系数",
            target_alpha_id=target_alpha_id,
            others_alpha_ids=others_alpha_ids,
            emoji="🔍",
        )

        correlation_dict: Dict[str, float] = {}

        if others_alpha_ids is None or len(others_alpha_ids) == 0:
            # 其他 Alpha ID 为空，获取生产相关系数，生产相关系数不提供关系对信息
            try:
                correlation_result: Optional[Correlation] = (
                    await correlation_dal.find_one_by(
                        alpha_id_a=target_alpha_id,
                        calc_type=CorrelationCalcType.PLATFORM_PROD,
                    )
                )
                if correlation_result is None:
                    await self.log.aerror(
                        "获取生产相关系数失败，未找到对应记录",
                        target_alpha_id=target_alpha_id,
                        others_alpha_ids=others_alpha_ids,
                        emoji="❌",
                    )
                    raise ValueError("获取生产相关系数失败，未找到对应记录")
                correlation_dict = {target_alpha_id: correlation_result.correlation}
                await self.log.ainfo(
                    "获取生产相关系数成功",
                    target_alpha_id=target_alpha_id,
                    correlation=correlation_result.correlation,
                    emoji="✅",
                )
                await self.log.ainfo(
                    event="退出 get_correlation_local 方法",
                    correlation_count=len(correlation_dict),
                    emoji=LoggingEmoji.STEP_OUT_FUNC.value,
                )
                return correlation_dict
            except Exception as exc:
                await self.log.aerror(
                    "获取生产相关系数异常",
                    target_alpha_id=target_alpha_id,
                    others_alpha_ids=others_alpha_ids,
                    error=str(exc),
                    emoji="❌",
                    exc_info=True,
                )
                raise ValueError("获取生产相关系数异常") from exc

        # 其他 Alpha ID 不为空，获取自相关系数
        all_correlations: List[Correlation] = []
        try:
            async with session_manager.get_session(Database.EVALUATE) as session:
                correlations: List[Correlation] = await correlation_dal.find_by(
                    Correlation.alpha_id_a == target_alpha_id,
                    Correlation.calc_type == CorrelationCalcType.PLATFORM_SELF,
                    Correlation.alpha_id_b.in_(others_alpha_ids),
                    session=session,
                )
                all_correlations.extend(correlations)

                correlations = await correlation_dal.find_by(
                    Correlation.alpha_id_a.in_(others_alpha_ids),
                    Correlation.alpha_id_b == target_alpha_id,
                    Correlation.calc_type == CorrelationCalcType.PLATFORM_SELF,
                    session=session,
                )
                all_correlations.extend(correlations)
        except Exception as exc:
            await self.log.aerror(
                "数据库查询自相关系数异常",
                target_alpha_id=target_alpha_id,
                others_alpha_ids=others_alpha_ids,
                error=str(exc),
                emoji="❌",
                exc_info=True,
            )
            raise ValueError("数据库查询自相关系数异常") from exc

        if len(all_correlations) == 0:
            await self.log.awarning(
                "未找到自相关系数记录",
                target_alpha_id=target_alpha_id,
                others_alpha_ids=others_alpha_ids,
                emoji="⚠️",
            )
            raise ValueError("未找到自相关系数记录")

        try:
            # 明确断言 alpha_id_b 不为 None，保证类型安全
            correlation_dict = {
                str(correlation.alpha_id_b): correlation.correlation
                for correlation in all_correlations
                if correlation.alpha_id_b is not None
            }
            await self.log.ainfo(
                "获取自相关系数成功",
                target_alpha_id=target_alpha_id,
                others_alpha_ids=others_alpha_ids,
                correlation_dict=correlation_dict,
                emoji="✅",
            )
        except Exception as exc:
            await self.log.aerror(
                "处理自相关系数结果异常",
                target_alpha_id=target_alpha_id,
                others_alpha_ids=others_alpha_ids,
                error=str(exc),
                emoji="❌",
                exc_info=True,
            )
            raise ValueError("处理自相关系数结果异常") from exc

        await self.log.ainfo(
            event="退出 get_correlation_local 方法",
            correlation_count=len(correlation_dict),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return correlation_dict

    @async_exception_handler
    async def get_correlation_platform(
        self,
        target_alpha_id: str,
        corr_type: CorrelationCalcType,
    ) -> TableView:
        await self.log.ainfo(
            event="进入 get_correlation_platform 方法",
            target_alpha_id=target_alpha_id,
            corr_type=corr_type.name,
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        if corr_type not in (
            CorrelationCalcType.PLATFORM_PROD,
            CorrelationCalcType.PLATFORM_SELF,
        ):
            await self.log.aerror(
                "不支持的相关系数类型",
                target_alpha_id=target_alpha_id,
                corr_type=corr_type,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("不支持的相关系数类型")

        await self.log.adebug(
            "开始获取平台相关系数",
            target_alpha_id=target_alpha_id,
            corr_type=corr_type,
            emoji=LoggingEmoji.DEBUG.value,
        )

        corr_table_data: TableView = await self.brain_client.fetch_alpha_correlation(
            alpha_id=target_alpha_id,
            correlation_type=(
                CorrelationType.PROD
                if corr_type == CorrelationCalcType.PLATFORM_PROD
                else CorrelationType.SELF
            ),
        )

        if corr_table_data is None:
            await self.log.aerror(
                "获取平台相关系数失败，未找到对应记录",
                target_alpha_id=target_alpha_id,
                corr_type=corr_type,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("获取平台相关系数失败，未找到对应记录")

        await self.log.ainfo(
            event="退出 get_correlation_platform 方法",
            table_data_available=corr_table_data is not None,
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return corr_table_data

    @async_exception_handler
    async def build_self_correlation_from_table(
        self,
        target_alpha_id: str,
        corr_table_data: TableView,
    ) -> SelfCorrelationView:
        await self.log.ainfo(
            event="进入 build_self_correlation_from_table 方法",
            target_alpha_id=target_alpha_id,
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        # 从平台相关系数表中构建自相关系数
        await self.log.adebug(
            "开始从平台相关系数表中构建自相关系数",
            target_alpha_id=target_alpha_id,
            corr_table_data=corr_table_data,
            emoji=LoggingEmoji.DEBUG.value,
        )

        if not isinstance(corr_table_data, TableView):
            await self.log.aerror(
                "输入数据不是有效的表格数据",
                target_alpha_id=target_alpha_id,
                corr_table_data=corr_table_data,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("输入数据不是有效的表格数据")

        correlation_items: List[SelfCorrelationView.CorrelationItem] = []

        data_df: Optional[pd.DataFrame] = corr_table_data.to_dataframe()
        if data_df is None or data_df.empty:
            await self.log.aerror(
                "平台相关系数表格数据为空",
                target_alpha_id=target_alpha_id,
                corr_table_data=corr_table_data,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("平台相关系数表格数据为空")

        for _, row in data_df.iterrows():
            correlation_item: SelfCorrelationView.CorrelationItem = (
                SelfCorrelationView.CorrelationItem(
                    alpha_id=row["id"],
                    correlation=row["correlation"],
                )
            )
            correlation_items.append(correlation_item)

        self_correlation_view: SelfCorrelationView = SelfCorrelationView(
            alpha_id=target_alpha_id,
            correlations=correlation_items,
            min=corr_table_data.min if corr_table_data.min is not None else 0.0,
            max=corr_table_data.max if corr_table_data.max is not None else 0.0,
        )

        await self.log.ainfo(
            "自相关系数构建完成",
            alpha_id=target_alpha_id,
            correlations=correlation_items,
            min=self_correlation_view.min,
            max=self_correlation_view.max,
            emoji=LoggingEmoji.INFO.value,
        )
        await self.log.ainfo(
            event="退出 build_self_correlation_from_table 方法",
            correlation_count=len(self_correlation_view.correlations),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return self_correlation_view

    @async_exception_handler
    async def build_prod_correlation_from_table(
        self,
        target_alpha_id: str,
        corr_table_data: TableView,
    ) -> ProdCorrelationView:
        await self.log.ainfo(
            event="进入 build_prod_correlation_from_table 方法",
            target_alpha_id=target_alpha_id,
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        # 从平台相关系数表中构建生产相关系数
        await self.log.adebug(
            "开始从平台相关系数表中构建生产相关系数",
            target_alpha_id=target_alpha_id,
            corr_table_data=corr_table_data,
            emoji=LoggingEmoji.DEBUG.value,
        )

        if not isinstance(corr_table_data, TableView):
            await self.log.aerror(
                "输入数据不是有效的表格数据",
                target_alpha_id=target_alpha_id,
                corr_table_data=corr_table_data,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("输入数据不是有效的表格数据")

        correlation_intervals: List[ProdCorrelationView.CorrelationInterval] = []

        data_df: Optional[pd.DataFrame] = corr_table_data.to_dataframe()
        if data_df is None or data_df.empty:
            await self.log.aerror(
                "平台相关系数表格数据为空",
                target_alpha_id=target_alpha_id,
                corr_table_data=corr_table_data,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("平台相关系数表格数据为空")

        for _, row in data_df.iterrows():
            correlation_interval: ProdCorrelationView.CorrelationInterval = (
                ProdCorrelationView.CorrelationInterval(
                    lower=row["min"],
                    upper=row["max"],
                    alphas=row["alphas"],
                )
            )
            correlation_intervals.append(correlation_interval)

        prod_correlation_view: ProdCorrelationView = ProdCorrelationView(
            alpha_id=target_alpha_id,
            intervals=correlation_intervals,
            min=corr_table_data.min if corr_table_data.min is not None else 0.0,
            max=corr_table_data.max if corr_table_data.max is not None else 0.0,
        )

        await self.log.ainfo(
            "生产相关系数构建完成",
            alpha_id=target_alpha_id,
            intervals=prod_correlation_view.intervals,
            min=prod_correlation_view.min,
            max=prod_correlation_view.max,
            emoji=LoggingEmoji.INFO.value,
        )
        await self.log.ainfo(
            event="退出 build_prod_correlation_from_table 方法",
            interval_count=len(prod_correlation_view.intervals),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return prod_correlation_view

    @staticmethod
    def find_closest_to_zero_correlation_chunk(
        corr_values: np.ndarray,  # 相关系数矩阵的 numpy 数组
        indices_chunk: List[
            Tuple[int, ...]
        ],  # 组合索引的列表，每个元素为一个组合（元组）
        submatrix_size: int,  # 子矩阵大小
    ) -> Tuple[Tuple[int, ...], float]:
        # 在一批组合中寻找最大相关系数最接近 0 的组合
        closest_to_zero_corr: float = float("inf")  # 最接近 0 的相关系数
        optimal_indices: Tuple[int, ...] = ()  # 最优组合的索引

        for indices in indices_chunk:
            sub_corr: np.ndarray = corr_values[np.ix_(indices, indices)]
            # mask: 非对角线掩码，True 表示非对角元素
            mask: np.ndarray = ~np.eye(submatrix_size, dtype=bool)
            non_diag_abs: np.ndarray = np.abs(sub_corr[mask])  # 非对角线元素的绝对值
            if non_diag_abs.size == 0:
                max_corr = 0.0  # 如果没有非对角元素，最大相关系数为 0
            else:
                max_corr = float(np.max(non_diag_abs))  # 获取非对角元素的最大值

            # 更新最优结果
            if abs(max_corr) < abs(closest_to_zero_corr):
                closest_to_zero_corr = max_corr
                optimal_indices = indices

        return optimal_indices, closest_to_zero_corr

    @async_timed
    async def find_least_relavant_submatrix(
        self,
        correlation_matrix: pd.DataFrame,  # 相关系数矩阵，元素类型为 float，行列索引为 T 类型
        submatrix_size: int,  # 子矩阵大小
        max_matrix_size: int = 20,  # 输入矩阵最大允许维度，防止穷举爆炸
        chunk_size: int = 1000,  # 每个子进程处理的组合数量
        max_workers: int = 4,  # 最大进程数
    ) -> Tuple[Set[T], float]:
        await self.log.ainfo(
            event="进入 find_least_relavant_submatrix 方法",
            matrix_shape=correlation_matrix.shape,
            submatrix_size=submatrix_size,
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            "开始寻找最小相关性的子矩阵",
            correlation_matrix_shape=correlation_matrix.shape,
            submatrix_size=submatrix_size,
            max_matrix_size=max_matrix_size,
            chunk_size=chunk_size,
            emoji="🔎",
        )

        matrix_dim: int = correlation_matrix.shape[0]
        if submatrix_size > matrix_dim:
            await self.log.aerror(
                "子矩阵大小超过原矩阵维度",
                submatrix_size=submatrix_size,
                matrix_shape=correlation_matrix.shape,
                emoji="❌",
            )
            raise ValueError("子矩阵大小超过原矩阵维度")

        if matrix_dim > max_matrix_size:
            await self.log.aerror(
                "输入矩阵维度过大，穷举组合数量过多，拒绝计算",
                matrix_dim=matrix_dim,
                max_matrix_size=max_matrix_size,
                emoji="🚫",
            )
            raise ValueError(
                f"输入矩阵维度过大（{matrix_dim}），最大支持 {max_matrix_size}，"
                "请缩小输入规模"
            )

        indices: List[T] = list(correlation_matrix.index)
        corr_values: np.ndarray = correlation_matrix.values  # 转为 numpy 数组，提升性能
        index_to_pos: Dict[T, int] = {idx: pos for pos, idx in enumerate(indices)}
        pos_to_index: Dict[int, T] = {pos: idx for idx, pos in index_to_pos.items()}

        all_positions: List[int] = list(range(len(indices)))

        # 生成所有组合并分片
        combinations_iter = list(combinations(all_positions, submatrix_size))
        total_combinations: int = len(combinations_iter)
        await self.log.adebug(
            "穷举组合总数",
            total_combinations=total_combinations,
            emoji="🔢",
        )
        num_chunks: int = ceil(total_combinations / chunk_size)
        chunks: List[List[Tuple[int, ...]]] = [
            combinations_iter[i * chunk_size : (i + 1) * chunk_size]
            for i in range(num_chunks)
        ]

        await self.log.adebug(
            "分片任务数",
            num_chunks=num_chunks,
            chunk_size=chunk_size,
            emoji="🧩",
        )

        closest_to_zero_corr: float = float("inf")
        optimal_indices: Set[T] = set()

        # 使用 run_in_executor 实现异步多进程，分片任务
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            tasks = [
                loop.run_in_executor(
                    executor,
                    CorrelationManager.find_closest_to_zero_correlation_chunk,
                    corr_values,
                    chunk,
                    submatrix_size,
                )
                for chunk in chunks
            ]
            for fut in asyncio.as_completed(tasks):
                indices_tuple, max_corr = await fut
                indices_set = {pos_to_index[pos] for pos in indices_tuple}
                await self.log.adebug(
                    "分片子矩阵相关性分析完成",
                    indices=indices_set,
                    max_corr=max_corr,
                    emoji="🧩",
                )
                # 更新最优结果
                if abs(max_corr) < abs(closest_to_zero_corr):
                    closest_to_zero_corr = max_corr
                    optimal_indices = indices_set

        await self.log.ainfo(
            "最小相关性子矩阵搜索完成",
            optimal_indices=optimal_indices,
            closest_to_zero_corr=closest_to_zero_corr,
            emoji="🏆",
        )
        await self.log.ainfo(
            event="退出 find_least_relavant_submatrix 方法",
            optimal_indices=optimal_indices,
            closest_to_zero_corr=closest_to_zero_corr,
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return optimal_indices, closest_to_zero_corr
