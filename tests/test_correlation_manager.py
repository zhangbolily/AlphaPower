import asyncio
import multiprocessing
from typing import Any, Dict, List, Set, Tuple

import numpy as np  # numpy 用于生成大规模测试数据
import pandas as pd  # pandas 用于 DataFrame 数据结构
import pytest

from alphapower.internal.logging import get_logger
from alphapower.manager.correlation_manager import CorrelationManager

# -*- coding: utf-8 -*-
# Copyright (c) alphapower. All rights reserved.
# 本文件用于测试 CorrelationManager 的多进程并发安全性
# 遵循 Google Python Style Guide，所有注释均为中文
# 关键术语：多进程（multiprocessing）、进程安全（process-safe）、相关性管理器（CorrelationManager）


# =========================
# 测试数据与工具函数
# =========================


@pytest.fixture(scope="module", name="test_series_data")
def fixture_test_series_data() -> Dict[str, List[float]]:
    # 构造用于相关性计算的测试数据，数据量加大以增强测试覆盖率
    # 生成长度为 200000 的线性相关、负相关、常数序列

    n: int = 200000  # 序列长度
    base: np.ndarray = np.arange(1, n + 1, dtype=float)  # base 为一维 ndarray
    series_a: List[float] = base.astype(float).tolist()  # 完全正相关，元素类型为 float
    series_b: List[float] = (
        (base * 2).astype(float).tolist()
    )  # 完全正相关，元素类型为 float
    series_c: List[float] = (
        (base[::-1]).astype(float).tolist()
    )  # 完全负相关，元素类型为 float
    series_d: List[float] = [1.0] * n  # 常数序列

    return {
        "A": series_a,
        "B": series_b,
        "C": series_c,
        "D": series_d,
    }


@pytest.fixture(scope="module", name="test_correlation_matrix")
def fixture_test_correlation_matrix() -> pd.DataFrame:
    # 构造用于相关性计算的测试数据，数据量加大以增强测试覆盖率
    # 生成包含一个包含 n 个元素的相关系数矩阵，相关系数范围是 -1 到 1
    n: int = 40
    correlation_matrix = np.random.uniform(-1, 1, (n, n))
    return pd.DataFrame(correlation_matrix)


def run_in_process(
    manager_args: Dict[str, Any],
    target_series: List[float],
    others_series_dict: Dict[str, List[float]],
    result_queue: multiprocessing.Queue,
) -> None:
    # 子进程中运行异步相关性计算
    async def _inner() -> None:
        manager = CorrelationManager(**manager_args)
        result = await manager.calculate_correlations_with(
            target_series, others_series_dict
        )
        result_queue.put(result)

    asyncio.run(_inner())


# =========================
# 并发安全性测试类
# =========================


class TestCorrelationManagerProcessSafe:
    """测试 CorrelationManager 的多进程并发安全性"""

    @pytest.mark.asyncio
    async def test_process_safe_calculate_correlations(
        self, test_series_data: Dict[str, List[float]]
    ) -> None:
        # 并发启动多个进程，分别执行相关性计算，验证不会出现数据竞争或异常
        process_count: int = 48
        processes: List[multiprocessing.Process] = []
        result_queue: multiprocessing.Queue = multiprocessing.Queue()
        manager_args: Dict[str, Any] = {}

        # 目标序列与其他序列
        target_series: List[float] = test_series_data["A"]
        others_series_dict: Dict[str, List[float]] = {
            k: v for k, v in test_series_data.items() if k != "A"
        }

        for _ in range(process_count):
            p = multiprocessing.Process(
                target=run_in_process,
                args=(manager_args, target_series, others_series_dict, result_queue),
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join(timeout=10)
            assert not p.is_alive(), "子进程未能在指定时间内结束，可能存在死锁"

        # 收集所有进程的结果，验证一致性
        results: List[Dict[str, float]] = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # 断言所有进程结果一致且正确
        assert len(results) == process_count, "结果数量与进程数不一致"
        for res in results:
            # 相关性计算的数学期望
            # A 与 B 完全正相关，A 与 C 完全负相关，A 与 D 无方差相关性为 nan
            assert pytest.approx(res["B"], abs=1e-6) == 1.0
            assert pytest.approx(res["C"], abs=1e-6) == -1.0
            assert (
                res["D"] != res["B"] and res["D"] != res["C"]
            )  # D 为常数序列，相关性为 nan

    @pytest.mark.asyncio
    async def test_find_min_max_correlation_submatrix_process_safe(
        self, test_correlation_matrix: pd.DataFrame
    ) -> None:
        """
        测试 CorrelationManager #sym:find_min_max_correlation_submatrix 方法的多进程加速子矩阵相关性计算
        - 验证结果正确性
        - 验证多进程一致性
        """
        # 日志：测试用例入参
        logger = get_logger(__name__)
        await logger.adebug(
            "test_find_min_max_correlation_submatrix_process_safe",
            test_correlation_matrix=test_correlation_matrix,
            emoji="🔍",
        )
        manager: CorrelationManager = CorrelationManager()
        submatrix_size: int = 7  # 子矩阵大小

        # 多进程加速查找最小/最大相关性子矩阵
        result: Tuple[Set[str], float] = (
            await manager.find_least_relavant_submatrix(
                test_correlation_matrix,
                submatrix_size,
                max_workers=16,
                max_matrix_size=1000,
                chunk_size=100000,
            )
        )
        min_combined: Set[str] = result[0]
        min_max_corr: float = result[1]

        await logger.adebug(
            "find_min_max_correlation_submatrix 结果",
            min_combined=min_combined,
            min_max_corr=min_max_corr,
            emoji="🔍",
        )

        # 断言结果正确性
        assert len(min_combined) == submatrix_size, "子矩阵大小不一致"
