from __future__ import annotations  # 解决类型前向引用问题

from typing import Any, List, Tuple

from alphapower.client import BeforeAndAfterPerformanceView
from alphapower.constants import CheckRecordType, RefreshPolicy
from alphapower.entity import Alpha
from alphapower.internal.logging import get_logger

from .base_evaluator import BaseEvaluator

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class ConsultantEvaluator(BaseEvaluator):
    """
    ConsultantEvaluator 是 BaseEvaluator 的子类，
    专门用于实现顾问相关的 Alpha 评估逻辑。
    """

    async def _get_checks_to_run(
        self, alpha: Alpha, **kwargs: Any
    ) -> Tuple[List[CheckRecordType], RefreshPolicy]:
        return (
            [
                CheckRecordType.CORRELATION_SELF,
                CheckRecordType.CORRELATION_PROD,
                CheckRecordType.SUBMISSION,
            ],
            RefreshPolicy.USE_EXISTING,
        )

    async def _determine_performance_diff_pass_status(
        self,
        alpha: Alpha,
        perf_diff_view: BeforeAndAfterPerformanceView,
        **kwargs: Any,
    ) -> bool:
        """
        默认不执行这个检查
        """
        raise NotImplementedError(
            "子类必须实现 _determine_performance_diff_pass_status 方法"
        )
