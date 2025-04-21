"""Alpha 评估器 (Evaluator) 的抽象基类定义。

此模块定义了 `AbstractEvaluator` 抽象基类 (Abstract Base Class, ABC)，
负责定义 Alpha 的评估流程，包括决定执行哪些检查以及如何执行这些检查。
它依赖于一个 `AbstractAlphaFetcher` 实例来获取待评估的 Alpha。

子类必须实现此基类中定义的所有抽象方法，以提供具体的业务逻辑。
"""

from __future__ import annotations  # 解决类型前向引用问题

import abc
from typing import Any, AsyncGenerator

from alphapower.constants import RefreshPolicy
from alphapower.dal.evaluate import EvaluateRecordDAL
from alphapower.entity import Alpha, EvaluateRecord

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .evaluate_stage_abc import AbstractEvaluateStage

# SQLAlchemy 列元素，用于构建数据库查询条件


class AbstractEvaluator(abc.ABC):
    """Alpha 评估器的抽象基类 (基于实例)。

    定义了 Alpha 评估流程的标准接口，包括批量评估、单个评估、
    确定检查项以及执行检查。通过构造函数注入依赖的 `AbstractAlphaFetcher` 实例
    和所需的 DAL (Data Access Layer, 数据访问层) 对象。
    子类需要实现具体的评估逻辑和检查逻辑。
    """

    def __init__(
        self,
        fetcher: AbstractAlphaFetcher,
        evaluate_stage_chain: AbstractEvaluateStage,
        evluate_record_dal: EvaluateRecordDAL,
    ):
        """初始化 Evaluator。

        Args:
            fetcher: 用于获取 Alpha 的数据获取器实例。
            correlation_dal: Correlation 数据访问层对象。
            check_record_dal: CheckRecord 数据访问层对象。
        """
        self.fetcher: AbstractAlphaFetcher = fetcher
        self.evaluate_stage_chain: AbstractEvaluateStage = evaluate_stage_chain
        self.evaluate_record_dal: EvaluateRecordDAL = evluate_record_dal

    @abc.abstractmethod
    async def evaluate_many(
        self,
        policy: RefreshPolicy,
        concurrency: int,
        **kwargs: Any,
    ) -> AsyncGenerator[Alpha, None]:
        """异步批量评估通过 `fetcher` 获取的 Alpha。

        此方法应使用注入的 `self.fetcher.fetch_alphas(**kwargs)` 获取 Alpha，
        然后使用指定的并发数 (`concurrency`) 并发地调用 `self.evaluate_one` 对每个 Alpha 进行评估。

        Args:
            policy: 应用于本次批量评估中所有检查的默认刷新策略 (RefreshPolicy)。
                    注意：`_get_checks_to_run` 可以覆盖单个 Alpha 的策略。
            concurrency: 并发执行 `evaluate_one` 任务的最大数量。
            **kwargs: 传递给 `self.fetcher.fetch_alphas` 和 `self.evaluate_one` 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Yields:
            逐个返回已成功通过所有评估检查 (`evaluate_one` 返回 True) 的 `Alpha` 对象。
            评估失败或未通过检查的 Alpha 不会被返回。具体的检查结果通常由实现类记录。
        """
        # 确保 AsyncGenerator 被正确注解
        if False:  # pylint: disable=W0125
            yield  # pragma: no cover
        raise NotImplementedError("子类必须实现 evaluate_many 方法")

    @abc.abstractmethod
    async def evaluate_one(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        """异步评估单个 Alpha 对象。

        此方法的核心逻辑是：
        1. 调用 `self._get_checks_to_run` 确定需要对该 Alpha 执行哪些检查 (`AlphaCheckType`)
           以及本次评估使用的刷新策略 (`RefreshPolicy`)。
        2. 调用 `self._execute_checks` 执行这些检查。
        3. 根据 `_execute_checks` 的结果（通常是所有检查都通过）返回最终评估结果。

        Args:
            alpha: 需要评估的 `Alpha` 实体对象。
            policy: 默认的刷新策略 (RefreshPolicy)，可能被 `_get_checks_to_run` 覆盖。
            **kwargs: 传递给 `self._get_checks_to_run` 和 `self._execute_checks` 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            布尔值 (bool)，指示此 `Alpha` 是否通过了所有必需的评估检查。
            `True` 表示通过，`False` 表示未通过。详细的检查结果通常由实现类记录。
        """
        raise NotImplementedError("子类必须实现 evaluate_one 方法")

    @abc.abstractmethod
    async def _handle_evaluate_success(
        self,
        alpha: Alpha,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> None:
        """处理评估成功的 Alpha。
        此方法通常用于记录评估成功的 Alpha 及其检查结果。
        Args:
            alpha: 评估成功的 `Alpha` 实体对象。
            checks_ctx: 包含检查结果的上下文字典，键为 `CheckRecordType`，
                        值为对应的检查结果数据。
            checks: 评估过程中执行的检查类型列表。
            **kwargs: 传递给具体实现的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。
        """
        raise NotImplementedError("子类必须实现 _handle_evluate_success 方法")

    @abc.abstractmethod
    async def _handle_evaluate_failure(
        self,
        alpha: Alpha,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> None:
        """处理评估失败的 Alpha。
        此方法通常用于记录评估失败的 Alpha 及其检查结果。
        Args:
            alpha: 评估失败的 `Alpha` 实体对象。
            checks_ctx: 包含检查结果的上下文字典，键为 `CheckRecordType`，
                        值为对应的检查结果数据。
            checks: 评估过程中执行的检查类型列表。
            **kwargs: 传递给具体实现的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。
        """
        raise NotImplementedError("子类必须实现 _handle_evaluate_failure 方法")

    @abc.abstractmethod
    async def to_evaluate_alpha_count(
        self,
        **kwargs: Any,
    ) -> int:
        """获取待评估的 Alpha 总数量。

        此方法通常直接委托给注入的 `self.fetcher.total_alpha_count(**kwargs)` 来获取计数。

        Args:
            **kwargs: 传递给 `self.fetcher.total_alpha_count` 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            符合 `fetcher` 筛选条件的 Alpha 实体总数。
        """
        raise NotImplementedError("子类必须实现 to_evaluate_alpha_count 方法")
