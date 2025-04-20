"""Alpha 评估器 (Evaluator) 的抽象基类定义。

此模块定义了 `AbstractEvaluator` 抽象基类 (Abstract Base Class, ABC)，
负责定义 Alpha 的评估流程，包括决定执行哪些检查以及如何执行这些检查。
它依赖于一个 `AbstractAlphaFetcher` 实例来获取待评估的 Alpha。

子类必须实现此基类中定义的所有抽象方法，以提供具体的业务逻辑。
"""

from __future__ import annotations  # 解决类型前向引用问题

import abc
from typing import Any, AsyncGenerator, Dict, List, Tuple

from alphapower.client import WorldQuantClient
from alphapower.constants import CheckRecordType, CorrelationType, RefreshPolicy
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.entity import Alpha

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .self_correlation_calculator import SelfCorrelationCalculator

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
        correlation_dal: CorrelationDAL,
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
        correlation_calculator: SelfCorrelationCalculator,
    ):
        """初始化 Evaluator。

        Args:
            fetcher: 用于获取 Alpha 的数据获取器实例。
            correlation_dal: Correlation 数据访问层对象。
            check_record_dal: CheckRecord 数据访问层对象。
        """
        self.fetcher = fetcher
        self.correlation_dal = correlation_dal
        self.check_record_dal = check_record_dal
        self.client = client
        self.correlation_calculator = correlation_calculator

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

    @abc.abstractmethod
    async def _get_checks_to_run(
        self, alpha: Alpha, **kwargs: Any
    ) -> Tuple[List[CheckRecordType], Dict[str, Any], RefreshPolicy]:
        """确定针对给定 Alpha 需要运行的检查类型及应用的刷新策略。

        子类必须实现此方法，以根据 Alpha 的具体属性（例如状态 `status`、类型 `type`）
        和评估器的配置（可能通过 `kwargs` 传入），动态决定需要执行哪些检查 (`AlphaCheckType`)。
        同时，也需要决定这些检查应遵循何种刷新策略 (`RefreshPolicy`)，这可能会覆盖
        `evaluate_one` 或 `evaluate_many` 传入的默认策略。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            **kwargs: 包含可选参数的字典，可能用于动态决策。
                      例如: `evaluation_context: str = 'daily'`。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            一个元组 (Tuple)，包含两个元素：
            1. `List[AlphaCheckType]`: 需要针对此 Alpha 执行的检查类型枚举列表。
            2. `RefreshPolicy`: 应用于本次检查执行的刷新策略。
        """
        raise NotImplementedError("子类必须实现 _get_checks_to_run 方法")

    @abc.abstractmethod
    async def _execute_checks(
        self,
        alpha: Alpha,
        checks: List[CheckRecordType],
        checks_kwargs: Dict[str, Any],
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> Dict[CheckRecordType, bool]:
        """执行指定的检查类型列表，并返回各项检查的结果。

        此方法负责根据 `checks` 列表中的每个 `AlphaCheckType`，
        调用相应的具体检查方法（例如 `_check_correlation`, `_check_submission` 等）。
        它需要处理刷新策略 (`policy`)，决定是否需要强制重新计算检查结果。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            checks: 由 `_get_checks_to_run` 返回的需要执行的检查类型列表。
            policy: 由 `_get_checks_to_run` 返回的应用于本次检查的刷新策略。
            **kwargs: 传递给具体检查方法 (`_check_*`) 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            一个字典，键是执行的 `AlphaCheckType` 枚举成员，
            值是布尔值 (bool)，指示对应的检查是否通过 (`True`) 或失败 (`False`)。
        """
        raise NotImplementedError("子类必须实现 _execute_checks 方法")

    @abc.abstractmethod
    async def _check_correlation(
        self,
        alpha: Alpha,
        corr_type: CorrelationType,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        """执行 Alpha 与其他 Alpha 之间的相关性检查。

        子类应实现此方法以定义具体的相关性计算和阈值判断逻辑。
        需要根据 `corr_type`（例如与同一用户的其他 Alpha 或与因子池的 Alpha）
        和 `policy`（是否强制重新计算）来执行检查。
        可以使用注入的 `self.correlation_dal` 访问相关性数据。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            corr_type: 指定相关性检查的类型 (`CorrelationType` 枚举)。
            policy: 应用于本次检查的刷新策略 (`RefreshPolicy`)。
            **kwargs: 可能包含相关性计算所需的额外参数，例如相关性阈值
                      (`correlation_threshold: float = 0.7`) 或目标因子池 ID
                      (`target_pool_id: Optional[str] = None`)。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            布尔值 (bool)，指示 Alpha 是否通过了相关性检查（通常意味着相关性低于阈值）。
            `True` 表示通过，`False` 表示未通过（相关性过高）。
        """
        raise NotImplementedError("子类必须实现 _check_correlation 方法")

    @abc.abstractmethod
    async def _check_alpha_pool_performance_diff(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        """检查将此 Alpha 加入指定因子池 (Alpha Pool) 后，因子池业绩表现的前后差异。

        子类应实现此方法，模拟将 Alpha 加入因子池，计算加入前后的关键性能指标
        (Key Performance Indicators, KPIs) 差异，并判断差异是否在可接受范围内。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            competition_id: 目标竞争或因子池的 ID (`competition_id`)。如果为 `None`，
                              可能表示检查与某个默认或基准池的差异。
            policy: 应用于本次检查的刷新策略 (`RefreshPolicy`)。
            **kwargs: 可能包含性能差异计算所需的额外参数，例如性能指标阈值
                      (`sharpe_diff_threshold: float = -0.1`) 或回测周期
                      (`backtest_period: str = '1y'`)。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            布尔值 (bool)，指示 Alpha 加入因子池后的业绩表现差异是否符合要求。
            `True` 表示符合要求，`False` 表示不符合（例如导致因子池性能下降过多）。
        """
        raise NotImplementedError(
            "子类必须实现 _check_alpha_pool_performance_diff 方法"
        )

    @abc.abstractmethod
    async def _check_submission(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        """检查 Alpha 是否满足提交 (Submission) 的条件。

        子类应实现此方法，根据业务规则检查 Alpha 的状态、完整性或其他
        提交前必须满足的要求。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            policy: 应用于本次检查的刷新策略 (`RefreshPolicy`)。
            **kwargs: 可能包含提交检查所需的上下文参数，例如目标提交平台
                      (`target_platform: str = 'live'`) 或检查级别
                      (`check_level: str = 'strict'`)。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            布尔值 (bool)，指示 Alpha 是否满足所有提交要求。
            `True` 表示满足，`False` 表示不满足。
        """
        raise NotImplementedError("子类必须实现 _check_submission 方法")
