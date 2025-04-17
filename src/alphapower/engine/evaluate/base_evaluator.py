"""Alpha 数据获取器 (Fetcher) 与评估器 (Evaluator) 的基础实现。

此模块提供了 `AbstractAlphaFetcher` 和 `AbstractEvaluator` 抽象基类的
基础实现版本：`BaseAlphaFetcher` 和 `BaseEvaluator`。
这些基础类继承了抽象方法，但默认实现会抛出 `NotImplementedError`，
需要子类根据具体业务逻辑进行覆盖。
"""

from __future__ import annotations  # 解决类型前向引用问题

from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from alphapower.constants import AlphaCheckType, CorrelationType, RefreshPolicy
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.entity import Alpha
from alphapower.internal.logging import get_logger

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .evaluator_abc import AbstractEvaluator

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class BaseEvaluator(AbstractEvaluator):
    """Alpha 评估器的基础实现。

    继承自 `AbstractEvaluator`，为所有抽象方法提供了默认的
    `NotImplementedError` 实现。子类应覆盖这些方法以提供具体的
    评估和检查逻辑。
    """

    def __init__(
        self,
        fetcher: AbstractAlphaFetcher,
        correlation_dal: CorrelationDAL,
        check_record_dal: CheckRecordDAL,
    ):
        """初始化 BaseEvaluator。

        Args:
            fetcher: 用于获取 Alpha 的数据获取器实例。
            correlation_dal: Correlation 数据访问层对象。
            check_record_dal: CheckRecord 数据访问层对象。
        """
        super().__init__(fetcher, correlation_dal, check_record_dal)

    async def evaluate_many(
        self,
        policy: RefreshPolicy,
        concurrency: int,
        **kwargs: Dict[str, Any],
    ) -> AsyncGenerator[Alpha, None]:
        """异步批量评估通过 `fetcher` 获取的 Alpha (待实现)。

        子类应覆盖此方法，实现并发评估逻辑。

        Args:
            policy: 应用于本次批量评估中所有检查的默认刷新策略。
            concurrency: 并发执行 `evaluate_one` 任务的最大数量。
            **kwargs: 传递给 `self.fetcher.fetch_alphas` 和 `self.evaluate_one` 的参数字典。

        Yields:
            逐个返回已成功通过所有评估检查的 `Alpha` 对象。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 evaluate_many 方法尚未实现",
            emoji="🚧",
            policy=policy,
            concurrency=concurrency,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 evaluate_many 方法")
        # 确保 AsyncGenerator 被正确注解
        if False:  # pylint: disable=W0125 # pragma: no cover
            yield

    async def evaluate_one(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Dict[str, Any],
    ) -> bool:
        """异步评估单个 Alpha 对象 (待实现)。

        子类应覆盖此方法，实现调用检查逻辑并返回结果。

        Args:
            alpha: 需要评估的 `Alpha` 实体对象。
            policy: 默认的刷新策略。
            **kwargs: 传递给 `self._get_checks_to_run` 和 `self._execute_checks` 的参数字典。

        Returns:
            布尔值 (bool)，指示此 `Alpha` 是否通过了所有必需的评估检查。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 evaluate_one 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 evaluate_one 方法")

    async def to_evaluate_alpha_count(
        self,
        **kwargs: Dict[str, Any],
    ) -> int:
        """获取待评估的 Alpha 总数量。

        此方法委托给注入的 `self.fetcher.total_alpha_count`。

        Args:
            **kwargs: 传递给 `self.fetcher.total_alpha_count` 的参数字典。

        Returns:
            符合 `fetcher` 筛选条件的 Alpha 实体总数。
        """
        await log.debug("调用 fetcher 获取待评估 Alpha 总数", emoji="🔢", kwargs=kwargs)
        # 直接调用 fetcher 的方法
        count = await self.fetcher.total_alpha_count(**kwargs)
        await log.debug("获取到待评估 Alpha 总数", emoji="🔢", count=count)
        return count

    async def _get_checks_to_run(
        self, alpha: Alpha, **kwargs: Dict[str, Any]
    ) -> Tuple[List[AlphaCheckType], RefreshPolicy]:
        """确定针对给定 Alpha 需要运行的检查类型及应用的刷新策略 (待实现)。

        子类应覆盖此方法，根据 Alpha 属性和上下文决定检查项和策略。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            **kwargs: 包含可选参数的字典。

        Returns:
            一个元组，包含检查类型列表和刷新策略。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 _get_checks_to_run 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _get_checks_to_run 方法")

    async def _execute_checks(
        self,
        alpha: Alpha,
        checks: List[AlphaCheckType],
        policy: RefreshPolicy,
        **kwargs: Dict[str, Any],
    ) -> Dict[AlphaCheckType, bool]:
        """执行指定的检查类型列表，并返回各项检查的结果 (待实现)。

        子类应覆盖此方法，调用具体的检查方法并处理刷新策略。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            checks: 需要执行的检查类型列表。
            policy: 应用于本次检查的刷新策略。
            **kwargs: 传递给具体检查方法的参数字典。

        Returns:
            一个字典，键是执行的 `AlphaCheckType`，值是检查结果 (bool)。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 _execute_checks 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            checks=checks,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _execute_checks 方法")

    async def _check_correlation(
        self,
        alpha: Alpha,
        corr_type: CorrelationType,
        policy: RefreshPolicy,
        **kwargs: Dict[str, Any],
    ) -> bool:
        """执行 Alpha 与其他 Alpha 之间的相关性检查 (待实现)。

        子类应覆盖此方法，实现具体的相关性计算和判断逻辑。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            corr_type: 指定相关性检查的类型。
            policy: 应用于本次检查的刷新策略。
            **kwargs: 可能包含相关性计算所需的额外参数。

        Returns:
            布尔值 (bool)，指示 Alpha 是否通过了相关性检查。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 _check_correlation 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            corr_type=corr_type,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _check_correlation 方法")

    async def _check_alpha_pool_performance_diff(
        self,
        alpha: Alpha,
        competition_id: Optional[str],
        policy: RefreshPolicy,
        **kwargs: Dict[str, Any],
    ) -> bool:
        """检查将此 Alpha 加入指定因子池后，因子池业绩表现的前后差异 (待实现)。

        子类应覆盖此方法，实现性能差异的计算和判断逻辑。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            competition_id: 目标竞争或因子池的 ID。
            policy: 应用于本次检查的刷新策略。
            **kwargs: 可能包含性能差异计算所需的额外参数。

        Returns:
            布尔值 (bool)，指示 Alpha 加入因子池后的业绩表现差异是否符合要求。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 _check_alpha_pool_performance_diff 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            competition_id=competition_id,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError(
            "子类必须实现 _check_alpha_pool_performance_diff 方法"
        )

    async def _check_submission(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Dict[str, Any],
    ) -> bool:
        """检查 Alpha 是否满足提交 (Submission) 的条件 (待实现)。

        子类应覆盖此方法，实现提交条件的检查逻辑。

        Args:
            alpha: 当前正在评估的 `Alpha` 对象。
            policy: 应用于本次检查的刷新策略。
            **kwargs: 可能包含提交检查所需的上下文参数。

        Returns:
            布尔值 (bool)，指示 Alpha 是否满足所有提交要求。

        Raises:
            NotImplementedError: 此方法尚未在子类中实现。
        """
        await log.debug(
            "🚧 _check_submission 方法尚未实现",
            emoji="🚧",
            alpha_id=alpha.id,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _check_submission 方法")
