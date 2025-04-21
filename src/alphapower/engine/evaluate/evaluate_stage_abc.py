from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Optional, Tuple

from structlog.stdlib import BoundLogger

from alphapower.constants import RefreshPolicy
from alphapower.entity import Alpha, CheckRecord, EvaluateRecord
from alphapower.internal.logging import get_logger

log: BoundLogger = get_logger(module_name=__name__)


class AbstractEvaluateStage(ABC):
    """
    抽象评估阶段类，使用责任链模式实现评估环节的封装。
    """

    class CheckAction(Enum):
        """指示检查数据时应执行的操作"""

        REFRESH = auto()  # 需要刷新数据
        USE_EXISTING = auto()  # 使用已存在的记录
        SKIP = auto()  # 根据策略跳过检查 (当记录不存在时)
        FAIL_MISSING = auto()  # 因记录不存在且策略不允许刷新而失败
        ERROR = auto()  # 无效的策略或状态组合

    def __init__(self, next_stage: Optional["AbstractEvaluateStage"] = None) -> None:
        """
        初始化评估阶段。

        Args:
            next_stage: 下一个评估阶段 (责任链中的下一个节点)。
        """
        self._next_stage = next_stage

    @property
    def next_stage(self) -> Optional["AbstractEvaluateStage"]:
        """
        获取下一个评估阶段。

        Returns:
            下一个评估阶段 (责任链中的下一个节点)。
        """
        return self._next_stage

    @next_stage.setter
    def next_stage(self, next_stage: Optional["AbstractEvaluateStage"]) -> None:
        """
        设置下一个评估阶段。

        Args:
            next_stage: 下一个评估阶段 (责任链中的下一个节点)。
        """
        self._next_stage = next_stage

    async def evaluate(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> Tuple[EvaluateRecord, bool]:
        """
        执行当前阶段的评估逻辑，并传递到下一个阶段。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            record: 当前评估的记录对象 (EvaluateRecord)。
            kwargs: 其他参数。

        Returns:
            EvaluateRecord: 更新后的评估记录对象。
        """
        try:
            # 调用当前阶段的评估逻辑
            if not await self._evaluate_stage(alpha, policy, record, **kwargs):
                # 如果当前阶段评估失败，记录失败信息
                await log.aerror(
                    "评估阶段失败",
                    emoji="❌",
                    stage=self.__class__.__name__,
                    alpha_id=alpha.alpha_id,
                    policy=policy.name,
                )
                return record, False

            # 如果存在下一个阶段，继续传递
            if self._next_stage:
                return await self._next_stage.evaluate(alpha, policy, record, **kwargs)

        except Exception as e:
            # 捕获异常并记录错误日志
            await log.aerror(
                "评估阶段发生异常",
                emoji="💥",
                stage=self.__class__.__name__,
                alpha_id=alpha.alpha_id,
                policy=policy.name,
                error=str(e),
                exc_info=True,  # 包含异常堆栈信息
            )
            raise

        return record, True

    async def _determine_check_action(
        self,
        policy: RefreshPolicy,
        exist_check_record: Optional[CheckRecord],
        alpha_id: str,
        check_type_name: str,
    ) -> CheckAction:
        """
        根据刷新策略和现有检查记录，决定应执行的操作。

        Args:
            policy: 刷新策略。
            exist_check_record: 数据库中存在的检查记录，如果不存在则为 None。
            alpha_id: 正在检查的 Alpha 的 ID。
            check_type_name: 正在执行的检查类型名称 (用于日志)。

        Returns:
            应执行的检查操作 (CheckAction)。
        """
        await log.adebug(
            f"开始判断 {check_type_name} 检查操作",
            emoji="🤔",
            alpha_id=alpha_id,
            policy=policy,
            record_exists=bool(exist_check_record),
        )
        action: AbstractEvaluateStage.CheckAction

        if policy == RefreshPolicy.FORCE_REFRESH:
            action = AbstractEvaluateStage.CheckAction.REFRESH
            await log.adebug(
                f"策略为强制刷新，动作：刷新 {check_type_name} 数据",
                emoji="🔄",
                alpha_id=alpha_id,
            )
        elif policy == RefreshPolicy.REFRESH_ASYNC_IF_MISSING:
            if not exist_check_record:
                action = AbstractEvaluateStage.CheckAction.REFRESH
                await log.adebug(
                    f"策略为缺失时刷新且记录不存在，动作：刷新 {check_type_name} 数据",
                    emoji="🔄",
                    alpha_id=alpha_id,
                )
            else:
                action = AbstractEvaluateStage.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为缺失时刷新且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
        elif policy == RefreshPolicy.USE_EXISTING:
            if exist_check_record:
                action = AbstractEvaluateStage.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为仅使用现有且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
            else:
                action = AbstractEvaluateStage.CheckAction.FAIL_MISSING
                await log.ainfo(
                    f"策略为仅使用现有但记录不存在，动作：{check_type_name} 检查失败",
                    emoji="🚫",
                    alpha_id=alpha_id,
                )
        elif policy == RefreshPolicy.SKIP_IF_MISSING:
            if exist_check_record:
                action = AbstractEvaluateStage.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为缺失时跳过且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
            else:
                action = AbstractEvaluateStage.CheckAction.SKIP
                await log.ainfo(
                    f"策略为缺失时跳过且记录不存在，动作：跳过 {check_type_name} 检查",
                    emoji="⏭️",
                    alpha_id=alpha_id,
                )
        else:
            action = AbstractEvaluateStage.CheckAction.ERROR
            await log.aerror(
                f"无效的 {check_type_name} 检查策略",
                emoji="❌",
                alpha_id=alpha_id,
                policy=policy,
                record_exists=bool(exist_check_record),
            )
            # 可以在这里抛出异常，或者让调用方处理 ERROR 状态
            # raise ValueError(f"不支持的 {check_type_name} 检查策略 '{policy}'")

        await log.adebug(
            f"结束判断 {check_type_name} 检查操作",
            emoji="🏁",
            alpha_id=alpha_id,
            action=action.name,
        )
        return action

    @abstractmethod
    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        当前阶段的具体评估逻辑，由子类实现。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            checks_ctx: 检查上下文，用于存储和共享检查结果。
            record: 当前评估的记录对象 (EvaluateRecord)。
            kwargs: 其他参数。
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} 必须实现 _evaluate_stage 方法。"
        )
