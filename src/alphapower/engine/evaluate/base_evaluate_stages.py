import asyncio
from typing import Any, Dict, Optional, Set

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    SubmissionCheckResultView,
    TableView,
    WorldQuantClient,
)
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CONSULTANT_MAX_SELF_CORRELATION,
    CheckRecordType,
    CorrelationType,
    RefreshPolicy,
    SubmissionCheckResult,
    SubmissionCheckType,
)
from alphapower.dal.evaluate import (
    CheckRecordDAL,
    CorrelationDAL,
)
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import Alpha, CheckRecord, EvaluateRecord
from alphapower.internal.logging import get_logger

from .correlation_calculator import CorrelationCalculator

log = get_logger(module_name=__name__)


class InSampleChecksEvaluateStage(AbstractEvaluateStage):
    """
    评估管道中的一个阶段，用于对 Alpha 对象执行样本内检查。

    属性:
        check_pass_result_map (Dict[SampleCheckType, Set[SampleCheckResult]]):
            检查类型与可接受检查结果集合的映射，子类可以重写此属性以定义不同的检查结果。

    方法:
        _evaluate_stage(self,
            alpha: Alpha,
            policy: RefreshPolicy,
            record: EvaluateRecord,
            **kwargs: Any,
        ) -> bool:
            异步评估给定 Alpha 对象的样本内检查。根据检查结果记录警告或信息日志。
            如果所有检查通过，返回 True；否则返回 False。
    """

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        check_pass_result_map: Dict[SubmissionCheckType, Set[SubmissionCheckResult]],
    ) -> None:
        super().__init__(next_stage)
        self._check_pass_result_map = check_pass_result_map

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        if alpha.in_sample is None:
            await log.awarning(
                "Alpha 对象缺少 in_sample 属性，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        record.in_sample_pnl = alpha.in_sample.pnl if alpha.in_sample.pnl else 0.0
        record.in_sample_long_count = (
            alpha.in_sample.long_count if alpha.in_sample.long_count else 0
        )
        record.in_sample_short_count = (
            alpha.in_sample.short_count if alpha.in_sample.short_count else 0
        )
        record.in_sample_book_size = (
            alpha.in_sample.book_size if alpha.in_sample.book_size else 0.0
        )
        record.in_sample_turnover = (
            alpha.in_sample.turnover if alpha.in_sample.turnover else 0.0
        )
        record.in_sample_returns = (
            alpha.in_sample.returns if alpha.in_sample.returns else 0.0
        )
        record.in_sample_drawdown = (
            alpha.in_sample.drawdown if alpha.in_sample.drawdown else 0.0
        )
        record.in_sample_sharpe = (
            alpha.in_sample.sharpe if alpha.in_sample.sharpe else 0.0
        )
        record.in_sample_fitness = (
            alpha.in_sample.fitness if alpha.in_sample.fitness else 0.0
        )

        if alpha.in_sample.checks is None:
            await log.awarning(
                "Alpha 对象的 in_sample 检查列表为空，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        for check in alpha.in_sample.checks:
            pass_result_set: Set[SubmissionCheckResult] = self._check_pass_result_map.get(
                SubmissionCheckType(check.name), set()
            )

            if (
                check.name == SubmissionCheckType.MATCHES_PYRAMID.value
                and check.result == SubmissionCheckResult.PASS
            ):
                record.pyramid_multiplier = (
                    check.multiplier if check.multiplier else 1.0
                )

            if (
                check.name == SubmissionCheckType.MATCHES_THEMES.value
                and check.result == SubmissionCheckResult.PASS
            ):
                record.theme_multiplier = check.multiplier if check.multiplier else 1.0

            if check.result == SubmissionCheckResult.FAIL:
                await log.awarning(
                    "Alpha 对象的 in_sample 检查未通过",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                return False

            if check.result == SubmissionCheckResult.PASS:
                await log.ainfo(
                    "Alpha 对象的 in_sample 检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                continue

            if len(pass_result_set) == 0:
                await log.awarning(
                    "Alpha 对象的 in_sample 检查通过结果集为空，跳过检查",
                    emoji="⚠️",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                )
                continue

            if check.result in pass_result_set:
                await log.ainfo(
                    "Alpha 对象的 in_sample 检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                    pass_result_set=pass_result_set,
                )
            else:
                await log.awarning(
                    "Alpha 对象的 in_sample 检查未通过",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                    pass_result_set=pass_result_set,
                )
                return False
        await log.ainfo(
            "Alpha 对象的 in_sample 检查全部通过",
            emoji="✅",
            alpha_id=alpha.alpha_id,
        )
        return True


class CorrelationLocalEvaluateStage(AbstractEvaluateStage):
    """
    本地相关性评估阶段，用于检查 Alpha 的自相关性。
    """

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        correlation_calculator: CorrelationCalculator,
    ) -> None:
        """
        初始化本地相关性评估阶段。

        Args:
            next_stage: 下一个评估阶段 (责任链中的下一个节点)。
            correlation_calculator: 相关性计算器实例。
        """
        super().__init__(next_stage)
        self.correlation_calculator: CorrelationCalculator = correlation_calculator

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        执行本地相关性检查。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略 (未使用)。
            record: 当前评估的记录对象 (未使用)。
            checks_ctx: 检查上下文，用于存储和共享检查结果。
            kwargs: 其他参数。

        Returns:
            bool: 如果检查通过返回 True，否则返回 False。
        """
        try:
            pairwise_correlation: Dict[str, float] = (
                await self.correlation_calculator.calculate_correlation(
                    alpha=alpha,
                )
            )

            max_corr: float = max(pairwise_correlation.values(), default=0.0)
            min_corr: float = min(pairwise_correlation.values(), default=0.0)

            await log.ainfo(
                "自相关性检查",
                emoji="🔍",
                alpha_id=alpha.alpha_id,
                record_self_correlation=record.self_correlation,
                max_corr=max_corr,
                min_corr=min_corr,
            )

            record.self_correlation = max(
                record.self_correlation if record.self_correlation else -1.0, max_corr
            )
            if max_corr > CONSULTANT_MAX_SELF_CORRELATION:
                await log.awarning(
                    "自相关性检查未通过，最大相关性超过阈值",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    max_corr=max_corr,
                    min_corr=min_corr,
                )
                return False

            await log.ainfo(
                "自相关性检查通过",
                emoji="✅",
                alpha_id=alpha.alpha_id,
                max_corr=max_corr,
            )
            return True
        except asyncio.TimeoutError as e:
            # 分类处理网络超时异常
            await log.awarning(
                "计算自相关性时发生超时异常，可能需要重试",
                emoji="⏳",
                alpha_id=alpha.alpha_id,
                error=str(e),
            )
            return False
        except ValueError as e:
            # 分类处理数据解析异常
            await log.aerror(
                "计算自相关性时发生数据解析异常",
                emoji="📉",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False
        except Exception as e:
            # 捕获其他异常并记录为 CRITICAL
            await log.acritical(
                "💥 计算自相关性时发生未知异常，程序可能无法继续",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False


class CorrelationPlatformEvaluateStage(AbstractEvaluateStage):
    """
    平台相关性评估阶段，用于检查 Alpha 的平台相关性 (self 或 prod)。
    """

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        correlation_type: CorrelationType,
        check_record_dal: CheckRecordDAL,
        correlation_dal: CorrelationDAL,
        client: WorldQuantClient,
    ) -> None:
        """
        初始化平台相关性评估阶段。

        Args:
            next_stage: 下一个评估阶段 (责任链中的下一个节点)。
            correlation_type: 相关性类型 (self 或 prod)。
            check_record_dal: 检查记录数据访问层实例。
            correlation_dal: 相关性数据访问层实例。
            client: 平台客户端实例。
        """
        super().__init__(next_stage)
        self.correlation_type: CorrelationType = correlation_type
        self.check_record_dal: CheckRecordDAL = check_record_dal
        self.correlation_dal: CorrelationDAL = correlation_dal
        self.client: WorldQuantClient = client

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        执行平台相关性检查。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            record: 当前评估的记录对象。
            kwargs: 其他参数。

        Returns:
            bool: 如果检查通过返回 True，否则返回 False。
        """
        record_type: CheckRecordType = (
            CheckRecordType.CORRELATION_SELF
            if self.correlation_type == CorrelationType.SELF
            else CheckRecordType.CORRELATION_PROD
        )
        check_type_name: str = (
            "自相关性"
            if self.correlation_type == CorrelationType.SELF
            else "生产相关性"
        )

        try:
            exist_check_record: Optional[CheckRecord] = (
                await self.check_record_dal.find_one_by(
                    alpha_id=alpha.alpha_id,
                    record_type=record_type,
                    order_by=CheckRecord.created_at.desc(),
                )
            )
            await log.adebug(
                f"查询现有{check_type_name}检查记录结果",
                emoji="💾" if exist_check_record else "❓",
                alpha_id=alpha.alpha_id,
                record_type=record_type,
                record_found=bool(exist_check_record),
            )

            action: AbstractEvaluateStage.CheckAction = (
                await self._determine_check_action(
                    policy=policy,
                    exist_check_record=exist_check_record,
                    alpha_id=alpha.alpha_id,
                    check_type_name=check_type_name,
                )
            )

            correlation_content: Optional[TableView] = None
            if action == AbstractEvaluateStage.CheckAction.REFRESH:
                correlation_content = await self._refresh_correlation_data(alpha)
                if not correlation_content:
                    await log.awarning(
                        f"{check_type_name}数据刷新失败，检查不通过",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        correlation_type=self.correlation_type,
                    )
                    return False
            elif (
                action == AbstractEvaluateStage.CheckAction.USE_EXISTING
                and exist_check_record
            ):
                correlation_content = TableView.model_validate(
                    exist_check_record.content
                )
            elif action in {
                AbstractEvaluateStage.CheckAction.SKIP,
                AbstractEvaluateStage.CheckAction.FAIL_MISSING,
            }:
                return False
            elif action == AbstractEvaluateStage.CheckAction.ERROR:
                await log.aerror(
                    f"处理 {check_type_name} 检查遇到错误状态",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    policy=policy,
                )
                return False

            if correlation_content:
                max_corr: float = correlation_content.max or 0.0
                if self.correlation_type == CorrelationType.SELF:
                    record.self_correlation = max_corr
                    if max_corr > CONSULTANT_MAX_SELF_CORRELATION:
                        await log.awarning(
                            f"{check_type_name}检查未通过，最大相关性超过阈值",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            max_corr=max_corr,
                        )
                        return False
                elif self.correlation_type == CorrelationType.PROD:
                    record.prod_correlation = max_corr
                    if max_corr > CONSULTANT_MAX_PROD_CORRELATION:
                        await log.awarning(
                            f"{check_type_name}检查未通过，最大相关性超过阈值",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            max_corr=max_corr,
                        )
                        return False

                await log.ainfo(
                    f"{check_type_name}检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    max_corr=max_corr,
                )
                return True
            else:
                await log.aerror(
                    f"未能获取或加载{check_type_name}数据，无法执行检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    correlation_type=self.correlation_type,
                )
                return False
        except Exception as e:
            await log.aerror(
                f"检查 {check_type_name} 时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
                exc_info=True,
            )
            return False

    async def _refresh_correlation_data(self, alpha: Alpha) -> Optional[TableView]:
        """
        刷新相关性数据。

        Args:
            alpha: 待评估的 Alpha 对象。

        Returns:
            Optional[TableView]: 刷新后的相关性数据。
        """
        try:
            retry_count: int = 0  # 重试计数器
            max_retries: int = 3  # 最大重试次数
            await log.adebug(
                "开始刷新相关性数据",
                emoji="🔄",
                alpha_id=alpha.alpha_id,
                max_retries=max_retries,
            )
            while retry_count < max_retries:
                finished: bool
                retry_after: Optional[float]
                api_result: Optional[TableView]
                finished, retry_after, api_result = (
                    await self.client.alpha_correlation_check(
                        alpha_id=alpha.alpha_id,
                        corr_type=self.correlation_type,
                    )
                )
                await log.adebug(
                    "相关性检查 API 调用结果",
                    emoji="📡",
                    alpha_id=alpha.alpha_id,
                    finished=finished,
                    retry_after=retry_after,
                    api_result=bool(api_result),
                )
                if finished:
                    if api_result:
                        await log.ainfo(
                            "相关性数据 API 获取成功",
                            emoji="🎉",
                            alpha_id=alpha.alpha_id,
                            corr_type=self.correlation_type,
                        )
                        check_record: CheckRecord = CheckRecord(
                            alpha_id=alpha.alpha_id,
                            record_type=(
                                CheckRecordType.CORRELATION_SELF
                                if self.correlation_type == CorrelationType.SELF
                                else CheckRecordType.CORRELATION_PROD
                            ),
                            content=api_result.model_dump(mode="python"),
                        )
                        await self.check_record_dal.create(check_record)
                        # FIXME: 这里因为没有 commit 导致没有及时持久化数据
                        await self.check_record_dal.session.commit()
                        await log.adebug(
                            "相关性数据已保存到数据库",
                            emoji="💾",
                            alpha_id=alpha.alpha_id,
                            record_type=check_record.record_type,
                        )
                        return api_result
                    else:
                        await log.awarning(
                            "相关性检查 API 声称完成，但未返回有效结果",
                            emoji="❓",
                            alpha_id=alpha.alpha_id,
                            corr_type=self.correlation_type,
                        )
                        return None
                elif retry_after and retry_after > 0:
                    await log.adebug(
                        "API 请求未完成，等待重试",
                        emoji="⏳",
                        alpha_id=alpha.alpha_id,
                        retry_after=retry_after,
                    )
                    await asyncio.sleep(retry_after)
                else:
                    retry_count += 1
                    await log.awarning(
                        "相关性检查 API 返回异常状态：未完成且无重试时间",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        corr_type=self.correlation_type,
                        retry_count=retry_count,
                    )
            await log.acritical(
                "相关性检查 API 多次重试失败，程序可能无法继续",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                corr_type=self.correlation_type,
                max_retries=max_retries,
            )
            return None
        except Exception as e:
            await log.aerror(
                "刷新相关性数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
                exc_info=True,
            )
            return None


class PerformanceDiffEvaluateStage(AbstractEvaluateStage):
    """
    业绩对比评估阶段。
    """

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        competition_id: Optional[str],
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ) -> None:
        """
        初始化业绩对比评估阶段。

        Args:
            competition_id: 如果提供，则执行竞赛专用业绩对比，否则执行普通业绩对比。
        """
        super().__init__(next_stage)
        self.competition_id = competition_id
        self.check_record_dal = check_record_dal
        self.client = client

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        执行业绩对比评估逻辑。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            record: 当前评估的记录对象。
            kwargs: 其他参数。

        Returns:
            bool: 评估是否通过。
        """
        check_type_name = "因子池绩效差异"
        await log.adebug(
            f"开始评估 {check_type_name}",
            emoji="🔍",
            alpha_id=alpha.alpha_id,
            competition_id=self.competition_id,
            policy=policy,
            kwargs=kwargs,
        )

        try:
            # 刷新或获取业绩对比数据
            perf_diff_view = await self._refresh_or_get_performance_diff(
                alpha, policy, check_type_name, **kwargs
            )
            if not perf_diff_view:
                await log.awarning(
                    f"{check_type_name}数据获取失败，评估不通过",
                    emoji="⚠️",
                    alpha_id=alpha.alpha_id,
                    competition_id=self.competition_id,
                )
                return False

            # 判断业绩是否符合要求
            result = await self._determine_performance_diff_pass_status(
                alpha=alpha,
                perf_diff_view=perf_diff_view,
                record=record,
                **kwargs,
            )
            await log.ainfo(
                f"{check_type_name}评估完成",
                emoji="✅" if result else "❌",
                alpha_id=alpha.alpha_id,
                competition_id=self.competition_id,
                passed=result,
            )
            return result

        except Exception as e:
            await log.aerror(
                f"{check_type_name}评估时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                competition_id=self.competition_id,
                error=str(e),
                exc_info=True,
            )
            return False

    async def _refresh_or_get_performance_diff(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        check_type_name: str,
        **kwargs: Any,
    ) -> Optional[BeforeAndAfterPerformanceView]:
        """
        刷新或获取业绩对比数据。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            check_type_name: 检查类型名称，用于日志。
            kwargs: 其他参数。

        Returns:
            Optional[BeforeAndAfterPerformanceView]: 业绩对比数据。
        """
        # 根据策略决定是否刷新数据
        action = await self._determine_check_action(
            policy=policy,
            exist_check_record=await self.check_record_dal.find_one_by(
                alpha_id=alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=CheckRecord.created_at.desc(),
            ),
            alpha_id=alpha.alpha_id,
            check_type_name=check_type_name,
        )

        if action == AbstractEvaluateStage.CheckAction.REFRESH:
            return await self._refresh_alpha_pool_performance_diff(alpha)
        elif action == AbstractEvaluateStage.CheckAction.USE_EXISTING:
            record = await self.check_record_dal.find_one_by(
                alpha_id=alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                order_by=CheckRecord.created_at.desc(),
            )
            if record:
                return BeforeAndAfterPerformanceView(**record.content)
        elif action in {
            AbstractEvaluateStage.CheckAction.SKIP,
            AbstractEvaluateStage.CheckAction.FAIL_MISSING,
        }:
            return None

        raise ValueError(f"无效的检查动作: {action}")

    async def _refresh_alpha_pool_performance_diff(
        self, alpha: Alpha
    ) -> Optional[BeforeAndAfterPerformanceView]:
        """
        刷新因子池绩效差异数据。

        Args:
            alpha: 待评估的 Alpha 对象。

        Returns:
            Optional[BeforeAndAfterPerformanceView]: 刷新后的绩效差异数据。
        """
        await log.adebug(
            "刷新因子池绩效差异数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
            competition_id=self.competition_id,
        )
        try:
            async with self.client:
                finished: bool = False
                while not finished:
                    finished, retry_after, result = (
                        await self.client.alpha_fetch_before_and_after_performance(
                            alpha_id=alpha.alpha_id,
                            competition_id=self.competition_id,
                        )
                    )
                    if finished and result:
                        await self.check_record_dal.create(
                            CheckRecord(
                                alpha_id=alpha.alpha_id,
                                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                                content=result.model_dump(mode="json"),
                            )
                        )
                        # FIXME: 这里因为没有 commit 导致没有及时持久化数据
                        await self.check_record_dal.session.commit()
                        return result
                    elif retry_after and retry_after > 0:
                        await log.adebug(
                            "等待重试",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            competition_id=self.competition_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        await log.awarning(
                            "刷新因子池绩效差异数据时发生异常",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            competition_id=self.competition_id,
                            retry_after=retry_after,
                        )
                        return None
        except Exception as e:
            await log.aerror(
                "刷新因子池绩效差异数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                competition_id=self.competition_id,
                error=str(e),
                exc_info=True,
            )
        return None

    async def _determine_performance_diff_pass_status(
        self,
        alpha: Alpha,
        perf_diff_view: BeforeAndAfterPerformanceView,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        判断业绩是否符合要求。

        Args:
            alpha: 待评估的 Alpha 对象。
            perf_diff_view: 业绩对比数据。
            kwargs: 其他参数。

        Returns:
            bool: 是否符合要求。
        """
        raise NotImplementedError("子类必须实现业绩对比条件的判断逻辑")


class SubmissionEvaluateStage(AbstractEvaluateStage):
    """
    提交检查评估阶段。
    """

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ) -> None:
        """
        初始化提交检查评估阶段。

        Args:
            next_stage: 下一个评估阶段 (责任链中的下一个节点)。
            check_record_dal: 检查记录数据访问层实例。
            client: 平台客户端实例。
        """
        super().__init__(next_stage)
        self.check_record_dal = check_record_dal
        self.client = client

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        """
        执行提交检查评估逻辑。

        Args:
            alpha: 待评估的 Alpha 对象。
            policy: 刷新策略。
            record: 当前评估的记录对象。
            kwargs: 其他参数。

        Returns:
            bool: 评估是否通过。
        """
        check_type_name = "提交检查"
        record_type = CheckRecordType.SUBMISSION

        await log.adebug(
            f"开始评估 {check_type_name}",
            emoji="🔍",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )

        try:
            # 查找现有的检查记录
            exist_check_record: Optional[CheckRecord] = (
                await self.check_record_dal.find_one_by(
                    alpha_id=alpha.alpha_id,
                    record_type=record_type,
                    order_by=CheckRecord.created_at.desc(),
                )
            )
            await log.adebug(
                f"查询现有{check_type_name}检查记录结果",
                emoji="💾" if exist_check_record else "❓",
                alpha_id=alpha.alpha_id,
                record_type=record_type,
                record_found=bool(exist_check_record),
            )

            # 根据策略决定执行的操作
            action = await self._determine_check_action(
                policy=policy,
                exist_check_record=exist_check_record,
                alpha_id=alpha.alpha_id,
                check_type_name=check_type_name,
            )

            submission_check_view: Optional[SubmissionCheckResultView] = None

            if action == AbstractEvaluateStage.CheckAction.REFRESH:
                submission_check_view = await self._refresh_submission_check_data(alpha)
                if not submission_check_view:
                    await log.awarning(
                        f"{check_type_name}数据刷新失败，检查不通过",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                    )
                    return False

            elif action == AbstractEvaluateStage.CheckAction.USE_EXISTING:
                if exist_check_record:
                    try:
                        submission_check_view = SubmissionCheckResultView(
                            **exist_check_record.content
                        )
                    except (TypeError, ValueError, KeyError) as parse_err:
                        await log.aerror(
                            f"解析现有{check_type_name}记录时出错",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            record_id=exist_check_record.id,
                            error=str(parse_err),
                            exc_info=True,
                        )
                        return False

            elif action in {
                AbstractEvaluateStage.CheckAction.SKIP,
                AbstractEvaluateStage.CheckAction.FAIL_MISSING,
            }:
                return False

            elif action == AbstractEvaluateStage.CheckAction.ERROR:
                await log.aerror(
                    f"处理 {check_type_name} 检查遇到错误状态",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    policy=policy,
                )
                return False

            # 判断检查是否通过
            if submission_check_view:
                result = await self._determine_submission_pass_status(
                    submission_check_view=submission_check_view,
                    **kwargs,
                )
                await log.ainfo(
                    f"{check_type_name}评估完成",
                    emoji="✅" if result else "❌",
                    alpha_id=alpha.alpha_id,
                    passed=result,
                )
                return result

            await log.aerror(
                f"未能获取或加载{check_type_name}数据，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        except Exception as e:
            await log.aerror(
                f"{check_type_name}评估时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False

    async def _refresh_submission_check_data(
        self,
        alpha: Alpha,
    ) -> Optional[SubmissionCheckResultView]:
        """
        刷新提交检查数据。

        Args:
            alpha: 待评估的 Alpha 对象。

        Returns:
            Optional[SubmissionCheckResultView]: 刷新后的提交检查数据。
        """
        await log.adebug(
            "开始刷新提交检查数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
        )
        try:
            async with self.client:
                finished: bool = False
                retry_after: Optional[float] = None
                result: Optional[SubmissionCheckResultView] = None
                while not finished:
                    finished, retry_after, result, _ = (
                        await self.client.alpha_fetch_submission_check_result(
                            alpha_id=alpha.alpha_id,
                        )
                    )
                    if finished and result:
                        await self.check_record_dal.create(
                            CheckRecord(
                                alpha_id=alpha.alpha_id,
                                record_type=CheckRecordType.SUBMISSION,
                                content=result.model_dump(),
                            )
                        )
                        # FIXME: 这里因为没有 commit 导致没有及时持久化数据
                        await self.check_record_dal.session.commit()
                        return result
                    elif retry_after and retry_after > 0:
                        await log.adebug(
                            "等待重试",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        await log.awarning(
                            "刷新提交检查数据时发生异常",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            retry_after=retry_after,
                        )
                        return None
        except Exception as e:
            await log.aerror(
                "刷新提交检查数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
        return None

    async def _determine_submission_pass_status(
        self,
        submission_check_view: SubmissionCheckResultView,
        **kwargs: Any,
    ) -> bool:
        """
        判断提交检查是否符合要求。

        Args:
            submission_check_view: 提交检查数据。

        Returns:
            bool: 是否符合要求。
        """
        if submission_check_view.in_sample is None:
            return False
        if submission_check_view.in_sample.checks is None:
            return False
        if len(submission_check_view.in_sample.checks) == 0:
            return False

        for check in submission_check_view.in_sample.checks:
            if check.result != SubmissionCheckResult.PASS:
                return False

        return True
