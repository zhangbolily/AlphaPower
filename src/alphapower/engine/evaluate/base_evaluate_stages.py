import asyncio
from datetime import date
from typing import Any, Dict, Optional, Set

from structlog.stdlib import BoundLogger

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    SubmissionCheckResultView,
    TableView,
    WorldQuantClient,
)
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CONSULTANT_MAX_SELF_CORRELATION,
    MIN_FORMULATED_PYRAMID_ALPHAS,
    CheckRecordType,
    CorrelationType,
    Database,
    Delay,
    RefreshPolicy,
    Region,
    SubmissionCheckResult,
    SubmissionCheckType,
)
from alphapower.dal.evaluate import (
    CheckRecordDAL,
    CorrelationDAL,
)
from alphapower.dal.session_manager import session_manager
from alphapower.engine.evaluate.evaluate_stage_abc import AbstractEvaluateStage
from alphapower.entity import Alpha, CheckRecord, EvaluateRecord
from alphapower.internal.logging import get_logger
from alphapower.view.activities import PyramidAlphasQuery, PyramidAlphasView

from .correlation_calculator import CorrelationCalculator


class InSampleChecksEvaluateStage(AbstractEvaluateStage):

    def __init__(
        self,
        client: WorldQuantClient,
        next_stage: Optional[AbstractEvaluateStage] = None,
        check_pass_result_map: Optional[
            Dict[SubmissionCheckType, Set[SubmissionCheckResult]]
        ] = None,
    ) -> None:
        super().__init__(next_stage)
        self.client: WorldQuantClient = client
        self.check_pass_result_map: Dict[
            SubmissionCheckType, Set[SubmissionCheckResult]
        ] = (check_pass_result_map if check_pass_result_map else {})
        self.initialized: bool = False
        self.region_category_delay_map: Dict[str, int] = {}
        self.log: BoundLogger = get_logger(
            f"{__name__}.{self.__class__.__name__}",
        )

    async def _get_pyramid_alpha_key(
        self,
        region: Region,
        delay: Delay,
        category_id: str,
    ) -> str:
        return f"{region.value}_D{delay.value}_{category_id}".upper()

    async def initialize(self) -> None:
        """初始化 InSampleChecksEvaluateStage，加载金字塔因子数据"""
        await self.log.adebug(
            "开始初始化 InSampleChecksEvaluateStage",
            emoji="🔄",
            initialized=self.initialized,
        )
        if not self.initialized:
            try:
                async with self.client as client:
                    # 获取当前季度的起止时间
                    today = date.today()
                    quarter_start_month = ((today.month - 1) // 3) * 3 + 1
                    quarter_end_month = quarter_start_month + 3
                    start_date = date(today.year, quarter_start_month, 1)
                    end_date = date(today.year, quarter_end_month, 1)

                    # 构造 PyramidAlphasQuery 查询对象
                    query: PyramidAlphasQuery = PyramidAlphasQuery(
                        start_date=start_date,
                        end_date=end_date,
                    )
                    pyramid_alphas: PyramidAlphasView = (
                        await client.user_fetch_pyramid_alphas(query=query)
                    )
                if pyramid_alphas and pyramid_alphas.pyramids:
                    for pyramid_alpha in pyramid_alphas.pyramids:
                        if not pyramid_alpha.category or not pyramid_alpha.category.id:
                            await self.log.aerror(
                                "金字塔因子缺少分类信息，无法处理",
                                emoji="❌",
                                pyramid_alpha=pyramid_alpha,
                            )
                            raise ValueError(
                                "金字塔因子缺少分类信息，无法处理",
                            )

                        key: str = await self._get_pyramid_alpha_key(
                            region=pyramid_alpha.region,
                            delay=pyramid_alpha.delay,
                            category_id=pyramid_alpha.category.id,
                        )
                        self.region_category_delay_map[key] = pyramid_alpha.alpha_count

                    self.initialized = True

                    await self.log.ainfo(
                        "InSampleChecksEvaluateStage 初始化成功",
                        emoji="✅",
                        initialized=self.initialized,
                        region_category_delay_map=self.region_category_delay_map,
                    )
                else:
                    await self.log.awarning(
                        "未能初始化 InSampleChecksEvaluateStage，缺少金字塔因子数据",
                        emoji="❌",
                    )
                    raise ValueError(
                        "InSampleChecksEvaluateStage 初始化失败，缺少金字塔因子数据",
                    )
            except asyncio.TimeoutError as e:
                await self.log.awarning(
                    "初始化 InSampleChecksEvaluateStage 时发生超时异常",
                    emoji="⏳",
                    error=str(e),
                )
                raise
            except Exception as e:
                await self.log.aerror(
                    "初始化 InSampleChecksEvaluateStage 时发生未知异常",
                    emoji="💥",
                    error=str(e),
                    exc_info=True,
                )
                raise
        else:
            await self.log.adebug(
                "InSampleChecksEvaluateStage 已初始化，无需重复操作",
                emoji="✅",
                initialized=self.initialized,
            )

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:
        if not self.initialized:
            await self.log.acritical(
                "InSampleChecksEvaluateStage 尚未初始化，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            raise RuntimeError(
                "InSampleChecksEvaluateStage 尚未初始化，无法执行检查",
            )

        if alpha.in_sample is None:
            await self.log.awarning(
                "Alpha 对象缺少 in_sample 属性，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        record.is_pnl = alpha.in_sample.pnl if alpha.in_sample.pnl else 0.0
        record.is_long_count = (
            alpha.in_sample.long_count if alpha.in_sample.long_count else 0
        )
        record.is_short_count = (
            alpha.in_sample.short_count if alpha.in_sample.short_count else 0
        )
        record.is_book_size = (
            alpha.in_sample.book_size if alpha.in_sample.book_size else 0.0
        )
        record.is_turnover = (
            alpha.in_sample.turnover if alpha.in_sample.turnover else 0.0
        )
        record.is_returns = (
            alpha.in_sample.returns if alpha.in_sample.returns else 0.0
        )
        record.is_drawdown = (
            alpha.in_sample.drawdown if alpha.in_sample.drawdown else 0.0
        )
        record.is_sharpe = (
            alpha.in_sample.sharpe if alpha.in_sample.sharpe else 0.0
        )
        record.is_fitness = (
            alpha.in_sample.fitness if alpha.in_sample.fitness else 0.0
        )
        record.is_margin = (
            alpha.in_sample.margin if alpha.in_sample.margin else 0.0
        )

        if alpha.in_sample.checks is None:
            await self.log.awarning(
                "Alpha 对象的 in_sample 检查列表为空，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        for check in alpha.in_sample.checks:
            pass_result_set: Set[SubmissionCheckResult] = (
                self.check_pass_result_map.get(SubmissionCheckType(check.name), set())
            )

            if (
                check.name == SubmissionCheckType.MATCHES_PYRAMID.value
                and check.result == SubmissionCheckResult.PASS
            ):
                record.pyramid_multiplier = (
                    check.multiplier if check.multiplier else 1.0
                )

                for pyramid in check.pyramids:
                    key: str = pyramid.name.replace("/", "_").upper()
                    alpha_count: int = self.region_category_delay_map.get(key, 0)
                    if alpha_count < MIN_FORMULATED_PYRAMID_ALPHAS:
                        record.matched_unformulated_pyramid = (
                            record.matched_unformulated_pyramid + 1
                            if record.matched_unformulated_pyramid
                            else 1
                        )
                        await self.log.ainfo(
                            "匹配的金字塔未点亮",
                            pyramid=pyramid,
                            key=key,
                            pyramid_alpha_count=alpha_count,
                            min_alpha_count=MIN_FORMULATED_PYRAMID_ALPHAS,
                            emoji="🔆",
                        )

                effective_pyramids: int = check.effective if check.effective else 0
                if (
                    record.matched_unformulated_pyramid
                    and record.matched_unformulated_pyramid > effective_pyramids
                ):
                    await self.log.awarning(
                        "匹配的未完成金字塔数量超过有效金字塔数量",
                        emoji="❌",
                        alpha_id=alpha.alpha_id,
                        matched_unformulated_pyramid=record.matched_unformulated_pyramid,
                        effective_pyramids=effective_pyramids,
                    )
                    record.matched_unformulated_pyramid = effective_pyramids

            if (
                check.name == SubmissionCheckType.MATCHES_THEMES.value
                and check.result == SubmissionCheckResult.PASS
            ):
                record.theme_multiplier = check.multiplier if check.multiplier else 1.0

            if check.result == SubmissionCheckResult.FAIL:
                await self.log.awarning(
                    "Alpha 对象的 in_sample 检查未通过",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                return False

            if check.result == SubmissionCheckResult.PASS:
                await self.log.ainfo(
                    "Alpha 对象的 in_sample 检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                continue

            if len(pass_result_set) == 0:
                await self.log.awarning(
                    "Alpha 对象的 in_sample 检查通过结果集为空，跳过检查",
                    emoji="⚠️",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                )
                continue

            if check.result in pass_result_set:
                await self.log.ainfo(
                    "Alpha 对象的 in_sample 检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                    pass_result_set=pass_result_set,
                )
            else:
                await self.log.awarning(
                    "Alpha 对象的 in_sample 检查未通过",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                    pass_result_set=pass_result_set,
                )
                return False
        await self.log.ainfo(
            "Alpha 对象的 in_sample 检查全部通过",
            emoji="✅",
            alpha_id=alpha.alpha_id,
        )
        return True


class CorrelationLocalEvaluateStage(AbstractEvaluateStage):

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        correlation_calculator: CorrelationCalculator,
        threshold: float = CONSULTANT_MAX_SELF_CORRELATION,
    ) -> None:

        super().__init__(next_stage)
        self.correlation_calculator: CorrelationCalculator = correlation_calculator
        self._threshold: float = threshold
        self.log: BoundLogger = get_logger(
            f"{__name__}.{self.__class__.__name__}",
        )

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:

        try:
            pairwise_correlation: Dict[Alpha, float] = (
                await self.correlation_calculator.calculate_correlation(
                    alpha=alpha,
                )
            )

            max_corr: float = max(pairwise_correlation.values(), default=0.0)
            min_corr: float = min(pairwise_correlation.values(), default=0.0)

            await self.log.ainfo(
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
            if max_corr > self._threshold:
                await self.log.awarning(
                    "自相关性检查未通过，最大相关性超过阈值",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    max_corr=max_corr,
                    min_corr=min_corr,
                    threshold=self._threshold,
                )
                return False

            await self.log.ainfo(
                "自相关性检查通过",
                emoji="✅",
                alpha_id=alpha.alpha_id,
                max_corr=max_corr,
            )
            return True
        except asyncio.TimeoutError as e:
            # 分类处理网络超时异常
            await self.log.awarning(
                "计算自相关性时发生超时异常，可能需要重试",
                emoji="⏳",
                alpha_id=alpha.alpha_id,
                error=str(e),
            )
            return False
        except ValueError as e:
            # 分类处理数据解析异常
            await self.log.aerror(
                "计算自相关性时发生数据解析异常",
                emoji="📉",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False
        except Exception as e:
            # 捕获其他异常并记录为 CRITICAL
            await self.log.acritical(
                "💥 计算自相关性时发生未知异常，程序可能无法继续",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False


class CorrelationPlatformEvaluateStage(AbstractEvaluateStage):

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        correlation_type: CorrelationType,
        check_record_dal: CheckRecordDAL,
        correlation_dal: CorrelationDAL,
        client: WorldQuantClient,
    ) -> None:

        super().__init__(next_stage)
        self.correlation_type: CorrelationType = correlation_type
        self.check_record_dal: CheckRecordDAL = check_record_dal
        self.correlation_dal: CorrelationDAL = correlation_dal
        self.client: WorldQuantClient = client
        self.log: BoundLogger = get_logger(
            f"{__name__}.{self.__class__.__name__}",
        )

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:

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

            async with session_manager.get_session(Database.EVALUATE) as session:
                self.check_record_dal.session = session
                exist_check_record: Optional[CheckRecord] = (
                    await self.check_record_dal.find_one_by(
                        alpha_id=alpha.alpha_id,
                        record_type=record_type,
                        order_by=CheckRecord.created_at.desc(),
                    )
                )
            await self.log.adebug(
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
                    await self.log.awarning(
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
                await self.log.aerror(
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
                        await self.log.awarning(
                            f"{check_type_name}检查未通过，最大相关性超过阈值",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            max_corr=max_corr,
                        )
                        return False
                elif self.correlation_type == CorrelationType.PROD:
                    record.prod_correlation = max_corr
                    if max_corr > CONSULTANT_MAX_PROD_CORRELATION:
                        await self.log.awarning(
                            f"{check_type_name}检查未通过，最大相关性超过阈值",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            max_corr=max_corr,
                        )
                        return False

                await self.log.ainfo(
                    f"{check_type_name}检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    max_corr=max_corr,
                )
                return True
            else:
                await self.log.aerror(
                    f"未能获取或加载{check_type_name}数据，无法执行检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    correlation_type=self.correlation_type,
                )
                return False
        except Exception as e:
            await self.log.aerror(
                f"检查 {check_type_name} 时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
                exc_info=True,
            )
            return False

    async def _refresh_correlation_data(self, alpha: Alpha) -> Optional[TableView]:

        try:
            retry_count: int = 0  # 重试计数器
            max_retries: int = 3  # 最大重试次数
            await self.log.adebug(
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
                await self.log.adebug(
                    "相关性检查 API 调用结果",
                    emoji="📡",
                    alpha_id=alpha.alpha_id,
                    finished=finished,
                    retry_after=retry_after,
                    api_result=bool(api_result),
                )
                if finished:
                    if api_result:
                        await self.log.ainfo(
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

                        async with (
                            session_manager.get_session(Database.EVALUATE) as session,
                            session.begin(),
                        ):
                            self.check_record_dal.session = session
                            await self.check_record_dal.create(check_record)
                            await self.log.adebug(
                                "相关性数据已保存到数据库",
                                emoji="💾",
                                alpha_id=alpha.alpha_id,
                                record_type=check_record.record_type,
                            )
                        return api_result
                    else:
                        await self.log.awarning(
                            "相关性检查 API 声称完成，但未返回有效结果",
                            emoji="❓",
                            alpha_id=alpha.alpha_id,
                            corr_type=self.correlation_type,
                        )
                        return None
                elif retry_after and retry_after > 0:
                    await self.log.adebug(
                        "API 请求未完成，等待重试",
                        emoji="⏳",
                        alpha_id=alpha.alpha_id,
                        retry_after=retry_after,
                    )
                    await asyncio.sleep(retry_after)
                else:
                    retry_count += 1
                    await self.log.awarning(
                        "相关性检查 API 返回异常状态：未完成且无重试时间",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        corr_type=self.correlation_type,
                        retry_count=retry_count,
                    )
            await self.log.acritical(
                "相关性检查 API 多次重试失败，程序可能无法继续",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                corr_type=self.correlation_type,
                max_retries=max_retries,
            )
            return None
        except Exception as e:
            await self.log.aerror(
                "刷新相关性数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
                exc_info=True,
            )
            return None


class PerformanceDiffEvaluateStage(AbstractEvaluateStage):

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        competition_id: Optional[str],
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ) -> None:

        super().__init__(next_stage)
        self.competition_id = competition_id
        self.check_record_dal = check_record_dal
        self.client = client
        self.log: BoundLogger = get_logger(
            f"{__name__}.{self.__class__.__name__}",
        )

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:

        check_type_name = "因子池绩效差异"
        await self.log.adebug(
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
                await self.log.awarning(
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
            await self.log.ainfo(
                f"{check_type_name}评估完成",
                emoji="✅" if result else "❌",
                alpha_id=alpha.alpha_id,
                competition_id=self.competition_id,
                passed=result,
            )
            return result

        except Exception as e:
            await self.log.aerror(
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

        # 根据策略决定是否刷新数据

        async with session_manager.get_session(Database.EVALUATE) as session:
            self.check_record_dal.session = session
            # 查找现有的检查记录
            exist_check_record: Optional[CheckRecord] = (
                await self.check_record_dal.find_one_by(
                    alpha_id=alpha.alpha_id,
                    record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                    order_by=CheckRecord.created_at.desc(),
                )
            )
            await self.log.adebug(
                f"查询现有{check_type_name}检查记录结果",
                emoji="💾" if exist_check_record else "❓",
                alpha_id=alpha.alpha_id,
                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                record_found=bool(exist_check_record),
            )

        action = await self._determine_check_action(
            policy=policy,
            exist_check_record=exist_check_record,
            alpha_id=alpha.alpha_id,
            check_type_name=check_type_name,
        )

        if action == AbstractEvaluateStage.CheckAction.REFRESH:
            return await self._refresh_alpha_pool_performance_diff(alpha)
        elif action == AbstractEvaluateStage.CheckAction.USE_EXISTING:

            async with session_manager.get_session(Database.EVALUATE) as session:
                self.check_record_dal.session = session
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

        await self.log.adebug(
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

                        async with (
                            session_manager.get_session(Database.EVALUATE) as session,
                            session.begin(),
                        ):
                            self.check_record_dal.session = session
                            await self.check_record_dal.create(
                                CheckRecord(
                                    alpha_id=alpha.alpha_id,
                                    record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                                    content=result.model_dump(mode="json"),
                                )
                            )
                        return result
                    elif retry_after and retry_after > 0:
                        await self.log.adebug(
                            "等待重试",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            competition_id=self.competition_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        await self.log.awarning(
                            "刷新因子池绩效差异数据时发生异常",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            competition_id=self.competition_id,
                            retry_after=retry_after,
                        )
                        return None
        except Exception as e:
            await self.log.aerror(
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

        raise NotImplementedError("子类必须实现业绩对比条件的判断逻辑")


class SubmissionEvaluateStage(AbstractEvaluateStage):

    def __init__(
        self,
        next_stage: Optional[AbstractEvaluateStage],
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ) -> None:
        super().__init__(next_stage)
        self.check_record_dal = check_record_dal
        self.client = client
        self.log: BoundLogger = get_logger(
            f"{__name__}.{self.__class__.__name__}",
        )

    async def _evaluate_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        record: EvaluateRecord,
        **kwargs: Any,
    ) -> bool:

        check_type_name = "提交检查"
        record_type = CheckRecordType.SUBMISSION

        await self.log.adebug(
            f"开始评估 {check_type_name}",
            emoji="🔍",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )

        try:
            # 查找现有的检查记录

            async with session_manager.get_session(Database.EVALUATE) as session:
                self.check_record_dal.session = session
                exist_check_record: Optional[CheckRecord] = (
                    await self.check_record_dal.find_one_by(
                        alpha_id=alpha.alpha_id,
                        record_type=record_type,
                        order_by=CheckRecord.created_at.desc(),
                    )
                )
            await self.log.adebug(
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
                    await self.log.awarning(
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
                        await self.log.aerror(
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
                await self.log.aerror(
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
                await self.log.ainfo(
                    f"{check_type_name}评估完成",
                    emoji="✅" if result else "❌",
                    alpha_id=alpha.alpha_id,
                    passed=result,
                )
                return result

            await self.log.aerror(
                f"未能获取或加载{check_type_name}数据，无法执行检查",
                emoji="❌",
                alpha_id=alpha.alpha_id,
            )
            return False

        except Exception as e:
            await self.log.aerror(
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

        await self.log.adebug(
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

                        async with (
                            session_manager.get_session(Database.EVALUATE) as session,
                            session.begin(),
                        ):
                            self.check_record_dal.session = session
                            await self.check_record_dal.create(
                                CheckRecord(
                                    alpha_id=alpha.alpha_id,
                                    record_type=CheckRecordType.SUBMISSION,
                                    content=result.model_dump(),
                                )
                            )
                        return result
                    elif retry_after and retry_after > 0:
                        await self.log.adebug(
                            "等待重试",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        await self.log.awarning(
                            "刷新提交检查数据时发生异常",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            retry_after=retry_after,
                        )
                        return None
        except Exception as e:
            await self.log.aerror(
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
