import asyncio
from datetime import date
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    SubmissionCheckResultView,
    TableView,
    WorldQuantClient,
)
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CONSULTANT_MAX_SELF_CORRELATION,
    MAX_EFFECTIVE_GENIUS_PYRAMIDS_IN_ALPHA,
    MIN_FORMULATED_PYRAMID_ALPHAS,
    CheckRecordType,
    CorrelationCalcType,
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
from alphapower.entity.evaluate import Correlation
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
        record.is_returns = alpha.in_sample.returns if alpha.in_sample.returns else 0.0
        record.is_drawdown = (
            alpha.in_sample.drawdown if alpha.in_sample.drawdown else 0.0
        )
        record.is_sharpe = alpha.in_sample.sharpe if alpha.in_sample.sharpe else 0.0
        record.is_fitness = alpha.in_sample.fitness if alpha.in_sample.fitness else 0.0
        record.is_margin = alpha.in_sample.margin if alpha.in_sample.margin else 0.0

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

                matched_unformulated_pyramid: List[str] = []

                for pyramid in check.pyramids:
                    key: str = pyramid.name.replace("/", "_").upper()
                    alpha_count: int = self.region_category_delay_map.get(key, 0)
                    if alpha_count < MIN_FORMULATED_PYRAMID_ALPHAS:
                        matched_unformulated_pyramid.append(pyramid.name)
                        await self.log.ainfo(
                            "匹配到 Genius 未点亮金字塔",
                            pyramid=pyramid,
                            key=key,
                            pyramid_alpha_count=alpha_count,
                            min_alpha_count=MIN_FORMULATED_PYRAMID_ALPHAS,
                            emoji="🔆",
                        )

                effective_pyramids: int = check.effective if check.effective else 0
                if (
                    matched_unformulated_pyramid
                    and len(matched_unformulated_pyramid) > effective_pyramids
                ):
                    await self.log.awarning(
                        "匹配到的 Genius 未点亮金字塔超过限制",
                        emoji="❌",
                        alpha_id=alpha.alpha_id,
                        matched_unformulated_pyramid=matched_unformulated_pyramid,
                        effective_pyramids=effective_pyramids,
                        max_pyramids_in_alpha=MAX_EFFECTIVE_GENIUS_PYRAMIDS_IN_ALPHA,
                    )
                    # 一个 Alpha 匹配到的 Pyramid 过多，不会被认为是能点亮 Genius 进度的 Pyramid
                    matched_unformulated_pyramid = []

                record.matched_unformulated_pyramid = matched_unformulated_pyramid  # type: ignore

            if (
                check.name == SubmissionCheckType.MATCHES_THEMES.value
                and check.result == SubmissionCheckResult.PASS
            ):
                record.theme_multiplier = check.multiplier if check.multiplier else 1.0

                themes: List[str] = []
                for theme in check.themes:
                    if theme.name:
                        themes.append(theme.name)
                    else:
                        await self.log.awarning(
                            "主题名称为空，无法处理",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            theme=theme,
                        )
                        continue
                record.themes = themes  # type: ignore

            if check.result == SubmissionCheckResult.FAIL:
                await self.log.awarning(
                    "Alpha 对象的 in_sample 检查未通过",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                return False

            # 处理部分 PENDING 和 WARNING 算通过的情况
            # 需要外部输入配置，指定哪些检查可以通过
            if len(pass_result_set) == 0:
                await self.log.adebug(
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

            if check.result == SubmissionCheckResult.PASS:
                await self.log.adebug(
                    "Alpha 对象的 in_sample 检查通过",
                    emoji="✅",
                    alpha_id=alpha.alpha_id,
                    check_name=check.name,
                    check_result=check.result,
                )
                continue

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
        inner: bool = False,
    ) -> None:

        super().__init__(next_stage)
        self.correlation_calculator: CorrelationCalculator = correlation_calculator
        self._threshold: float = threshold
        self._inner: bool = inner
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
                    inner=self._inner,
                )
            )

            if not pairwise_correlation:
                await self.log.awarning(
                    "自相关性检查未能计算出任何相关性",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                )
                return True

            max_corr: float = max(pairwise_correlation.values(), default=1.0)
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

                for other_alpha, corr in pairwise_correlation.items():
                    if corr >= self._threshold:
                        # 需要比所有相关系数超过阈值的 Alpha Sharpe 有明显提升才可以提交
                        # 检查夏普率（Sharpe Ratio）不为 None，避免除法异常
                        if (
                            alpha.in_sample.sharpe is not None
                            and other_alpha.in_sample.sharpe is not None
                            and other_alpha.in_sample.sharpe != 0
                        ):
                            sharp_improvement: float = (
                                alpha.in_sample.sharpe / other_alpha.in_sample.sharpe
                                - 1
                            )
                            if sharp_improvement < 0.1:
                                await self.log.awarning(
                                    "夏普率提升幅度不足，无法提交",
                                    emoji="❌",
                                    alpha_id=alpha.alpha_id,
                                    other_alpha_id=other_alpha.alpha_id,
                                    alpha_sharpe=alpha.in_sample.sharpe,
                                    other_alpha_sharpe=other_alpha.in_sample.sharpe,
                                    sharp_improvement=sharp_improvement,
                                )
                                return False
                        else:
                            await self.log.awarning(
                                "夏普率为空或为零，无法计算提升幅度",
                                emoji="⚠️",
                                alpha_id=alpha.alpha_id,
                                alpha_sharpe=alpha.in_sample.sharpe,
                                other_alpha_sharpe=other_alpha.in_sample.sharpe,
                            )

                return True

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
        client: WorldQuantBrainClient,
    ) -> None:

        super().__init__(next_stage)
        self.correlation_type: CorrelationType = correlation_type
        self.check_record_dal: CheckRecordDAL = check_record_dal
        self.correlation_dal: CorrelationDAL = correlation_dal
        self.client: WorldQuantBrainClient = client
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

        await self.log.ainfo(
            f"开始评估 {check_type_name} 检查",
            emoji="🔍",
            alpha_id=alpha.alpha_id,
            record_type=record_type,
            policy=policy,
        )

        try:

            async with session_manager.get_session(Database.EVALUATE) as session:
                exist_check_record: Optional[CheckRecord] = (
                    await self.check_record_dal.find_one_by(
                        session=session,
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

    async def _save_correlation_data(
        self,
        corr_type: CorrelationType,
        alpha: Optional[Alpha],
        data: Optional[TableView],
    ) -> None:
        if not alpha:
            await self.log.awarning(
                "空的 Alpha 对象",
                emoji="❌",
                alpha_id=None,
                correlation_type=corr_type,
            )
            return

        if not data:
            await self.log.awarning(
                "尝试保存空的相关性数据",
                emoji="❌",
                alpha_id=None,
                correlation_type=corr_type,
            )
            return

        corr_records: List[Correlation] = []
        check_record: CheckRecord = CheckRecord(
            alpha_id=alpha.alpha_id,
            record_type=(
                CheckRecordType.CORRELATION_SELF
                if self.correlation_type == CorrelationType.SELF
                else CheckRecordType.CORRELATION_PROD
            ),
            content=data.model_dump(mode="python"),
        )

        if corr_type == CorrelationType.SELF:
            corr_df: Optional[pd.DataFrame] = data.to_dataframe()

            if corr_df is None or corr_df.empty:
                await self.log.awarning(
                    "相关性数据为空，无法保存",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    correlation_type=corr_type,
                )
                return

            for _, row in corr_df.iterrows():
                if row["id"] == alpha.alpha_id:
                    continue

                corr_record = Correlation(
                    alpha_id_a=alpha.alpha_id,
                    alpha_id_b=row["id"],
                    correlation=row["correlation"],
                    calc_type=CorrelationCalcType.PLATFORM_SELF,
                )
                corr_records.append(corr_record)

        elif corr_type == CorrelationType.PROD:
            corr_record = Correlation(
                alpha_id_a=alpha.alpha_id,
                alpha_id_b=None,
                correlation=data.max if data.max else 1.0,
                calc_type=CorrelationCalcType.PLATFORM_PROD,
            )
            corr_records.append(corr_record)

        async with (
            session_manager.get_session(Database.EVALUATE) as session,
            session.begin(),
        ):
            await self.correlation_dal.bulk_create(
                entities=corr_records,
                session=session,
            )
            check_record = await self.check_record_dal.create(
                entity=check_record,
                session=session,
            )

        await self.log.ainfo(
            "相关性数据已成功保存",
            emoji="✅",
            corr_type=corr_type,
            alpha_id=alpha.alpha_id,
            correlation_type=corr_type,
            check_record_id=check_record.id if check_record else None,
        )

    async def _refresh_correlation_data(self, alpha: Alpha) -> Optional[TableView]:
        """
        刷新相关性数据，调用平台 API，支持重试机制

        参数:
            alpha: Alpha 实体对象
        返回:
            TableView | None
        """
        await self.log.adebug(
            "准备刷新相关性数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
            correlation_type=self.correlation_type,
        )
        try:
            api_result: TableView = await self.client.fetch_alpha_correlation(
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                override_retry_after=2.0,
            )
            await self.log.ainfo(
                "相关性数据刷新成功",
                emoji="✅",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                api_result_summary=str(api_result)[
                    :80
                ],  # 只输出前 80 字符，避免日志过长
            )
            await self._save_correlation_data(
                corr_type=self.correlation_type,
                alpha=alpha,
                data=api_result,
            )
            return api_result
        except asyncio.TimeoutError as e:
            await self.log.awarning(
                "刷新相关性数据时发生超时异常",
                emoji="⏳",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
            )
            return None
        except ValueError as e:
            await self.log.aerror(
                "刷新相关性数据时发生数据解析异常",
                emoji="📉",
                alpha_id=alpha.alpha_id,
                correlation_type=self.correlation_type,
                error=str(e),
                exc_info=True,
            )
            return None
        except Exception as e:
            await self.log.acritical(
                "刷新相关性数据时发生未知严重异常，程序可能无法继续",
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
            # 查找现有的检查记录
            exist_check_record: Optional[CheckRecord] = (
                await self.check_record_dal.find_one_by(
                    session=session,
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
                record = await self.check_record_dal.find_one_by(
                    session=session,
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
                            await self.check_record_dal.create(
                                session=session,
                                entity=CheckRecord(
                                    alpha_id=alpha.alpha_id,
                                    record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                                    content=result.model_dump(mode="json"),
                                ),
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
                exist_check_record: Optional[CheckRecord] = (
                    await self.check_record_dal.find_one_by(
                        session=session,
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
                            await self.check_record_dal.create(
                                session=session,
                                entity=CheckRecord(
                                    alpha_id=alpha.alpha_id,
                                    record_type=CheckRecordType.SUBMISSION,
                                    content=result.model_dump(),
                                ),
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
