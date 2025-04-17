from __future__ import annotations  # 解决类型前向引用问题

import asyncio
from enum import Enum, auto
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from alphapower.client import BeforeAndAfterPerformanceView, TableView, WorldQuantClient
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CONSULTANT_MAX_SELF_CORRELATION,
    CheckRecordType,
    CorrelationCalcType,
    CorrelationType,
    RefreshPolicy,
)
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.entity import Alpha
from alphapower.entity.evaluate import CheckRecord, Correlation
from alphapower.internal.logging import get_logger

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .evaluator_abc import AbstractEvaluator

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class BaseEvaluator(AbstractEvaluator):

    class CheckAction(Enum):
        """指示检查数据时应执行的操作"""

        REFRESH = auto()  # 需要刷新数据
        USE_EXISTING = auto()  # 使用已存在的记录
        SKIP = auto()  # 根据策略跳过检查 (当记录不存在时)
        FAIL_MISSING = auto()  # 因记录不存在且策略不允许刷新而失败
        ERROR = auto()  # 无效的策略或状态组合

    def __init__(
        self,
        fetcher: AbstractAlphaFetcher,
        correlation_dal: CorrelationDAL,
        check_record_dal: CheckRecordDAL,
        client: WorldQuantClient,
    ):
        super().__init__(fetcher, correlation_dal, check_record_dal, client)
        # 使用同步日志记录器，因为 __init__ 通常是同步的
        log.info("📊 BaseEvaluator 初始化完成", emoji="📊")

    async def evaluate_many(
        self,
        policy: RefreshPolicy,
        concurrency: int,
        **kwargs: Any,
    ) -> AsyncGenerator[Alpha, None]:
        await log.adebug(
            "🚧 evaluate_many 方法尚未实现，需要子类覆盖",
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
        **kwargs: Any,
    ) -> bool:
        await log.adebug(
            "🚧 evaluate_one 方法尚未实现，需要子类覆盖",
            emoji="🚧",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 evaluate_one 方法")

    async def to_evaluate_alpha_count(
        self,
        **kwargs: Any,
    ) -> int:
        await log.adebug(
            "准备调用 fetcher 获取待评估 Alpha 总数", emoji="🔢", kwargs=kwargs
        )
        # 直接调用 fetcher 的方法
        count = await self.fetcher.total_alpha_count(**kwargs)
        await log.adebug("成功获取待评估 Alpha 总数", emoji="✅", count=count)
        return count

    async def _get_checks_to_run(
        self, alpha: Alpha, **kwargs: Any
    ) -> Tuple[List[CheckRecordType], RefreshPolicy]:
        await log.adebug(
            "🚧 _get_checks_to_run 方法尚未实现，需要子类覆盖",
            emoji="🚧",
            alpha_id=alpha.alpha_id,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _get_checks_to_run 方法")

    async def _execute_checks(
        self,
        alpha: Alpha,
        checks: List[CheckRecordType],
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> Dict[CheckRecordType, bool]:
        await log.adebug(
            "🚧 _execute_checks 方法尚未实现，需要子类覆盖",
            emoji="🚧",
            alpha_id=alpha.alpha_id,
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
        **kwargs: Any,
    ) -> bool:
        record_type = (
            CheckRecordType.CORRELATION_SELF
            if corr_type == CorrelationType.SELF
            else CheckRecordType.CORRELATION_PROD
        )
        check_type_name = "相关性"  # 用于日志
        await log.adebug(
            f"开始检查 Alpha {check_type_name}",
            emoji="🔗",
            alpha_id=alpha.alpha_id,
            correlation_type=corr_type,
            policy=policy,
            kwargs=kwargs,
        )

        check_result: bool = False
        correlation_content: Optional[TableView] = None

        try:
            # 1. 查找现有的检查记录
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

            # 2. 根据策略决定执行什么操作
            action: BaseEvaluator.CheckAction = await self._determine_check_action(
                policy=policy,
                exist_check_record=exist_check_record,
                check_type_name=check_type_name,
                alpha_id=alpha.alpha_id,
            )

            # 3. 根据操作执行逻辑
            if action == BaseEvaluator.CheckAction.REFRESH:
                refreshed_result: Optional[TableView] = (
                    await self._refresh_correlation_data(alpha, corr_type)
                )
                if refreshed_result:
                    correlation_content = refreshed_result
                else:
                    await log.awarning(
                        f"{check_type_name}数据刷新失败，检查不通过",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        correlation_type=corr_type,
                    )
                    check_result = False
                    return check_result  # 刷新失败直接返回

            elif (
                action == BaseEvaluator.CheckAction.USE_EXISTING and exist_check_record
            ):
                correlation_content = TableView.model_validate(
                    exist_check_record.content
                )

            elif action == BaseEvaluator.CheckAction.SKIP:
                check_result = False  # 跳过视为不通过
                return check_result  # 跳过直接返回

            elif action == BaseEvaluator.CheckAction.FAIL_MISSING:
                check_result = False  # 因缺失而失败
                return check_result  # 失败直接返回

            elif action == BaseEvaluator.CheckAction.ERROR:
                await log.aerror(
                    f"处理 {check_type_name} 检查遇到错误状态",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    policy=policy,
                )
                check_result = False
                # 可以选择抛出异常或直接返回 False
                # raise ValueError(f"无效的检查策略 '{policy}' 或状态组合")
                return check_result

            # 4. 判断检查是否通过 (如果获取或加载了内容)
            if correlation_content:
                check_result = self._determine_correlation_pass_status(
                    correlation_content, corr_type, **kwargs
                )
                await log.ainfo(
                    "Alpha 相关性检查判定完成",
                    emoji="✅" if check_result else "❌",
                    alpha_id=alpha.alpha_id,
                    correlation_type=corr_type,
                    check_passed=check_result,
                )
            else:
                # 如果 correlation_content 仍然是 None (理论上不应发生，除非刷新失败已返回)
                await log.aerror(
                    "未能获取或加载相关性数据，无法执行检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    correlation_type=corr_type,
                    policy=policy,
                )
                check_result = False

        except asyncio.CancelledError:
            await log.ainfo(
                "Alpha 相关性检查任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
                correlation_type=corr_type,
            )
            check_result = False  # 取消视为检查不通过
            # 不向上抛出，评估流程应能处理
        except Exception as e:
            await log.aerror(
                "检查 Alpha 相关性时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=corr_type,
                policy=policy,
                error=str(e),
                exc_info=True,
            )
            check_result = False  # 异常视为检查不通过
            # 可以选择是否向上抛出，取决于评估流程设计
            # raise

        await log.adebug("结束检查 Alpha 相关性", emoji="🏁", check_result=check_result)
        return check_result

    async def _refresh_correlation_data(
        self, alpha: Alpha, corr_type: CorrelationType
    ) -> Optional[TableView]:
        await log.adebug(
            "开始刷新 Alpha 相关性数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
            correlation_type=corr_type,
        )
        record_type = (
            CheckRecordType.CORRELATION_SELF
            if corr_type == CorrelationType.SELF
            else CheckRecordType.CORRELATION_PROD
        )
        final_result: Optional[TableView] = None

        try:
            # 注意：这里的 self.client 应该由外部管理生命周期
            # async with self.client: # 假设 client 实例是持久的或由外部管理
            while True:
                await log.adebug(
                    "执行单次相关性检查 API 调用",
                    emoji="📞",
                    alpha_id=alpha.alpha_id,
                    corr_type=corr_type,
                )
                finished: bool
                retry_after: Optional[float]
                api_result: Optional[TableView]
                finished, retry_after, api_result = (
                    await self.client.alpha_correlation_check(
                        alpha_id=alpha.alpha_id,
                        corr_type=corr_type,
                    )
                )
                await log.adebug(
                    "相关性检查 API 调用返回",
                    emoji="📥",
                    alpha_id=alpha.alpha_id,
                    corr_type=corr_type,
                    finished=finished,
                    retry_after=retry_after,
                    # result=api_result # 可能包含大量数据，谨慎打印
                )

                if finished:
                    if api_result:
                        await log.ainfo(
                            "相关性数据 API 获取成功",
                            emoji="🎉",
                            alpha_id=alpha.alpha_id,
                            corr_type=corr_type,
                        )
                        final_result = api_result
                        # --- 存储结果 ---
                        check_record: CheckRecord = CheckRecord(
                            alpha_id=alpha.alpha_id,
                            record_type=record_type,
                            content=final_result.model_dump(mode="python"),
                        )
                        await self.check_record_dal.create(
                            check_record,
                        )
                        await log.adebug(
                            "相关性检查记录已保存",
                            emoji="💾",
                            alpha_id=alpha.alpha_id,
                            record_type=record_type,
                        )

                        # 如果是自相关性，解析并存储具体的相关性值
                        if corr_type == CorrelationType.SELF and final_result.records:
                            correlations: List[Correlation] = (
                                self._parse_self_correlation_result(
                                    alpha.alpha_id, final_result
                                )
                            )
                            if correlations:
                                await self.correlation_dal.bulk_upsert(correlations)
                                await log.adebug(
                                    "自相关性详细数据已批量更新/插入",
                                    emoji="💾",
                                    alpha_id=alpha.alpha_id,
                                    count=len(correlations),
                                )
                        # --- 存储结束 ---
                        break  # 成功获取并处理，退出循环
                    else:
                        # API 完成但无结果
                        await log.awarning(
                            "相关性检查 API 声称完成，但未返回有效结果",
                            emoji="❓",
                            alpha_id=alpha.alpha_id,
                            corr_type=corr_type,
                        )
                        final_result = None  # 明确标记失败
                        break  # 退出循环

                elif retry_after and retry_after > 0:
                    # 检查未完成，按建议时间等待后重试
                    await log.adebug(
                        "相关性检查未完成，将在指定时间后重试",
                        emoji="⏳",
                        alpha_id=alpha.alpha_id,
                        corr_type=corr_type,
                        retry_after=round(retry_after, 2),
                    )
                    await asyncio.sleep(retry_after)
                else:
                    # API 返回既未完成也无重试时间，视为异常情况
                    await log.awarning(
                        "相关性检查 API 返回异常状态：未完成且无重试时间",
                        emoji="❓",
                        alpha_id=alpha.alpha_id,
                        corr_type=corr_type,
                        finished=finished,
                        retry_after=retry_after,
                    )
                    final_result = None  # 明确标记失败
                    break  # 退出循环
        except asyncio.CancelledError:
            await log.ainfo(
                "刷新相关性数据任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
                correlation_type=corr_type,
            )
            final_result = None  # 标记失败
            raise  # 重新抛出，让上层处理取消状态
        except Exception as e:
            await log.aerror(
                "刷新相关性数据过程中发生未预期异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                correlation_type=corr_type,
                error=str(e),
                exc_info=True,
            )
            final_result = None  # 标记失败
            # 不再向上抛出，返回 None 表示刷新失败

        await log.adebug(
            "结束刷新 Alpha 相关性数据",
            emoji="🏁",
            alpha_id=alpha.alpha_id,
            correlation_type=corr_type,
            success=bool(final_result),
        )
        return final_result

    def _parse_self_correlation_result(
        self, alpha_id_a: str, result: TableView
    ) -> List[Correlation]:
        correlations: List[Correlation] = []
        try:
            corr_index: int = result.table_schema.index_of("correlation")
            alpha_id_index: int = result.table_schema.index_of("id")

            if corr_index == -1 or alpha_id_index == -1:
                log.error(  # 使用同步日志，因为这是纯计算方法
                    "自相关性检查结果中缺少必要的字段",
                    emoji="❌",
                    alpha_id=alpha_id_a,
                    schema=result.table_schema.model_dump(mode="python"),
                )
                raise ValueError("自相关性检查结果中缺少必要的字段，无法解析相关性数据")

            if not result.records:
                log.error(
                    "自相关性检查结果为空",
                    emoji="❌",
                    alpha_id=alpha_id_a,
                )
                raise ValueError("自相关性检查结果为空，无法解析相关性数据")

            for record in result.records:
                try:
                    # 确保 record 是列表或元组，并且索引有效
                    if isinstance(record, (list, tuple)) and len(record) > max(
                        alpha_id_index, corr_index
                    ):
                        alpha_id_b: str = str(record[alpha_id_index])  # 确保是字符串
                        corr_value_raw: Any = record[corr_index]
                        # 尝试将相关性值转换为浮点数
                        corr_value: float = float(corr_value_raw)

                        # 忽略与自身的相关性 (通常为 1 或未定义)
                        if alpha_id_a == alpha_id_b:
                            continue

                        correlation: Correlation = Correlation(
                            alpha_id_a=alpha_id_a,
                            alpha_id_b=alpha_id_b,
                            correlation=corr_value,
                            calc_type=CorrelationCalcType.PLATFORM,  # 标记为平台计算
                        )
                        correlations.append(correlation)
                    else:
                        log.error(
                            "自相关性检查结果记录格式无效",
                            emoji="❌",
                            alpha_id=alpha_id_a,
                            record=record,
                        )
                        raise ValueError(
                            "自相关性检查结果记录格式无效，无法解析相关性数据"
                        )
                except (ValueError, TypeError, IndexError) as parse_err:
                    log.error(
                        "解析自相关性检查结果记录时发生错误",
                        emoji="❌",
                        alpha_id=alpha_id_a,
                        record=record,
                        error=str(parse_err),
                    )
                    raise ValueError(
                        "解析自相关性检查结果记录时发生错误，无法解析相关性数据"
                    ) from parse_err

        except Exception as e:
            log.error(
                "解析自相关性结果时发生未预期异常",
                emoji="💥",
                alpha_id=alpha_id_a,
                error=str(e),
                exc_info=True,
            )
            raise

        return correlations

    def _determine_correlation_pass_status(
        self, content: TableView, corr_type: CorrelationType, **kwargs: Any
    ) -> bool:
        # 使用同步日志，因为这是纯计算方法
        log.debug(
            "开始判定相关性检查是否通过",
            emoji="🤔",
            correlation_type=corr_type,
            kwargs=kwargs,
        )
        try:
            max_corr: float = content.max or 0.0
            min_corr: float = content.min or 0.0

            if corr_type == CorrelationType.SELF:
                if max_corr > CONSULTANT_MAX_SELF_CORRELATION:
                    log.error(
                        "相关性检查未通过，最大相关性超过阈值",
                        emoji="❌",
                        correlation_type=corr_type,
                        max_corr=max_corr,
                        min_corr=min_corr,
                    )
                    return False

                log.info(
                    "相关性检查通过",
                    emoji="✅",
                    correlation_type=corr_type,
                    max_corr=max_corr,
                    min_corr=min_corr,
                )
                return True  # 通过
            elif corr_type == CorrelationType.PROD:
                if max_corr > CONSULTANT_MAX_PROD_CORRELATION:
                    log.error(
                        "相关性检查未通过，最大相关性超过阈值",
                        emoji="❌",
                        correlation_type=corr_type,
                        max_corr=max_corr,
                        min_corr=min_corr,
                    )
                    return False

                log.info(
                    "相关性检查通过",
                    emoji="✅",
                    correlation_type=corr_type,
                    max_corr=max_corr,
                    min_corr=min_corr,
                )
                return True
            else:
                log.error("未知的相关性类型", emoji="❓", correlation_type=corr_type)
                return False  # 未知类型视为失败

        except Exception as e:
            log.error(
                "判定相关性检查状态时发生异常",
                emoji="💥",
                correlation_type=corr_type,
                error=str(e),
                exc_info=True,
            )
            return False  # 异常视为失败

    async def _determine_performance_diff_pass_status(
        self,
        alpha: Alpha,
        perf_diff_view: BeforeAndAfterPerformanceView,
        **kwargs: Any,
    ) -> bool:
        # 使用同步日志，因为这是纯计算方法
        raise NotImplementedError("绩效差异检查逻辑必须由子类实现")

    async def _refresh_alpha_pool_performance_diff(
        self,
        alpha: Alpha,
        competition_id: Optional[str],
    ) -> BeforeAndAfterPerformanceView:
        await log.adebug(
            "准备刷新 Alpha 因子池绩效差异数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
            competition_id=competition_id,
        )
        try:
            async with self.client:
                finished: bool = False
                retry_after: Optional[float] = None
                result: Optional[BeforeAndAfterPerformanceView] = None

                while not finished:
                    finished, retry_after, result = (
                        await self.client.alpha_fetch_before_and_after_performance(
                            alpha_id=alpha.alpha_id,
                            competition_id=competition_id,
                        )
                    )

                    if finished:
                        if isinstance(result, BeforeAndAfterPerformanceView):
                            await log.ainfo(
                                "成功获取 Alpha 因子池绩效差异数据",
                                emoji="✅",
                                alpha_id=alpha.alpha_id,
                                competition_id=competition_id,
                            )
                            check_record: CheckRecord = CheckRecord(
                                alpha_id=alpha.alpha_id,
                                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                                content=result.model_dump(),
                            )
                            await self.check_record_dal.create(check_record)
                            await log.adebug(
                                "绩效差异数据已存入 CheckRecord",
                                emoji="💾",
                                alpha_id=alpha.alpha_id,
                                competition_id=competition_id,
                                check_record_id=check_record.id,  # 假设 CheckRecord 有 id
                            )
                            return result
                        else:
                            # API 返回 finished=True 但 result 不是预期类型
                            await log.aerror(
                                "Alpha 绩效数据计算完成，但返回结果类型不匹配",
                                emoji="❌",
                                alpha_id=alpha.alpha_id,
                                competition_id=competition_id,
                                result_type=type(result).__name__,
                            )
                            raise TypeError(
                                f"预期结果类型 BeforeAndAfterPerformanceView，实际为 {type(result)}"
                            )
                    elif retry_after and retry_after > 0.0:
                        await log.adebug(
                            "Alpha 绩效数据计算中，等待重试...",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            competition_id=competition_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        # API 返回 finished=False 且没有 retry_after，视为计算失败
                        await log.awarning(
                            "Alpha 绩效数据计算失败或未提供重试时间",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            competition_id=competition_id,
                            finished=finished,
                            retry_after=retry_after,
                        )
                        # 根据业务逻辑决定是抛出异常还是返回特定值/None
                        raise RuntimeError(f"Alpha {alpha.id} 绩效数据计算失败")

                # 如果循环结束，说明任务被取消或发生了其他异常
                await log.aerror(
                    "Alpha 绩效数据刷新任务被取消或发生异常",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    competition_id=competition_id,
                    finished=finished,
                    retry_after=retry_after,
                )
                raise RuntimeError(f"Alpha {alpha.id} 绩效数据刷新任务被取消或发生异常")
        except asyncio.CancelledError:
            await log.ainfo(
                "Alpha 绩效数据刷新任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
                competition_id=competition_id,
            )
            raise
        except Exception as e:
            await log.aerror(
                "刷新 Alpha 绩效数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                competition_id=competition_id,
                error=str(e),
                exc_info=True,  # 添加堆栈信息
            )
            raise

    async def _check_alpha_pool_performance_diff(
        self,
        alpha: Alpha,
        competition_id: Optional[str],
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        check_type_name: str = "因子池绩效差异"  # 用于日志
        record_type: CheckRecordType = CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE
        await log.adebug(
            f"开始检查 Alpha {check_type_name}",
            emoji="🔍",
            alpha_obj_id=alpha.id,
            alpha_id=alpha.alpha_id,
            competition_id=competition_id,
            policy=policy,
            kwargs=kwargs,
        )

        check_result: bool = False  # 初始化检查结果
        perf_diff_view: Optional[BeforeAndAfterPerformanceView] = None

        try:
            # 1. 查找现有的检查记录
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

            # 2. 根据策略决定执行什么操作
            action: BaseEvaluator.CheckAction = await self._determine_check_action(
                policy=policy,
                exist_check_record=exist_check_record,
                alpha_id=alpha.alpha_id,
                check_type_name=check_type_name,
            )

            # 3. 根据操作执行逻辑
            if action == BaseEvaluator.CheckAction.REFRESH:
                try:
                    perf_diff_view = await self._refresh_alpha_pool_performance_diff(
                        alpha=alpha,
                        competition_id=competition_id,
                    )
                    if not perf_diff_view:
                        # 刷新函数返回 None 表示失败
                        await log.awarning(
                            f"{check_type_name}数据刷新失败，检查不通过",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            competition_id=competition_id,
                        )
                        check_result = False
                        return check_result  # 刷新失败直接返回
                except (RuntimeError, TypeError) as refresh_err:
                    # 捕获刷新函数可能抛出的已知业务或类型错误
                    await log.awarning(
                        f"{check_type_name}数据刷新失败，检查不通过",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        competition_id=competition_id,
                        error=str(refresh_err),
                    )
                    check_result = False
                    return check_result  # 刷新失败直接返回
                # 注意：CancelledError 和其他 Exception 会在外部 try...except 中捕获

            elif action == BaseEvaluator.CheckAction.USE_EXISTING:
                # _determine_check_action 保证了 exist_check_record 在此非空
                await log.adebug(
                    f"根据策略使用现有{check_type_name}数据",
                    emoji="💾",
                    alpha_id=alpha.alpha_id,
                    competition_id=competition_id,
                    policy=policy,
                )
                try:
                    # 从记录中加载数据用于后续判断
                    # 断言确保类型检查器知道 exist_check_record 不为 None
                    assert exist_check_record is not None
                    perf_diff_view = BeforeAndAfterPerformanceView(
                        **exist_check_record.content
                    )
                except (
                    TypeError,
                    ValueError,
                    KeyError,
                ) as parse_err:  # 捕获解析/验证错误
                    await log.aerror(
                        f"解析现有{check_type_name}记录时出错",
                        emoji="❌",
                        alpha_id=alpha.alpha_id,
                        record_id=(
                            exist_check_record.id if exist_check_record else "N/A"
                        ),
                        error=str(parse_err),
                        exc_info=True,
                    )
                    check_result = False  # 解析失败视为检查不通过
                    perf_diff_view = None  # 确保后续不执行判断逻辑

            elif action == BaseEvaluator.CheckAction.SKIP:
                # 日志已在 _determine_check_action 中记录
                check_result = False  # 跳过视为不通过
                return check_result  # 跳过直接返回

            elif action == BaseEvaluator.CheckAction.FAIL_MISSING:
                # 日志已在 _determine_check_action 中记录
                check_result = False  # 因缺失而失败
                return check_result  # 失败直接返回

            elif action == BaseEvaluator.CheckAction.ERROR:
                # 日志已在 _determine_check_action 中记录
                check_result = False
                # 可以选择抛出异常或直接返回 False
                # raise ValueError(f"无效的检查策略 '{policy}' 或状态组合")
                return check_result

            # 4. 执行检查逻辑 (如果成功获取或加载了 perf_diff_view)
            if perf_diff_view:
                check_result = await self._determine_performance_diff_pass_status(
                    alpha=alpha,
                    perf_diff_view=perf_diff_view,
                    competition_id=competition_id,
                    **kwargs,
                )
                await log.ainfo(
                    "Alpha 绩效差异检查判定完成",
                    emoji="✅" if check_result else "❌",
                    alpha_id=alpha.alpha_id,
                    competition_id=competition_id,
                    check_passed=check_result,
                )

                return check_result  # 返回检查结果

            # 如果 perf_diff_view 仍然是 None (例如刷新失败、解析失败)
            # 之前的逻辑应该已经处理并可能返回了，但为了健壮性，再次检查
            # 仅在 check_result 仍为 False 时记录错误 (避免重复记录)
            if not check_result:
                await log.aerror(
                    f"未能获取或加载{check_type_name}数据，无法执行检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    competition_id=competition_id,
                    policy=policy,
                    action=action.name,  # 记录导致此状态的动作
                )
            # check_result 保持之前的状态 (通常是 False)

        except asyncio.CancelledError:
            await log.ainfo(
                f"Alpha {check_type_name}检查任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
                competition_id=competition_id,
            )
            check_result = False  # 取消视为检查不通过
            raise  # 重新抛出 CancelledError，让上层处理
        except Exception as e:
            await log.aerror(
                f"检查 Alpha {check_type_name}时发生未预期异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                competition_id=competition_id,
                policy=policy,
                error=str(e),
                exc_info=True,  # 添加堆栈信息
            )
            check_result = False  # 异常视为检查不通过
            raise  # 重新抛出未捕获的异常，表明评估流程中出现严重问题

        await log.adebug(
            f"结束检查 Alpha {check_type_name}", emoji="🏁", check_result=check_result
        )
        return check_result

    async def _check_submission(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        await log.adebug(
            "🚧 _check_submission 方法尚未实现，需要子类覆盖",
            emoji="🚧",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )
        raise NotImplementedError("子类必须实现 _check_submission 方法")

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
        action: BaseEvaluator.CheckAction

        if policy == RefreshPolicy.FORCE_REFRESH:
            action = BaseEvaluator.CheckAction.REFRESH
            await log.adebug(
                f"策略为强制刷新，动作：刷新 {check_type_name} 数据",
                emoji="🔄",
                alpha_id=alpha_id,
            )
        elif policy == RefreshPolicy.REFRESH_ASYNC_IF_MISSING:
            if not exist_check_record:
                action = BaseEvaluator.CheckAction.REFRESH
                await log.adebug(
                    f"策略为缺失时刷新且记录不存在，动作：刷新 {check_type_name} 数据",
                    emoji="🔄",
                    alpha_id=alpha_id,
                )
            else:
                action = BaseEvaluator.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为缺失时刷新且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
        elif policy == RefreshPolicy.USE_EXISTING:
            if exist_check_record:
                action = BaseEvaluator.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为仅使用现有且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
            else:
                action = BaseEvaluator.CheckAction.FAIL_MISSING
                await log.ainfo(
                    f"策略为仅使用现有但记录不存在，动作：{check_type_name} 检查失败",
                    emoji="🚫",
                    alpha_id=alpha_id,
                )
        elif policy == RefreshPolicy.SKIP_IF_MISSING:
            if exist_check_record:
                action = BaseEvaluator.CheckAction.USE_EXISTING
                await log.adebug(
                    f"策略为缺失时跳过且记录存在，动作：使用现有 {check_type_name} 数据",
                    emoji="💾",
                    alpha_id=alpha_id,
                )
            else:
                action = BaseEvaluator.CheckAction.SKIP
                await log.ainfo(
                    f"策略为缺失时跳过且记录不存在，动作：跳过 {check_type_name} 检查",
                    emoji="⏭️",
                    alpha_id=alpha_id,
                )
        else:
            action = BaseEvaluator.CheckAction.ERROR
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
