from __future__ import annotations  # 解决类型前向引用问题

import asyncio
from enum import Enum, auto
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

import aiostream.stream as stream
from aiostream import Stream
from pydantic import TypeAdapter

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    SubmissionCheckResultView,
    TableView,
    WorldQuantClient,
)
from alphapower.client.models import CompetitionRefView
from alphapower.constants import (
    CONSULTANT_MAX_PROD_CORRELATION,
    CONSULTANT_MAX_SELF_CORRELATION,
    CheckRecordType,
    CorrelationCalcType,
    CorrelationType,
    RefreshPolicy,
    SampleCheckResult,
    SampleCheckType,
)
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.entity import Alpha
from alphapower.entity.evaluate import CheckRecord, Correlation
from alphapower.internal.logging import get_logger

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .evaluator_abc import AbstractEvaluator
from .self_correlation_calculator import SelfCorrelationCalculator

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
        correlation_calculator: SelfCorrelationCalculator,
    ):
        super().__init__(
            fetcher, correlation_dal, check_record_dal, client, correlation_calculator
        )
        # 使用同步日志记录器，因为 __init__ 通常是同步的
        log.info("📊 BaseEvaluator 初始化完成", emoji="📊")

    async def evaluate_many(
        self,
        policy: RefreshPolicy,
        concurrency: int,
        **kwargs: Any,
    ) -> AsyncGenerator[Alpha, None]:
        await log.ainfo(
            "🚀 开始批量评估 Alpha (aiostream 模式)",
            emoji="🚀",
            policy=policy.name,
            concurrency=concurrency,
            kwargs=kwargs,
        )

        processed_count: int = 0  # 初始化已处理计数器
        passed_count: int = 0  # 初始化已通过计数器
        total_to_evaluate: int = -1  # 初始化待评估总数

        # 内部包装函数，用于调用 evaluate_one 并处理结果/异常
        async def evaluate_wrapper(alpha: Alpha, *args: Any) -> Optional[Alpha]:
            """
            包裹 evaluate_one 以处理异常并返回 Alpha 或 None。

            Args:
                alpha: 待评估的 Alpha 对象 (Alpha)。

            Returns:
                如果评估通过则返回 Alpha 对象，否则返回 None (Optional[Alpha])。
            """
            nonlocal processed_count  # 允许修改外部作用域的计数器
            try:
                await log.adebug(
                    "开始处理单个 Alpha 评估任务 (aiostream wrapper)",
                    emoji="⏳",
                    alpha_id=alpha.alpha_id,
                    args=args,
                )
                # 调用核心评估逻辑
                passed: bool = await self.evaluate_one(
                    alpha=alpha, policy=policy, **kwargs
                )
                processed_count += 1  # 增加已处理计数
                if passed:
                    await log.adebug(
                        "Alpha 评估通过 (aiostream wrapper)",
                        emoji="✅",
                        alpha_id=alpha.alpha_id,
                    )
                    return alpha  # 评估通过，返回 Alpha 对象

                await log.adebug(
                    "Alpha 评估未通过 (aiostream wrapper)",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                )
                return None  # 评估未通过，返回 None
            except asyncio.CancelledError:
                processed_count += 1  # 取消也算处理
                await log.ainfo(
                    "单个 Alpha 评估任务被取消 (aiostream wrapper)",
                    emoji="🚫",
                    alpha_id=alpha.alpha_id,
                )
                # 不重新抛出，让 aiostream 处理
                return None
            except Exception as task_exc:
                processed_count += 1  # 异常也算处理
                await log.aerror(
                    "💥 单个 Alpha 评估任务中发生异常 (aiostream wrapper)",
                    emoji="💥",
                    alpha_id=alpha.alpha_id,
                    error=str(task_exc),
                    exc_info=True,  # 包含异常堆栈信息
                )
                # 不重新抛出，返回 None 表示失败
                return None
            finally:
                progress_percent: float = (processed_count / total_to_evaluate) * 100
                await log.ainfo(
                    "📊 批量评估进度 (aiostream)",
                    emoji="📊",
                    processed=processed_count,
                    passed=passed_count,  # 注意：这里的 passed_count 可能稍微滞后
                    total=total_to_evaluate,
                    progress=f"{progress_percent:.2f}%",
                )

        # 结束 evaluate_wrapper

        try:
            # 获取待评估总数 (用于日志记录和进度)
            try:
                total_to_evaluate = await self.to_evaluate_alpha_count(**kwargs)
                await log.ainfo(
                    "🔢 待评估 Alpha 总数",
                    emoji="🔢",
                    count=total_to_evaluate,
                )
            except Exception as e:
                await log.aerror(
                    "💥 获取待评估 Alpha 总数失败",
                    emoji="💥",
                    error=str(e),
                    exc_info=True,
                )
                total_to_evaluate = -1  # 标记未知

            # 1. 创建 Alpha 源流
            # 使用 self.fetcher.fetch_alphas 获取异步生成器
            alpha_source: Stream[Alpha] = stream.iterate(
                self.fetcher.fetch_alphas(**kwargs)
            )

            # 2. 使用 map 并发执行评估
            # task_limit 控制并发数
            results_stream: Stream[Optional[Alpha]] = stream.map(
                alpha_source, evaluate_wrapper, task_limit=concurrency
            )

            # 3. 过滤掉评估失败或未通过的结果 (None)
            # 使用 filter 保留非 None 的结果 (即通过评估的 Alpha)
            passed_alphas_stream: Stream[Optional[Alpha]] = stream.filter(
                results_stream, lambda x: x is not None
            )

            # 4. 异步迭代最终结果流并 yield
            async with (
                passed_alphas_stream.stream() as streamer  # pylint: disable=E1101
            ):
                async for passed_alpha in streamer:
                    passed_count += 1  # 增加通过计数

                    if not passed_alpha:
                        await log.aerror(
                            "💥 评估结果为 None，可能是异常或未通过",
                            emoji="💥",
                        )
                        continue

                    yield passed_alpha  # 产生通过评估的 Alpha

        except asyncio.CancelledError:
            await log.ainfo("🚫 批量评估任务被取消 (aiostream)", emoji="🚫")
            # aiostream 应该会处理内部任务的取消，这里记录总体取消事件
            raise  # 重新抛出 CancelledError，让调用者知道任务被取消
        except Exception as e:
            await log.aerror(
                "💥 批量评估过程中发生未预期异常 (aiostream)",
                emoji="💥",
                policy=policy.name,
                concurrency=concurrency,
                error=str(e),
                exc_info=True,  # 包含异常堆栈信息
            )
            # 根据需要决定是否重新抛出异常
            raise  # 重新抛出，表明批量评估失败

        finally:
            # 记录最终的评估结果统计
            final_total_str: str = (
                str(total_to_evaluate) if total_to_evaluate != -1 else "未知"
            )
            await log.ainfo(
                "🏁 批量评估完成 (aiostream)",
                emoji="🏁",
                total_processed=processed_count,  # 记录实际处理的数量
                total_passed=passed_count,  # 记录通过评估的数量
                total_expected=final_total_str,  # 记录预期处理的总数
            )

    async def evaluate_one(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        await log.adebug(
            "🎬 开始评估单个 Alpha",
            emoji="🎬",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )
        overall_result: bool = False  # 初始化最终结果

        try:
            # 1. 获取需要运行的检查列表和实际使用的策略
            checks_to_run: List[CheckRecordType]
            effective_policy: RefreshPolicy
            checks_to_run, checks_kwargs, effective_policy = (
                await self._get_checks_to_run(alpha=alpha, policy=policy, **kwargs)
            )
            await log.adebug(
                "📋 确定需要执行的检查列表",
                emoji="📋",
                alpha_id=alpha.alpha_id,
                checks=checks_to_run,
                effective_policy=effective_policy,
            )

            if not checks_to_run:
                await log.ainfo(
                    "🤔 没有需要为该 Alpha 执行的检查，评估跳过 (视为失败)",
                    emoji="🤔",
                    alpha_id=alpha.alpha_id,
                )
                return True  # 没有检查，默认通过或根据业务逻辑调整

            # 2. 执行检查
            check_results: Dict[CheckRecordType, bool] = await self._execute_checks(
                alpha=alpha,
                checks=checks_to_run,
                checks_kwargs=checks_kwargs,
                policy=effective_policy,  # 使用从 _get_checks_to_run 返回的策略
                **kwargs,
            )
            await log.adebug(
                "📊 各项检查执行结果",
                emoji="📊",
                alpha_id=alpha.alpha_id,
                results=check_results,
            )

            # 3. 判断总体结果 (要求所有执行的检查都通过)
            # 确保所有在 checks_to_run 中的检查都在 check_results 中，并且值为 True
            overall_result = all(
                check_results.get(check, False) for check in checks_to_run
            )

            await log.ainfo(
                "🏁 Alpha 评估完成",
                emoji="✅" if overall_result else "❌",
                alpha_id=alpha.alpha_id,
                passed=overall_result,
            )

        except NotImplementedError as nie:
            await log.aerror(
                "评估失败：子类必须实现必要的检查方法",
                emoji="❌",
                alpha_id=alpha.alpha_id,
                error=str(nie),
                exc_info=True,
            )
            raise  # 重新抛出，表明实现不完整
        except asyncio.CancelledError:
            await log.ainfo(
                "🚫 Alpha 评估任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
            )
            overall_result = False  # 取消视为失败
            raise  # 重新抛出，让上层处理
        except Exception as e:
            await log.aerror(
                "💥 评估 Alpha 时发生未预期异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                policy=policy,
                error=str(e),
                exc_info=True,
            )
            overall_result = False  # 异常视为失败
            raise

        return overall_result

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
    ) -> Tuple[List[CheckRecordType], Dict[str, Any], RefreshPolicy]:
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
        checks_kwargs: Dict[str, Any],
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> Dict[CheckRecordType, bool]:
        await log.adebug(
            "🚀 开始执行 Alpha 的各项检查",
            emoji="🚀",
            alpha_id=alpha.alpha_id,
            checks=[c.name for c in checks],  # 记录检查名称列表
            policy=policy.name,
            kwargs=kwargs,
        )
        results: Dict[CheckRecordType, bool] = {}
        # 定义检查类型到检查方法的映射
        check_method_map: Dict[CheckRecordType, Callable] = {
            CheckRecordType.CORRELATION_SELF: lambda: self._check_correlation(
                alpha, CorrelationType.SELF, policy, **checks_kwargs
            ),
            CheckRecordType.CORRELATION_PROD: lambda: self._check_correlation(
                alpha, CorrelationType.PROD, policy, **checks_kwargs
            ),
            CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE: lambda: self._check_alpha_pool_performance_diff(
                alpha,
                policy,
                **checks_kwargs,
            ),
            CheckRecordType.SUBMISSION: lambda: self._check_submission(
                alpha, policy, **checks_kwargs
            ),
            # 添加其他检查类型的映射...
        }

        for check_type in checks:
            check_method = check_method_map.get(check_type)
            if not check_method:
                await log.aerror(
                    "❌ 未找到检查类型的实现方法",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    check_type=check_type.name,
                )
                # 或者根据需要抛出 NotImplementedError
                raise NotImplementedError(
                    f"检查类型 '{check_type.name}' 的执行方法未实现"
                )

            await log.adebug(
                f"▶️ 开始执行检查: {check_type.name}",
                emoji="▶️",
                alpha_id=alpha.alpha_id,
            )
            try:
                # 调用对应的检查方法
                result: bool = await check_method()
                results[check_type] = result
                await log.adebug(
                    f"⏹️ 完成检查: {check_type.name}",
                    emoji="✅" if result else "❌",
                    alpha_id=alpha.alpha_id,
                    result=result,
                )

                if not result:
                    await log.ainfo(
                        f"⚠️ 检查 '{check_type.name}' 结果为 False",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                    )
                    return results  # 如果检查失败，提前返回结果

            except NotImplementedError as nie:
                await log.aerror(
                    f"🚧 检查 '{check_type.name}' 未在子类中实现",
                    emoji="🚧",
                    alpha_id=alpha.alpha_id,
                    error=str(nie),
                    exc_info=True,
                )
                raise
            except asyncio.CancelledError:
                await log.ainfo(
                    f"🚫 检查 '{check_type.name}' 被取消",
                    emoji="🚫",
                    alpha_id=alpha.alpha_id,
                )
                results[check_type] = False  # 取消视为失败
                raise  # 重新抛出 CancelledError
            except Exception as e:
                await log.aerror(
                    f"💥 执行检查 '{check_type.name}' 时发生异常",
                    emoji="💥",
                    alpha_id=alpha.alpha_id,
                    check_type=check_type.name,
                    policy=policy.name,
                    error=str(e),
                    exc_info=True,
                )
                raise

        await log.adebug(
            "🏁 完成所有请求的检查执行",
            emoji="🏁",
            alpha_id=alpha.alpha_id,
            results={k.name: v for k, v in results.items()},  # 记录结果
        )
        return results

    async def _check_correlation_local(self, alpha: Alpha) -> bool:
        try:
            pairwise_correlation: Dict[str, float] = (
                await self.correlation_calculator.calculate_self_correlation(
                    alpha=alpha
                )
            )

            for alpha_id, corr in pairwise_correlation.items():
                if corr > CONSULTANT_MAX_SELF_CORRELATION:
                    await log.awarning(
                        "自相关性检查未通过，最大相关性超过阈值",
                        emoji="❌",
                        alpha_id_a=alpha.alpha_id,
                        alpha_id_b=alpha_id,
                        correlation=corr,
                    )
                    return False

            await log.ainfo(
                "自相关性检查通过",
                emoji="✅",
                alpha_id=alpha.alpha_id,
                max_corr=max(pairwise_correlation.values(), default=0.0),
            )
            return True
        except Exception as e:
            await log.aerror(
                "💥 计算自相关性时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,
            )
            return False

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

        if corr_type == CorrelationType.SELF:
            # 向平台发起自相关性检查之前，先在本地检查过滤一次
            local_check_result: bool = await self._check_correlation_local(alpha)
            if not local_check_result:
                await log.awarning(
                    "本地自相关性检查未通过，跳过平台检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                )
                return False

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
                                content=result.model_dump(mode="json"),
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
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:

        competition_id: Optional[str]
        if "competition_id" in kwargs:
            competition_id = kwargs["competition_id"]

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

    async def _refresh_submission_check_data(
        self,
        alpha: Alpha,
        **kwargs: Any,
    ) -> Optional[SubmissionCheckResultView]:
        await log.adebug(
            "开始刷新 Alpha 提交检查数据",
            emoji="🔄",
            alpha_id=alpha.alpha_id,
            kwargs=kwargs,
        )
        try:
            async with self.client:
                finished: bool = False
                retry_after: Optional[float] = None
                result: Optional[SubmissionCheckResultView] = None

                while not finished:
                    finished, retry_after, result = (
                        await self.client.alpha_fetch_submission_check_result(
                            alpha_id=alpha.alpha_id,
                        )
                    )

                    if finished:
                        if isinstance(result, SubmissionCheckResultView):
                            await log.ainfo(
                                "成功获取 Alpha 提交检查数据",
                                emoji="✅",
                                alpha_id=alpha.alpha_id,
                            )

                            # TODO: 更新 Alpha 中 Sample 的逻辑太复杂，后面有时间再说
                            check_record: CheckRecord = CheckRecord(
                                alpha_id=alpha.alpha_id,
                                record_type=CheckRecordType.SUBMISSION,
                                content=result.model_dump(),
                            )
                            await self.check_record_dal.create(check_record)
                            await log.adebug(
                                "提交检查记录已保存",
                                emoji="💾",
                                alpha_id=alpha.alpha_id,
                                check_record_id=check_record.id,
                            )
                            return result
                        else:
                            await log.aerror(
                                "Alpha 提交检查 API 返回结果类型不匹配",
                                emoji="❌",
                                alpha_id=alpha.alpha_id,
                                result_type=type(result).__name__,
                            )
                            raise TypeError(
                                f"预期结果类型 SubmissionCheckResultView，实际为 {type(result)}"
                            )
                    elif retry_after and retry_after > 0.0:
                        await log.adebug(
                            "Alpha 提交检查未完成，等待重试...",
                            emoji="⏳",
                            alpha_id=alpha.alpha_id,
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                    else:
                        await log.awarning(
                            "Alpha 提交检查 API 返回异常状态：未完成且无重试时间",
                            emoji="⚠️",
                            alpha_id=alpha.alpha_id,
                            finished=finished,
                            retry_after=retry_after,
                        )
                        raise RuntimeError(f"Alpha {alpha.id} 提交检查失败")
                # 如果循环结束，说明任务被取消或发生了其他异常
                await log.aerror(
                    "Alpha 提交检查刷新任务被取消或发生异常",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    finished=finished,
                    retry_after=retry_after,
                )
                raise RuntimeError(f"Alpha {alpha.id} 提交检查刷新任务被取消或发生异常")
        except asyncio.CancelledError:
            await log.ainfo(
                "Alpha 提交检查刷新任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
            )
            raise
        except Exception as e:
            await log.aerror(
                "刷新 Alpha 提交检查数据时发生异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                error=str(e),
                exc_info=True,  # 添加堆栈信息
            )
            raise
        await log.adebug(
            "结束刷新 Alpha 提交检查数据",
            emoji="🏁",
            alpha_id=alpha.alpha_id,
            success=bool(result),
        )
        # 结束刷新
        return result

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
            if check.result != SampleCheckResult.PASS:
                return False

        return True

    async def _check_submission(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        check_type_name: str = "提交检查"  # 用于日志
        record_type: CheckRecordType = CheckRecordType.SUBMISSION
        await log.adebug(
            f"开始检查 Alpha {check_type_name}",
            emoji="🔍",
            alpha_obj_id=alpha.id,
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )
        check_result: bool = False  # 初始化检查结果
        submission_check_view: Optional[SubmissionCheckResultView] = None
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
                submission_check_view = await self._refresh_submission_check_data(
                    alpha=alpha, **kwargs
                )
                if not submission_check_view:
                    await log.awarning(
                        f"{check_type_name}数据刷新失败，检查不通过",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                    )
                    check_result = False
                    return check_result  # 刷新失败直接返回

            elif action == BaseEvaluator.CheckAction.USE_EXISTING:
                # _determine_check_action 保证了 exist_check_record 在此非空
                await log.adebug(
                    f"根据策略使用现有{check_type_name}数据",
                    emoji="💾",
                    alpha_id=alpha.alpha_id,
                    policy=policy,
                )
                try:
                    # 从记录中加载数据用于后续判断
                    assert exist_check_record is not None
                    submission_check_view = SubmissionCheckResultView(
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
                    submission_check_view = None
                # 注意：如果解析失败，submission_check_view 将为 None
            elif action == BaseEvaluator.CheckAction.SKIP:
                # 日志已在 _determine_check_action 中记录
                check_result = False
                return check_result  # 跳过直接返回
            elif action == BaseEvaluator.CheckAction.FAIL_MISSING:
                # 日志已在 _determine_check_action 中记录
                check_result = False
                return check_result  # 失败直接返回
            elif action == BaseEvaluator.CheckAction.ERROR:
                # 日志已在 _determine_check_action 中记录
                check_result = False
                return check_result
                # 可以选择抛出异常或直接返回 False
                # raise ValueError(f"无效的检查策略 '{policy}' 或状态组合")
            # 4. 执行检查逻辑 (如果成功获取或加载了 submission_check_view)
            if submission_check_view:
                check_result = await self._determine_submission_pass_status(
                    submission_check_view=submission_check_view,
                    **kwargs,
                )
                await log.ainfo(
                    "Alpha 提交检查判定完成",
                    emoji="✅" if check_result else "❌",
                    alpha_id=alpha.alpha_id,
                    check_passed=check_result,
                )
                return check_result  # 返回检查结果
            # 如果 submission_check_view 仍然是 None (例如刷新失败、解析失败)
            # 之前的逻辑应该已经处理并可能返回了，但为了健壮性，再次检查
            # 仅在 check_result 仍为 False 时记录错误 (避免重复记录)
            if not check_result:
                await log.aerror(
                    f"未能获取或加载{check_type_name}数据，无法执行检查",
                    emoji="❌",
                    alpha_id=alpha.alpha_id,
                    policy=policy,
                    action=action.name,  # 记录导致此状态的动作
                )
            # check_result 保持之前的状态 (通常是 False)
        except asyncio.CancelledError:
            await log.ainfo(
                f"Alpha {check_type_name}检查任务被取消",
                emoji="🚫",
                alpha_id=alpha.alpha_id,
            )
            check_result = False
            raise  # 重新抛出 CancelledError，让上层处理
        except Exception as e:
            await log.aerror(
                f"检查 Alpha {check_type_name}时发生未预期异常",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                policy=policy,
                error=str(e),
                exc_info=True,  # 添加堆栈信息
            )
            check_result = False
            raise  # 重新抛出未捕获的异常，表明评估流程中出现严重问题
        await log.adebug(
            f"结束检查 Alpha {check_type_name}", emoji="🏁", check_result=check_result
        )
        # 结束检查
        return check_result

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

    async def matched_competitions(
        self, alpha: Alpha
    ) -> Tuple[List[CompetitionRefView], SampleCheckResult]:
        await log.adebug(
            "开始获取 Alpha 匹配的竞赛列表",
            emoji="🔍",
            alpha_id=alpha.alpha_id,
        )
        # 创建 TypeAdapter 实例，用于验证和解析 JSON 数据到 CompetitionRefView 列表
        competitions_adapter: TypeAdapter[List[CompetitionRefView]] = TypeAdapter(
            List[CompetitionRefView]
        )

        # 确保 in_sample 存在且已加载 (如果使用延迟加载)
        # 注意：如果 in_sample 可能为 None，需要先检查
        if not alpha.in_sample:
            await log.awarning(
                "Alpha 缺少样本内 (in_sample) 数据，无法获取匹配竞赛",
                emoji="⚠️",
                alpha_id=alpha.alpha_id,
            )
            return [], SampleCheckResult.DEFAULT

        # 遍历 Alpha 的样本内 (in_sample) 检查项
        for check in alpha.in_sample.checks:
            # 检查项名称是否为匹配竞赛
            if check.name == SampleCheckType.MATCHES_COMPETITION.value:
                # 检查项中是否有竞赛信息
                if check.competitions:
                    try:
                        # 使用 TypeAdapter 验证并解析 JSON 字符串
                        competitions: List[CompetitionRefView] = (
                            competitions_adapter.validate_python(check.competitions)
                        )
                        await log.adebug(
                            "成功解析匹配的竞赛列表",
                            emoji="✅",
                            alpha_id=alpha.alpha_id,
                            competitions_count=len(competitions),
                            competitions=competitions,  # 如果列表不长，可以考虑打印
                        )
                        return competitions, check.result
                    except Exception as e:
                        # 如果解析失败，记录错误并抛出 ValueError
                        await log.aerror(
                            "解析竞赛列表 JSON 时出错",
                            emoji="❌",
                            alpha_id=alpha.alpha_id,
                            check_name=check.name,
                            competitions_json=check.competitions,
                            error=str(e),
                            exc_info=True,  # 记录异常堆栈
                        )
                        raise ValueError(
                            f"Alpha (ID: {alpha.alpha_id}) 的 "
                            f"{check.name} 检查项中的竞赛列表 JSON 无效: {e}"
                        ) from e
                else:
                    # 如果有匹配竞赛的检查项但无竞赛数据，记录警告并抛出 ValueError
                    await log.awarning(
                        "匹配竞赛检查项存在，但竞赛列表为空",
                        emoji="⚠️",
                        alpha_id=alpha.alpha_id,
                        check_name=check.name,
                    )
                    # 根据需求决定是否抛出异常，或者仅记录警告并返回空列表
                    # raise ValueError(
                    #     f"Alpha (ID: {alpha.alpha_id}) 的 "
                    #     f"{check.name} 检查项存在，但没有对应的竞赛项数据。"
                    # )
                    return [], SampleCheckResult.DEFAULT

        # 如果遍历完所有检查项都没有找到匹配的竞赛项，返回空列表
        await log.adebug(
            "未找到匹配的竞赛检查项",
            emoji="🤷",
            alpha_id=alpha.alpha_id,
        )
        return [], SampleCheckResult.DEFAULT
