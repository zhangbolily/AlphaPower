from __future__ import annotations  # 解决类型前向引用问题

import asyncio
from typing import (
    Any,
    AsyncGenerator,
    Optional,
)

import aiostream.stream as stream
from aiostream import Stream

from alphapower.constants import (
    RefreshPolicy,
)
from alphapower.dal.evaluate import EvaluateRecordDAL
from alphapower.entity import Alpha, EvaluateRecord
from alphapower.internal.logging import get_logger

from .alpha_fetcher_abc import AbstractAlphaFetcher
from .evaluate_stage_abc import AbstractEvaluateStage
from .evaluator_abc import AbstractEvaluator

# 获取日志记录器 (logger)
log = get_logger(module_name=__name__)


class BaseEvaluator(AbstractEvaluator):

    def __init__(
        self,
        fetcher: AbstractAlphaFetcher,
        evaluate_stage_chain: AbstractEvaluateStage,
        evaluate_record_dal: EvaluateRecordDAL,
    ):
        super().__init__(fetcher, evaluate_stage_chain, evaluate_record_dal)
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

        processed_count: int = 0
        passed_count: int = 0
        total_to_evaluate: int = await self._get_total_to_evaluate(**kwargs)

        async def evaluate_wrapper(alpha: Alpha, *args: Any) -> Optional[Alpha]:
            """包装单个 Alpha 的评估逻辑，处理异常并记录日志"""
            nonlocal processed_count, passed_count
            try:
                await self._log_start_alpha(alpha)
                passed: bool = await self.evaluate_one(
                    alpha=alpha, policy=policy, **kwargs
                )
                processed_count += 1
                if passed:
                    passed_count += 1
                    await self._log_alpha_passed(alpha)
                    return alpha
                await self._log_alpha_failed(alpha)
                return None
            except asyncio.CancelledError:
                processed_count += 1
                await self._log_alpha_cancelled(alpha)
                return None
            except Exception as task_exc:
                processed_count += 1
                await self._log_alpha_exception(alpha, task_exc)
                return None
            finally:
                await self._log_progress(
                    processed_count, passed_count, total_to_evaluate
                )

        try:
            async for passed_alpha in self._process_alphas(
                evaluate_wrapper, concurrency, **kwargs
            ):
                yield passed_alpha
        except asyncio.CancelledError:
            await log.ainfo("🚫 批量评估任务被取消", emoji="🚫")
            raise
        except Exception as e:
            await log.aerror(
                "💥 批量评估过程中发生未预期异常",
                emoji="💥",
                policy=policy.name,
                concurrency=concurrency,
                error=str(e),
                exc_info=True,
            )
            raise
        finally:
            await self._log_final_statistics(
                processed_count, passed_count, total_to_evaluate
            )

    async def _get_total_to_evaluate(self, **kwargs: Any) -> int:
        """获取待评估 Alpha 的总数"""
        try:
            count = await self.to_evaluate_alpha_count(**kwargs)
            await log.ainfo("🔢 待评估 Alpha 总数", emoji="🔢", count=count)
            return count
        except Exception as e:
            await log.aerror(
                "💥 获取待评估 Alpha 总数失败",
                emoji="💥",
                error=str(e),
                exc_info=True,
            )
            return -1

    async def _process_alphas(
        self,
        evaluate_wrapper: Any,
        concurrency: int,
        **kwargs: Any,
    ) -> AsyncGenerator[Alpha, None]:
        """处理 Alpha 的异步生成器"""
        alpha_source: Stream[Alpha] = stream.iterate(
            self.fetcher.fetch_alphas(**kwargs)
        )
        results_stream: Stream[Optional[Alpha]] = stream.map(
            alpha_source, evaluate_wrapper, task_limit=concurrency
        )
        passed_alphas_stream: Stream[Optional[Alpha]] = stream.filter(
            results_stream, lambda x: x is not None
        )
        try:
            async with (
                passed_alphas_stream.stream() as streamer  # pylint: disable=E1101
            ):
                async for passed_alpha in streamer:
                    if passed_alpha:
                        yield passed_alpha
        except Exception as e:
            await log.aerror(
                "💥 处理 Alpha 流时发生异常",
                emoji="💥",
                error=str(e),
                exc_info=True,
            )
            raise

    async def _log_progress(
        self,
        processed_count: int,
        passed_count: int,
        total_to_evaluate: int,
    ) -> None:
        """记录评估进度日志"""
        progress_percent: float = (
            (processed_count / total_to_evaluate) * 100 if total_to_evaluate > 0 else 0
        )
        await log.ainfo(
            "📊 批量评估进度",
            emoji="📊",
            processed=processed_count,
            passed=passed_count,
            total=total_to_evaluate,
            progress=f"{progress_percent:.2f}%",
        )

    async def _log_final_statistics(
        self,
        processed_count: int,
        passed_count: int,
        total_to_evaluate: int,
    ) -> None:
        """记录最终统计日志"""
        final_total_str: str = (
            str(total_to_evaluate) if total_to_evaluate > 0 else "未知"
        )
        await log.ainfo(
            "🏁 批量评估完成",
            emoji="🏁",
            total_processed=processed_count,
            total_passed=passed_count,
            total_expected=final_total_str,
        )

    async def _log_start_alpha(self, alpha: Alpha) -> None:
        """记录单个 Alpha 开始评估的日志"""
        await log.adebug(
            "⏳ 开始处理单个 Alpha",
            emoji="⏳",
            alpha_id=alpha.alpha_id,
        )

    async def _log_alpha_passed(self, alpha: Alpha) -> None:
        """记录单个 Alpha 评估通过的日志"""
        await log.adebug(
            "✅ Alpha 评估通过",
            emoji="✅",
            alpha_id=alpha.alpha_id,
        )

    async def _log_alpha_failed(self, alpha: Alpha) -> None:
        """记录单个 Alpha 评估未通过的日志"""
        await log.adebug(
            "❌ Alpha 评估未通过",
            emoji="❌",
            alpha_id=alpha.alpha_id,
        )

    async def _log_alpha_cancelled(self, alpha: Alpha) -> None:
        """记录单个 Alpha 评估任务被取消的日志"""
        await log.ainfo(
            "🚫 Alpha 评估任务被取消",
            emoji="🚫",
            alpha_id=alpha.alpha_id,
        )

    async def _log_alpha_exception(self, alpha: Alpha, exception: Exception) -> None:
        """记录单个 Alpha 评估任务中发生异常的日志"""
        await log.aerror(
            "💥 Alpha 评估任务中发生异常",
            emoji="💥",
            alpha_id=alpha.alpha_id,
            error=str(exception),
            exc_info=True,
        )

    async def evaluate_one(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        **kwargs: Any,
    ) -> bool:
        """评估单个 Alpha 对象"""
        await self._log_evaluation_start(alpha, policy, kwargs)
        overall_result: bool = False  # 初始化最终结果

        try:
            evaluate_record: EvaluateRecord = EvaluateRecord(
                alpha_id=alpha.alpha_id,
                in_sample_pnl=0.0,
                in_sample_long_count=0,
                in_sample_short_count=0,
                in_sample_book_size=0.0,
                in_sample_turnover=0.0,
                in_sample_returns=0.0,
                in_sample_drawdown=0.0,
                in_sample_sharpe=0.0,
                in_sample_fitness=0.0,
                self_correlation=0.0,
            )

            # 调用评估阶段链的核心逻辑
            evaluate_record, overall_result = await self._evaluate_alpha_stage(
                alpha, policy, evaluate_record, **kwargs
            )

            # 根据评估结果处理 Alpha
            if overall_result:
                await self._handle_evaluate_success(alpha, evaluate_record, **kwargs)
            else:
                await self._handle_evaluate_failure(alpha, evaluate_record, **kwargs)

            await self._log_evaluation_complete(alpha, overall_result)

        except NotImplementedError as nie:
            await self._log_not_implemented_error(alpha, nie)
            raise  # 重新抛出，表明实现不完整
        except asyncio.CancelledError:
            await self._log_evaluation_cancelled(alpha)
            overall_result = False  # 取消视为失败
            raise  # 重新抛出，让上层处理
        except Exception as e:
            await self._log_unexpected_error(alpha, policy, e)
            overall_result = False  # 异常视为失败
            raise

        return overall_result

    async def _evaluate_alpha_stage(
        self,
        alpha: Alpha,
        policy: RefreshPolicy,
        evaluate_record: EvaluateRecord,
        **kwargs: Any,
    ) -> tuple[EvaluateRecord, bool]:
        """调用评估阶段链的核心逻辑"""
        try:
            return await self.evaluate_stage_chain.evaluate(
                alpha=alpha,
                policy=policy,
                record=evaluate_record,
                **kwargs,
            )
        except Exception as e:
            await log.aerror(
                "💥 评估阶段链执行失败",
                emoji="💥",
                alpha_id=alpha.alpha_id,
                policy=policy,
                error=str(e),
                exc_info=True,
            )
            raise

    async def _log_evaluation_start(
        self, alpha: Alpha, policy: RefreshPolicy, kwargs: Any
    ) -> None:
        """记录评估开始的日志"""
        await log.adebug(
            "🎬 开始评估单个 Alpha",
            emoji="🎬",
            alpha_id=alpha.alpha_id,
            policy=policy,
            kwargs=kwargs,
        )

    async def _log_evaluation_complete(
        self, alpha: Alpha, overall_result: bool
    ) -> None:
        """记录评估完成的日志"""
        await log.ainfo(
            "🏁 Alpha 评估完成",
            emoji="✅" if overall_result else "❌",
            alpha_id=alpha.alpha_id,
            passed=overall_result,
        )

    async def _log_not_implemented_error(
        self, alpha: Alpha, exception: NotImplementedError
    ) -> None:
        """记录未实现错误的日志"""
        await log.aerror(
            "评估失败：子类必须实现必要的检查方法",
            emoji="❌",
            alpha_id=alpha.alpha_id,
            error=str(exception),
            exc_info=True,
        )

    async def _log_evaluation_cancelled(self, alpha: Alpha) -> None:
        """记录评估任务被取消的日志"""
        await log.ainfo(
            "🚫 Alpha 评估任务被取消",
            emoji="🚫",
            alpha_id=alpha.alpha_id,
        )

    async def _log_unexpected_error(
        self, alpha: Alpha, policy: RefreshPolicy, exception: Exception
    ) -> None:
        """记录未预期异常的日志"""
        await log.aerror(
            "💥 评估 Alpha 时发生未预期异常",
            emoji="💥",
            alpha_id=alpha.alpha_id,
            policy=policy,
            error=str(exception),
            exc_info=True,
        )

    async def _handle_evaluate_success(
        self, alpha: Alpha, record: EvaluateRecord, **kwargs: Any
    ) -> None:
        raise NotImplementedError("子类必须实现 _handle_evaluate_success 方法")

    async def _handle_evaluate_failure(
        self, alpha: Alpha, record: EvaluateRecord, **kwargs: Any
    ) -> None:
        raise NotImplementedError("子类必须实现 _handle_evaluate_failure 方法")

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
