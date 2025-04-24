"""Alpha 数据获取器 (Fetcher) 与评估器 (Evaluator) 的基础实现。

此模块提供了 `AbstractAlphaFetcher` 和 `AbstractEvaluator` 抽象基类的
基础实现版本：`BaseAlphaFetcher` 和 `BaseEvaluator`。
这些基础类继承了抽象方法，但默认实现会抛出 `NotImplementedError`，
需要子类根据具体业务逻辑进行覆盖。
"""

from __future__ import annotations  # 解决类型前向引用问题

from datetime import datetime
from typing import Any, AsyncGenerator, List, Optional, cast

from sqlalchemy import ColumnExpressionArgument, Select, and_, case, func, select
from sqlalchemy.orm import selectinload

from alphapower import constants  # 导入常量模块
from alphapower.constants import AlphaType, Database, Delay, Region, Stage
from alphapower.dal.alphas import AggregateDataDAL, AlphaDAL
from alphapower.dal.session_manager import session_manager
from alphapower.entity import AggregateData, Alpha
from alphapower.internal.logging import get_logger

from .alpha_fetcher_abc import AbstractAlphaFetcher

logger = get_logger(module_name=__name__)


class BaseAlphaFetcher(AbstractAlphaFetcher):
    """Alpha 数据获取器的基础实现。

    继承自 `AbstractAlphaFetcher`，为所有抽象方法提供了默认的
    `NotImplementedError` 实现。子类应覆盖这些方法以提供具体的
    数据筛选和获取逻辑。
    """

    def __init__(
        self,
        alpha_dal: AlphaDAL,
        aggregate_data_dal: AggregateDataDAL,
        **kwargs: Any,
    ):
        """初始化 BaseAlphaFetcher。

        Args:
            alpha_dal: Alpha 数据访问层对象。
            sample_dal: Sample 数据访问层对象。
            setting_dal: Setting 数据访问层对象。
        """
        super().__init__(alpha_dal, aggregate_data_dal)
        self._fetched_count: int = 0  # 追踪已获取的 Alpha 数量

        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        if kwargs:
            # 处理额外的参数 (如果有)
            for key, value in kwargs.items():
                setattr(self, key, value)

    async def _build_alpha_select_query(
        self,
        **kwargs: Any,
    ) -> Select:
        """构建用于筛选 Alpha 的 SQLAlchemy 查询对象 (Select Object)。

        根据世坤 (WorldQuant) 顾问因子过滤要求 (Consultant Alpha Filtering Requirements)
        构建查询，使用 `alphapower.constants` 中定义的阈值。

        Args:
            **kwargs: 额外的筛选参数 (当前未使用，但保留以备将来扩展)。

        Returns:
            构建好的 SQLAlchemy Select 查询对象。

        Raises:
            NotImplementedError: 如果子类没有实现具体的筛选逻辑 (虽然基类提供了实现)。
        """
        await logger.adebug(
            "🏗️ 开始构建 Alpha 筛选查询 (使用常量)",
            emoji="🏗️",
            filter_kwargs=kwargs,
        )

        # 定义连接条件别名，提高可读性
        query: Select = (
            select(Alpha)
            .join(Alpha.in_sample)  # 连接到 Alpha 的样本内数据
            .options(
                selectinload(Alpha.in_sample),  # 预加载样本内数据
            )
        )

        # 构建筛选条件列表
        # 注意：常量中的百分比值需要除以 100 转换为小数
        criteria: List[ColumnExpressionArgument] = [
            Alpha.stage == Stage.IS,
            # Sample 相关条件 (通用)
            AggregateData.turnover
            > (constants.CONSULTANT_TURNOVER_MIN_PERCENT / 100.0),
            AggregateData.turnover
            < (constants.CONSULTANT_TURNOVER_MAX_PERCENT / 100.0),
            # 区域和延迟相关的条件 (使用 case 语句)
            case(
                (
                    Alpha.region != Region.CHN,  # 非中国区域
                    case(
                        (
                            Alpha.delay == Delay.ZERO,  # 延迟为 0
                            and_(
                                AggregateData.sharpe
                                > constants.CONSULTANT_SHARPE_THRESHOLD_DELAY_0,
                                AggregateData.fitness
                                > constants.CONSULTANT_FITNESS_THRESHOLD_DELAY_0,
                            ),
                        ),
                        (
                            Alpha.delay == Delay.ONE,  # 延迟为 1
                            and_(
                                AggregateData.sharpe
                                > constants.CONSULTANT_SHARPE_THRESHOLD_DELAY_1,
                                AggregateData.fitness
                                > constants.CONSULTANT_FITNESS_THRESHOLD_DELAY_1,
                            ),
                        ),
                        else_=False,  # 如果 delay 不是 0 或 1，则不满足条件
                    ),
                ),
                # 中国区域 (else 分支)
                else_=case(
                    (
                        Alpha.delay == Delay.ZERO,  # 延迟为 0
                        and_(
                            AggregateData.sharpe
                            > constants.CONSULTANT_CHN_SHARPE_THRESHOLD_DELAY_0,
                            AggregateData.returns
                            > (
                                constants.CONSULTANT_CHN_RETURNS_MIN_PERCENT_DELAY_0
                                / 100.0
                            ),
                            AggregateData.fitness
                            >= constants.CONSULTANT_CHN_FITNESS_THRESHOLD_DELAY_0,
                        ),
                    ),
                    (
                        Alpha.delay == Delay.ONE,  # 延迟为 1
                        and_(
                            AggregateData.sharpe
                            > constants.CONSULTANT_CHN_SHARPE_THRESHOLD_DELAY_1,
                            AggregateData.returns
                            > (
                                constants.CONSULTANT_CHN_RETURNS_MIN_PERCENT_DELAY_1
                                / 100.0
                            ),
                            AggregateData.fitness
                            >= constants.CONSULTANT_CHN_FITNESS_THRESHOLD_DELAY_1,
                        ),
                    ),
                    else_=False,  # 如果 delay 不是 0 或 1，则不满足条件
                ),
            ),
            # 超级 Alpha (Superalphas) 的特殊换手率条件
            case(
                (
                    Alpha.type == AlphaType.SUPER,  # 如果是超级 Alpha
                    and_(
                        AggregateData.turnover
                        >= (
                            constants.CONSULTANT_SUPERALPHA_TURNOVER_MIN_PERCENT / 100.0
                        ),
                        AggregateData.turnover
                        < (
                            constants.CONSULTANT_SUPERALPHA_TURNOVER_MAX_PERCENT / 100.0
                        ),
                    ),
                ),
                # 如果不是超级 Alpha，则此条件为 True (不应用额外过滤)
                else_=True,
            ),
        ]

        if self.start_time:
            # 如果指定了开始时间，则添加时间范围条件
            criteria.append(Alpha.date_created >= self.start_time)
        if self.end_time:
            # 如果指定了结束时间，则添加时间范围条件
            criteria.append(Alpha.date_created <= self.end_time)

        # 应用筛选条件到查询
        final_query: Select = query.where(and_(*criteria))

        # 记录构建完成的查询 (截断长查询)
        query_str = str(final_query)
        log_query = query_str[:70] + "..." if len(query_str) > 70 else query_str
        await logger.adebug(
            "✅ Alpha 筛选查询构建完成 (使用常量)",
            emoji="✅",
            query=log_query,
            full_query_len=len(query_str),
        )
        return final_query

    async def fetch_alphas(
        self,
        **kwargs: Any,
    ) -> AsyncGenerator[Alpha, None]:
        """异步获取符合筛选条件的 Alpha 实体。

        根据 `_build_alpha_select_query` 构建的查询，执行并异步产生 Alpha 对象。

        Args:
            **kwargs: 传递给 `self._build_alpha_select_query` 的参数字典。

        Yields:
            逐个返回符合筛选条件的 `Alpha` 实体对象。

        Raises:
            Exception: 如果在数据库查询或流式处理过程中发生错误。
        """
        await logger.ainfo("🚀 <= 开始执行 fetch_alphas", emoji="🚀", **kwargs)
        query: Select = await self._build_alpha_select_query(**kwargs)
        query_str = str(query)
        log_query = query_str[:70] + "..." if len(query_str) > 70 else query_str
        await logger.adebug(
            "构建的 Alpha 查询",
            query=log_query,
            full_query_len=len(query_str),
        )

        try:
            async with session_manager.get_session(Database.ALPHAS) as session:
                async for alpha in self.alpha_dal.execute_stream_query(
                    query, session=session
                ):
                    self._fetched_count += 1
                    await logger.adebug(
                        "🔍 获取到 Alpha",
                        emoji="🔍",
                        alpha_id=alpha.id,
                        current_fetched_count=self._fetched_count,
                    )
                    yield alpha
            await logger.ainfo(
                "✅ => fetch_alphas 执行完成",
                emoji="✅",
                total_fetched=self._fetched_count,
            )
        except Exception as e:
            await logger.aerror(
                "❌ fetch_alphas 执行时发生错误",
                emoji="❌",
                error=str(e),  # 记录错误信息字符串
                kwargs=kwargs,
                exc_info=True,  # 包含堆栈信息
            )
            raise  # 重新抛出异常，让上层处理

    async def total_alpha_count(
        self,
        **kwargs: Any,
    ) -> int:
        """获取符合筛选条件的 Alpha 总数量。

        执行计数查询以确定满足条件的 Alpha 总数。

        Args:
            **kwargs: 传递给 `self._build_alpha_select_query` 的参数字典。

        Returns:
            符合筛选条件的 Alpha 实体总数。
        """
        await logger.ainfo("🔢 开始计算 Alpha 总数", emoji="🔢", **kwargs)
        query: Select = await self._build_alpha_select_query(**kwargs)
        # 计算符合条件的 Alpha 实体总数
        count_query = select(func.count()).select_from(  # pylint: disable=E1102
            query.subquery()
        )
        await logger.adebug("构建的计数查询", query=str(count_query))

        try:
            async with session_manager.get_session(Database.ALPHAS) as session:
                result = await session.execute(count_query)
                count = cast(int, result.scalar())
            await logger.ainfo("✅ Alpha 总数计算完成", emoji="✅", total_count=count)
            return count
        except Exception as e:
            await logger.aerror(
                "❌ 计算 Alpha 总数时发生错误",
                emoji="❌",
                error=e,
                exc_info=True,
            )
            raise  # 重新抛出异常

    async def fetched_alpha_count(
        self,
        **kwargs: Any,  # pylint: disable=unused-argument
    ) -> int:
        """获取已获取的 Alpha 数量。

        Returns:
            已通过 `fetch_alphas` 获取的 Alpha 对象数量。
        """
        await logger.ainfo("📊 开始统计已获取 Alpha 数量", emoji="📊")
        count = self._fetched_count
        await logger.adebug("当前已获取 Alpha 数量", count=count)
        await logger.ainfo("✅ 已获取 Alpha 数量统计完成", emoji="✅", count=count)
        return count

    async def remaining_alpha_count(
        self,
        **kwargs: Any,
    ) -> int:
        """计算剩余待获取的 Alpha 数量。

        通过总数减去已获取数计算。

        Args:
            **kwargs: 传递给 `total_alpha_count` 的参数。

        Returns:
            剩余待获取的 Alpha 对象数量。
        """
        await logger.ainfo("⏳ 计算剩余 Alpha 数量", emoji="⏳", **kwargs)
        try:
            total = await self.total_alpha_count(**kwargs)
            fetched = await self.fetched_alpha_count(**kwargs)
            remaining = total - fetched
            await logger.adebug(
                "剩余 Alpha 数量计算详情",
                total=total,
                fetched=fetched,
                remaining=remaining,
                **kwargs,
            )
            await logger.ainfo(
                "✅ 剩余 Alpha 数量计算完成", emoji="✅", remaining=remaining
            )
            return remaining
        except Exception as e:
            await logger.aerror(
                "❌ 计算剩余 Alpha 数量失败",
                emoji="❌",
                error=e,
                exc_info=True,
            )
            raise  # 重新抛出异常
            raise  # 重新抛出异常
