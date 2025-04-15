"""
模块名称: checks

模块功能:
    提供数据相关性检查功能，包括自相关性检查和生产相关性检查。
    使用异步方法执行检查，并通过日志记录检查的过程和结果。

主要类:
    - BaseEvaluator: 提供相关性检查的核心功能。

依赖:
    - asyncio: 用于异步操作。
    - structlog: 用于结构化日志记录。
    - alphapower.client: 提供与 WorldQuant 客户端的交互。
    - alphapower.constants: 定义相关性类型的枚举。
    - alphapower.internal.logging: 提供日志初始化功能。

日志:
    - 使用 structlog 记录模块初始化、检查过程和结果。
    - 日志级别包括 INFO、WARNING、ERROR 等，支持 Emoji 表情丰富日志内容。
"""

import asyncio
from datetime import datetime

# 导入 AsyncGenerator
from typing import AsyncGenerator, ClassVar, List, Optional, Tuple  # 导入 ClassVar

from pydantic import TypeAdapter

# 导入 and_, join, Select, case, select, ColumnExpressionArgument
from sqlalchemy import ColumnExpressionArgument, Select, and_, case, select
from structlog.stdlib import BoundLogger

from alphapower.client import (
    BeforeAndAfterPerformanceView,
    CompetitionRefView,
    TableView,
    WorldQuantClient,
    wq_client,
)
from alphapower.constants import (
    AlphaType,
    CheckRecordType,
    CheckType,
    CorrelationCalcType,
    CorrelationType,
    Database,
    Delay,
    Region,
    Universe,
    UserRole,
)

# 导入 Alpha, Sample, Setting 实体
from alphapower.dal.base import DALFactory
from alphapower.dal.evaluate import CheckRecordDAL, CorrelationDAL
from alphapower.entity import Alpha, CheckRecord, Correlation, Sample, Setting
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import setup_logging

logger: BoundLogger = setup_logging(module_name=__name__)

# TODO: 相关检查依赖 Alpha 上下文，需要做封装和注入
# TODO: 完成检查结果写入数据库的操作


class BaseEvaluator:
    """
    基础评估器类，用于执行 Alpha 数据的相关性检查和性能评估。

    该类提供了多种检查方法，包括自相关性检查 (Self-correlation Check)、
    生产相关性检查 (Production Correlation Check) 以及前后性能对比检查。
    相关性检查的结果会通过日志记录。
    该类使用异步方法执行检查，并在检查完成后处理结果。

    Attributes:
        _alpha (Alpha): 需要评估的 Alpha 实体对象。
        user_alpha_pick_filter (ClassVar[Optional[Select]]): 用户因子筛选查询 (待实现)。
        consultant_alpha_pick_filter (ClassVar[Optional[Select]]): 顾问因子筛选查询。

    Methods:
        matched_competitions: 获取与 Alpha 匹配的竞赛列表。
        correlation_check: 检查数据的相关性 (自相关或生产相关)。
        self_correlation_check: 检查数据的自相关性。
        prod_correlation_check: 检查数据的生产相关性。
        before_and_after_performance_check: 检查数据在特定竞赛中的前后性能表现。
    """

    # 世坤 (WorldQuant) 用户因子过滤基本要求
    # TODO(ballchang): 不紧急，有时间再实现用户因子的具体过滤条件
    user_alpha_pick_filter: ClassVar[Optional[Select]] = None

    # 世坤 (WorldQuant) 顾问因子过滤基本要求
    # 适用于除中国 (CHN) 区域外的 Alpha:
    # - 适应度 (Fitness): 延迟 0 (delay 0) 时 alpha > 1.5，延迟 1 (delay 1) 时 alpha > 1
    # - 夏普比率 (Sharpe Ratio): 延迟 0 时 alpha > 2.69，延迟 1 时 alpha > 1.58
    # - 换手率 (Turnover): 大于 1% 且小于 70%
    # - 权重 (Weight): 任何单一股票的最大权重 < 10%。衡量是否有足够数量的股票被赋予显著权重。
    #   具体数量取决于模拟范围 (如 top 3000, top 2000 等)。
    # - 子宇宙测试 (Sub-universe Test): 在不同的子市场或股票池中的夏普比率必须高于特定阈值。
    #   这些阈值会随着子宇宙规模的减小而降低。
    # - 自相关性 (Self-correlation): PNL (Profit and Loss, 盈亏) 序列与用户其他 Alpha 的相关性 < 0.7，
    #   或者夏普比率至少比用户提交的其他相关 Alpha 高 10%。
    # - 生产相关性 (Prod-correlation): 与自相关标准相同，但适用于 BRAIN 平台中提交的所有 Alpha，
    #   而不仅仅是用户自己的 Alpha。
    # - 样本内夏普比率/阶梯测试 (IS-Sharpe or IS-Ladder Test): 样本内最近 2, 3, ..., 10 年的夏普比率
    #   应高于为延迟 1 (D1) 和延迟 0 (D0) 设置的夏普比率阈值。
    # - 偏差测试 (Bias Test): 测量 Alpha 中是否存在任何前向偏差 (Forward Bias)。
    #   对于表达式生成的 Alpha (Expression Alphas)，此测试不应失败。
    # 适用于中国 (CHN) 地区的 Alpha:
    # - 由于中国市场交易成本较高，要求的回报也更高。
    # - 延迟 1 (D1) 提交标准: 夏普比率 >= 2.08, 收益率 (Returns) >= 8%, 适应度 (Fitness) >= 1.0
    # - 延迟 0 (D0) 提交标准: 夏普比率 >= 3.5, 收益率 (Returns) >= 12%, 适应度 (Fitness) >= 1.5
    # - 附加测试: 稳健宇宙检验性能 (Robust Universe Test Performance) - 如果稳健宇宙 (Robust Universe)
    #   成分保留了提交版本至少 40% 的收益和夏普值，则认为 Alpha 表现良好。
    # 超级 Alpha (Superalphas):
    # - 适用与普通 Alpha 相同的提交标准，但换手率要求更严格: 2% <= Turnover < 40%。
    @staticmethod
    def _build_consultant_alpha_select_query() -> Select:
        """
        构建顾问因子筛选的 SQL 查询 (基于 in_sample 数据)。

        此方法构建一个 SQLAlchemy 查询对象，用于筛选满足顾问标准的 Alpha。
        筛选条件基于 Alpha 的设置 (`Setting`) 和其样本内 (`in_sample`) 的表现 (`Sample`)。
        查询显式地连接了 Alpha, Setting, 和 Sample 表。

        Returns:
            Select: 构建好的 SQLAlchemy 查询对象。
        """
        logger.debug("开始构建顾问因子筛选查询 (基于 in_sample)", emoji="🛠️")

        # 定义连接条件别名，提高可读性
        # 注意：这里使用 Alpha.in_sample 和 Alpha.settings 关系进行连接
        # SQLAlchemy ORM 会自动处理外键关联
        query: Select = (
            select(Alpha)
            .join(Alpha.settings)  # 连接到 Alpha 的设置
            .join(Alpha.in_sample)  # 连接到 Alpha 的样本内数据
        )

        # 构建筛选条件列表
        # 注意：现在可以直接引用 Setting 和 Sample 的属性
        criteria: List[ColumnExpressionArgument] = [
            # Sample 相关条件
            Sample.self_correration < 0.7,
            Sample.turnover > 0.01,
            Sample.turnover < 0.7,
            # 区域和延迟相关的条件 (使用 case 语句)
            case(
                (
                    Setting.region != Region.CHN,  # 非中国区域
                    case(
                        (
                            Setting.delay == 0,  # 延迟为 0
                            # 使用 and_ 组合多个条件
                            and_(Sample.sharpe > 2.69, Sample.fitness > 1.5),
                        ),
                        (
                            Setting.delay == 1,  # 延迟为 1
                            and_(Sample.sharpe > 1.58, Sample.fitness > 1.0),
                        ),
                        # 可选：为不匹配的 delay 添加 else 条件
                        else_=False,  # 如果 delay 不是 0 或 1，则不满足条件
                    ),
                ),
                # 中国区域 (else 分支)
                else_=case(
                    (
                        Setting.delay == 0,  # 延迟为 0
                        and_(
                            Sample.sharpe > 3.5,
                            Sample.returns > 0.12,
                            Sample.fitness >= 1.5,
                        ),
                    ),
                    (
                        Setting.delay == 1,  # 延迟为 1
                        and_(
                            Sample.sharpe > 2.08,
                            Sample.returns > 0.08,
                            Sample.fitness >= 1.0,
                        ),
                    ),
                    # 可选：为不匹配的 delay 添加 else 条件
                    else_=False,  # 如果 delay 不是 0 或 1，则不满足条件
                ),
            ),
            # 超级 Alpha 的特殊换手率条件
            case(
                (
                    Alpha.type == AlphaType.SUPER,  # 如果是超级 Alpha
                    and_(Sample.turnover >= 0.02, Sample.turnover < 0.4),
                ),
                # 如果不是超级 Alpha，则此条件为 True (不应用额外过滤)
                else_=True,
            ),
        ]

        # 应用筛选条件到查询
        # 使用 and_() 将所有条件组合起来
        final_query: Select = query.where(and_(*criteria))

        logger.debug("顾问因子筛选查询构建完成", emoji="✅", query=str(final_query))
        return final_query

    # 使用静态方法构建并赋值给类变量
    # 使用 ClassVar 注解，并确保类型为 Optional[Select]
    consultant_alpha_select_query: ClassVar[Optional[Select]] = (
        _build_consultant_alpha_select_query.__func__()  # type: ignore
    )

    @classmethod
    async def fetch_alphas_for_evaluation(
        cls,
        role: UserRole,
        alpha_type: AlphaType,
        start_time: datetime,
        end_time: datetime,
        region: Optional[Region] = None,
        delay: Optional[Delay] = None,
        universe: Optional[Universe] = None,
    ) -> AsyncGenerator[Alpha, None]:  # 修改返回类型为 AsyncGenerator
        """
        根据指定的条件筛选 Alpha，并以异步生成器的方式返回结果。

        此方法构建查询以根据用户角色、Alpha 类型、创建时间范围以及可选的区域、
        延迟和宇宙筛选 Alpha。它使用流式处理从数据库中检索 Alpha，以避免
        一次性将大量数据加载到内存中。

        Args:
            role (UserRole): 请求评估的用户角色 (目前仅支持顾问)。
            alpha_type (AlphaType): 要筛选的 Alpha 类型。
            start_time (datetime): 筛选 Alpha 的起始创建时间。
            end_time (datetime): 筛选 Alpha 的结束创建时间。
            region (Optional[Region]): 可选的区域筛选条件。
            delay (Optional[Delay]): 可选的延迟筛选条件。
            universe (Optional[Universe]): 可选的宇宙筛选条件。

        Yields:
            Alpha: 满足筛选条件的 Alpha 对象。

        Raises:
            NotImplementedError: 如果角色是 UserRole.USER (尚未实现)。
            ValueError: 如果顾问因子筛选查询未初始化。
            TypeError: 如果顾问因子筛选查询类型错误。
        """
        await logger.adebug(
            "开始准备获取待评估的 Alpha (生成器)",
            emoji="🔍",
            role=role,
            type=alpha_type,
            start_time=start_time,
            end_time=end_time,
            region=region,
            delay=delay,
            universe=universe,
        )
        if role == UserRole.USER:
            # 用户角色的筛选逻辑尚未实现
            await logger.aerror(
                "用户因子筛选查询尚未实现",
                emoji="❌",
                role=role,
            )
            raise NotImplementedError("用户因子筛选查询尚未实现")

        # 检查顾问查询是否已正确初始化
        if cls.consultant_alpha_select_query is None:
            await logger.aerror(
                "顾问因子筛选查询未初始化",
                emoji="❌",
                role=role,
            )
            raise ValueError("顾问因子筛选查询未初始化")
        elif not isinstance(cls.consultant_alpha_select_query, Select):
            await logger.aerror(
                "顾问因子筛选查询类型错误",
                emoji="❌",
                role=role,
                query_type=type(cls.consultant_alpha_select_query),
            )
            raise TypeError("顾问因子筛选查询类型错误")

        # 基于基础顾问查询构建最终查询
        query: Select = cls.consultant_alpha_select_query.where(
            and_(
                Alpha.type == alpha_type,
                Alpha.date_created >= start_time,
                Alpha.date_created <= end_time,
            )
        )

        # 应用可选的筛选条件
        # 注意：这里假设 Setting 是通过 Alpha 的 relationship 访问的，
        # SQLAlchemy 会自动处理 JOIN。如果性能有问题，可能需要显式 JOIN。
        if region:
            query = query.where(Alpha.settings.any(Setting.region == region))
        if delay:
            query = query.where(Alpha.settings.any(Setting.delay == delay))
        if universe:
            query = query.where(Alpha.settings.any(Setting.universe == universe))

        await logger.adebug(
            "顾问因子筛选查询构建完成，准备执行流式查询",
            emoji="⚙️",
            query=str(query),
        )

        # 执行流式查询并逐个返回结果
        async with get_db_session(Database.EVALUATE) as session:
            # 使用 stream_scalars 进行流式查询
            stream_result = await session.stream_scalars(query)
            alpha_count: int = 0
            async for alpha in stream_result:
                alpha_count += 1
                await logger.adebug(
                    "产出一个符合条件的 Alpha",
                    emoji="✨",
                    alpha_id=alpha.alpha_id,
                    current_count=alpha_count,
                )
                yield alpha  # 使用 yield 返回 Alpha 对象

            await logger.ainfo(
                "所有符合条件的 Alpha 已通过生成器产出",
                emoji="✅",
                role=role,
                type=alpha_type,
                start_time=start_time,
                end_time=end_time,
                region=region,
                delay=delay,
                universe=universe,
                total_alphas_yielded=alpha_count,
            )

    def __init__(self, alpha: Alpha) -> None:
        """
        初始化 BaseEvaluator 类。

        Args:
            alpha (Alpha): 需要进行评估的 Alpha 实体对象。
        """
        self._alpha: Alpha = alpha
        # __init__ 是同步方法，使用同步日志接口
        logger.info("BaseEvaluator 初始化完成", emoji="🚀", alpha_id=alpha.alpha_id)

    async def matched_competitions(self) -> List[CompetitionRefView]:
        """
        从 Alpha 的样本内检查结果中获取其匹配的竞赛列表。

        遍历 Alpha 的 `in_sample.checks` 属性，查找名称为 `MATCHES_COMPETITION` 的检查项，
        并解析其 `competitions` 字段 (JSON 字符串) 以获取竞赛参考视图列表。

        Args:
            无

        Returns:
            List[CompetitionRefView]: 与该 Alpha 匹配的竞赛参考视图 (CompetitionRefView) 列表。
                                      如果找不到匹配的竞赛检查项或 `competitions` 字段为空，则返回空列表。

        Raises:
            ValueError: 如果找到了 `MATCHES_COMPETITION` 检查项，但其 `competitions` 字段为空或无法解析。
            pydantic.ValidationError: 如果 `competitions` 字段的 JSON 数据不符合 `List[CompetitionRefView]` 的结构。
        """
        await logger.adebug(
            "开始获取 Alpha 匹配的竞赛列表",
            emoji="🔍",
            alpha_id=self._alpha.alpha_id,
        )
        # 创建 TypeAdapter 实例，用于验证和解析 JSON 数据到 CompetitionRefView 列表
        competitions_adapter: TypeAdapter[List[CompetitionRefView]] = TypeAdapter(
            List[CompetitionRefView]
        )

        # 确保 in_sample 存在且已加载 (如果使用延迟加载)
        # 注意：如果 in_sample 可能为 None，需要先检查
        if not self._alpha.in_sample:
            await logger.awarning(
                "Alpha 缺少样本内 (in_sample) 数据，无法获取匹配竞赛",
                emoji="⚠️",
                alpha_id=self._alpha.alpha_id,
            )
            return []

        # 遍历 Alpha 的样本内 (in_sample) 检查项
        for check in self._alpha.in_sample.checks:
            # 检查项名称是否为匹配竞赛
            if check.name == CheckType.MATCHES_COMPETITION.value:
                # 检查项中是否有竞赛信息
                if check.competitions:
                    try:
                        # 使用 TypeAdapter 验证并解析 JSON 字符串
                        competitions: List[CompetitionRefView] = (
                            competitions_adapter.validate_json(check.competitions)
                        )
                        await logger.adebug(
                            "成功解析匹配的竞赛列表",
                            emoji="✅",
                            alpha_id=self._alpha.alpha_id,
                            competitions_count=len(competitions),
                            # competitions=competitions # 如果列表不长，可以考虑打印
                        )
                        return competitions
                    except Exception as e:
                        # 如果解析失败，记录错误并抛出 ValueError
                        await logger.aerror(
                            "解析竞赛列表 JSON 时出错",
                            emoji="❌",
                            alpha_id=self._alpha.alpha_id,
                            check_name=check.name,
                            competitions_json=check.competitions,
                            error=str(e),
                            exc_info=True,  # 记录异常堆栈
                        )
                        raise ValueError(
                            f"Alpha (ID: {self._alpha.alpha_id}) 的 "
                            f"{check.name} 检查项中的竞赛列表 JSON 无效: {e}"
                        ) from e
                else:
                    # 如果有匹配竞赛的检查项但无竞赛数据，记录警告并抛出 ValueError
                    await logger.awarning(
                        "匹配竞赛检查项存在，但竞赛列表为空",
                        emoji="⚠️",
                        alpha_id=self._alpha.alpha_id,
                        check_name=check.name,
                    )
                    # 根据需求决定是否抛出异常，或者仅记录警告并返回空列表
                    # raise ValueError(
                    #     f"Alpha (ID: {self._alpha.alpha_id}) 的 "
                    #     f"{check.name} 检查项存在，但没有对应的竞赛项数据。"
                    # )
                    return []  # 返回空列表可能更健壮

        # 如果遍历完所有检查项都没有找到匹配的竞赛项，返回空列表
        await logger.adebug(
            "未找到匹配的竞赛检查项",
            emoji="🤷",
            alpha_id=self._alpha.alpha_id,
        )
        return []

    # ... (correlation_check 和其他方法保持不变，确保日志和异步调用符合规范) ...

    async def correlation_check(self, corr_type: CorrelationType) -> None:
        """
        检查数据的相关性。

        此方法会循环调用 WorldQuant API 进行相关性检查，直到检查完成或发生错误。
        它会处理 API 可能返回的重试逻辑。

        Args:
            corr_type (CorrelationType): 相关性类型，枚举值包括 SELF（自相关性）和 PROD（生产相关性）。
        """
        await logger.ainfo(
            "启动数据相关性检查循环",
            emoji="🔄",
            alpha_id=self._alpha.alpha_id,
            corr_type=corr_type,
        )
        # 注意：wq_client 应该在外部管理其生命周期或确保每次调用都能正确获取
        # 这里的 async with 可能每次都会创建和关闭连接，取决于 wq_client 的实现
        async with wq_client as client:
            while True:
                try:
                    await logger.adebug(
                        "执行单次相关性检查 API 调用",
                        emoji="📞",
                        alpha_id=self._alpha.alpha_id,
                        corr_type=corr_type,
                    )
                    finished: bool
                    retry_after: Optional[float]
                    result: Optional[TableView]
                    finished, retry_after, result = (
                        await self._perform_correlation_check(client, corr_type)
                    )
                    await logger.adebug(
                        "相关性检查 API 调用返回",
                        emoji="📥",
                        alpha_id=self._alpha.alpha_id,
                        corr_type=corr_type,
                        finished=finished,
                        retry_after=retry_after,
                        # result=result # 可能包含大量数据，谨慎打印
                    )

                    if finished:
                        # 检查完成，处理结果并退出循环
                        await self._handle_correlation_finished_check(result, corr_type)
                        break

                    if retry_after and retry_after > 0:
                        # 检查未完成，按建议时间等待后重试
                        await self._handle_correlation_unfinished_check(
                            retry_after, corr_type
                        )
                    else:
                        # API 返回既未完成也无重试时间，视为异常情况
                        await logger.awarning(
                            "数据相关性检查 API 返回异常状态：未完成且无重试时间",
                            emoji="❓",
                            alpha_id=self._alpha.alpha_id,
                            corr_type=corr_type,
                            finished=finished,
                            retry_after=retry_after,
                        )
                        break
                except asyncio.CancelledError:
                    # 捕获任务取消异常
                    await logger.awarning(
                        "数据相关性检查任务被取消",
                        emoji="🛑",
                        alpha_id=self._alpha.alpha_id,
                        corr_type=corr_type,
                    )
                    raise
                except Exception as e:
                    # 捕获其他所有异常
                    await logger.aerror(
                        "数据相关性检查过程中发生未预期异常",
                        emoji="💥",
                        alpha_id=self._alpha.alpha_id,
                        corr_type=corr_type,
                        error=str(e),
                        exc_info=True,  # 记录完整的异常堆栈信息
                    )
                    break
        await logger.ainfo(
            "数据相关性检查循环结束",
            emoji="🏁",
            alpha_id=self._alpha.alpha_id,
            corr_type=corr_type,
        )

    async def _perform_correlation_check(
        self, client: WorldQuantClient, corr_type: CorrelationType
    ) -> Tuple[bool, Optional[float], Optional[TableView]]:
        """
        执行单次相关性检查 API 调用。

        Args:
            client (WorldQuantClient): WorldQuant 客户端实例。
            corr_type (CorrelationType): 相关性类型。

        Returns:
            Tuple[bool, Optional[float], Optional[TableView]]:
                - finished (bool): 检查是否完成。
                - retry_after (Optional[float]): 建议的重试等待时间（秒），如果未完成。
                - result (Optional[TableView]): 检查结果对象，如果已完成。

        Raises:
            # 根据 client.alpha_correlation_check 可能抛出的异常添加说明
            Exception: 调用 WorldQuant API 时可能发生的网络或认证等错误。
        """
        await logger.adebug(
            "调用 client.alpha_correlation_check",
            emoji="📡",
            alpha_id=self._alpha.alpha_id,
            corr_type=corr_type,
        )
        # 假设 wq_client 已经正确处理了上下文管理和异步调用
        # 注意：原代码中这里又有一个 async with wq_client，可能导致嵌套或重复获取客户端
        # 这里假设传入的 client 是有效的，直接使用
        # async with wq_client as client: # 移除内部的 async with
        result_tuple: Tuple[bool, Optional[float], Optional[TableView]] = (
            await client.alpha_correlation_check(
                alpha_id=self._alpha.alpha_id,
                corr_type=corr_type,
            )
        )
        await logger.adebug(
            "client.alpha_correlation_check 调用完成",
            emoji="✅",
            alpha_id=self._alpha.alpha_id,
            corr_type=corr_type,
            # result_tuple=result_tuple # 可能包含敏感或大量数据
        )
        return result_tuple

    async def _handle_correlation_finished_check(
        self, result: Optional[TableView], corr_type: CorrelationType
    ) -> None:
        """
        处理相关性检查完成的情况。

        Args:
            result (Optional[AlphaCorrelationRecordView]): 检查结果对象。如果检查失败或无结果，可能为 None。
            corr_type (CorrelationType): 相关性类型。
        """
        if result:
            # 检查成功完成并返回了结果
            await logger.ainfo(
                "数据相关性检查成功完成",
                emoji="🎉",
                alpha_id=self._alpha.alpha_id,
                corr_type=corr_type,
                # result=result, # 结果对象可能很大，谨慎记录完整内容
                # 可以考虑记录关键指标，例如：
                # correlation_count=len(result.correlations) if result.correlations else 0,
            )

            check_record: CheckRecord = CheckRecord(
                alpha_id=self._alpha.alpha_id,
                record_type=(
                    CheckRecordType.CORRELATION_SELF
                    if corr_type == CorrelationType.SELF
                    else CheckRecordType.CORRELATION_PROD
                ),
                content=result.model_dump(mode="python"),
            )

            async with get_db_session(Database.EVALUATE) as session:
                checks_dal: CheckRecordDAL = DALFactory.create_dal(
                    session=session, dal_class=CheckRecordDAL
                )
                correlation_dal: CorrelationDAL = DALFactory.create_dal(
                    session=session, dal_class=CorrelationDAL
                )

                await checks_dal.create(check_record)

                # 生产相关性返回的结果只有相关系数的因子数量分布，没有具体的相关性值
                if corr_type == CorrelationType.SELF and result and result.records:
                    corr_index: int = result.table_schema.index_of("correlation")
                    alpha_id_index: int = result.table_schema.index_of("id")

                    if corr_index == -1 or alpha_id_index == -1:
                        await logger.aerror(
                            "相关性检查结果中缺少必要的字段",
                            emoji="❌",
                            alpha_id=self._alpha.alpha_id,
                            corr_type=corr_type,
                        )
                        return

                    correlations: List[Correlation] = []
                    # FIXME: 这里应该有报错
                    for record in result.records:
                        alpha_id: str = record[alpha_id_index]
                        corr_value: float = record[corr_index]
                        correlation: Correlation = Correlation(
                            alpha_id_a=self._alpha.alpha_id,
                            alpha_id_b=alpha_id,
                            correlation=corr_value,
                            calc_type=CorrelationCalcType.PLATFORM,
                        )
                        correlations.append(correlation)

                    await correlation_dal.bulk_upsert(correlations)
        else:
            # 检查声称已完成，但没有返回有效结果，视为失败或异常情况
            await logger.awarning(
                "数据相关性检查声称完成，但未返回有效结果",
                emoji="❓",
                alpha_id=self._alpha.alpha_id,
                corr_type=corr_type,
            )
            # TODO: 根据业务逻辑决定是否需要错误处理或重试

    async def _handle_correlation_unfinished_check(
        self, retry_after: float, corr_type: CorrelationType
    ) -> None:
        """
        处理相关性检查未完成，需要等待重试的情况。

        Args:
            retry_after (float): 建议的重试等待时间（秒）。
            corr_type (CorrelationType): 相关性类型。

        Raises:
            asyncio.CancelledError: 如果在等待期间任务被取消。
        """
        await logger.ainfo(
            "数据相关性检查未完成，将在指定时间后重试",
            emoji="⏳",
            alpha_id=self._alpha.alpha_id,
            corr_type=corr_type,
            retry_after=round(retry_after, 2),  # 保留两位小数，提高可读性
        )
        try:
            # 等待建议的秒数
            await asyncio.sleep(retry_after)
            await logger.adebug(
                "等待重试时间结束，准备进行下一次检查",
                emoji="⏯️",
                alpha_id=self._alpha.alpha_id,
                corr_type=corr_type,
            )
        except asyncio.CancelledError:
            # 如果在 sleep 期间任务被取消，记录警告并重新抛出
            await logger.awarning(
                "等待相关性检查重试时任务被取消",
                emoji="🛑",
                alpha_id=self._alpha.alpha_id,
                corr_type=corr_type,
            )
            raise  # 必须重新抛出 CancelledError

    async def self_correlation_check(self) -> None:
        """
        执行数据的自相关性检查。

        这是一个便捷方法，内部调用 `correlation_check` 并指定类型为 `SELF`。
        """
        await logger.ainfo(
            "开始执行自相关性检查",
            emoji="🔍",
            alpha_id=self._alpha.alpha_id,
        )
        await self.correlation_check(CorrelationType.SELF)
        await logger.ainfo(
            "自相关性检查流程结束",
            emoji="🏁",
            alpha_id=self._alpha.alpha_id,
        )

    async def prod_correlation_check(self) -> None:
        """
        执行数据的生产相关性检查。

        这是一个便捷方法，内部调用 `correlation_check` 并指定类型为 `PROD`。
        """
        await logger.ainfo(
            "开始执行生产相关性检查",
            emoji="🔍",
            alpha_id=self._alpha.alpha_id,
        )
        await self.correlation_check(CorrelationType.PROD)
        await logger.ainfo(
            "生产相关性检查流程结束",
            emoji="🏁",
            alpha_id=self._alpha.alpha_id,
        )

    async def before_and_after_performance_check(self, competition_id: str) -> None:
        """
        获取并记录 Alpha 在指定竞赛前后的性能表现。

        Args:
            competition_id (str): 竞赛的唯一标识符。
        """
        await logger.ainfo(
            "开始获取数据前后性能表现",
            emoji="📊",
            alpha_id=self._alpha.alpha_id,
            competition_id=competition_id,
        )

        if competition_id is None:
            await logger.aerror(
                "竞赛 ID 不能为空",
                emoji="❌",
                alpha_id=self._alpha.alpha_id,
            )
            return

        try:
            async with wq_client as client:
                # 调用 WorldQuant API 获取性能数据
                finished: bool = False
                retry_after: Optional[float] = None
                result: Optional[BeforeAndAfterPerformanceView] = None
                while True:
                    finished, retry_after, result, _ = (
                        await client.alpha_fetch_before_and_after_performance(
                            alpha_id=self._alpha.alpha_id,
                            competition_id=competition_id,
                        )
                    )

                    if finished:
                        if isinstance(result, BeforeAndAfterPerformanceView):
                            await logger.adebug(
                                "获取到前后性能表现数据",
                                emoji="✅",
                                alpha_id=self._alpha.alpha_id,
                                competition_id=competition_id,
                                score=result.score,
                                stats=result.stats,
                                yearly_stats=result.yearly_stats,
                                partition=result.partition,
                                competition=result.competition,
                            )

                            check_record: CheckRecord = CheckRecord(
                                alpha_id=self._alpha.alpha_id,
                                record_type=CheckRecordType.BEFORE_AND_AFTER_PERFORMANCE,
                                content=result.model_dump(mode="python"),
                            )

                            async with get_db_session(Database.EVALUATE) as session:
                                checks_dal: CheckRecordDAL = DALFactory.create_dal(
                                    session=session, dal_class=CheckRecordDAL
                                )
                                await checks_dal.create(check_record)

                            await logger.ainfo(
                                "数据前后性能表现获取成功",
                                emoji="🎉",
                                alpha_id=self._alpha.alpha_id,
                                competition_id=competition_id,
                            )

                        else:
                            await logger.aerror(
                                "获取前后性能表现数据失败，返回结果无效",
                                emoji="❌",
                                alpha_id=self._alpha.alpha_id,
                                competition_id=competition_id,
                            )
                    elif retry_after and retry_after > 0.0:
                        await logger.adebug(
                            "数据前后性能表现未完成，将在指定时间后重试",
                            emoji="⏳",
                            alpha_id=self._alpha.alpha_id,
                            competition_id=competition_id,
                            retry_after=round(retry_after, 2),
                        )
                        await asyncio.sleep(retry_after)

        except asyncio.CancelledError:
            await logger.awarning(
                "获取数据前后性能表现任务被取消",
                emoji="🛑",
                alpha_id=self._alpha.alpha_id,
                competition_id=competition_id,
            )
            raise
        except Exception as e:
            # 捕获 API 调用或其他处理中可能发生的异常
            await logger.aerror(
                "获取数据前后性能表现时发生异常",
                emoji="💥",
                alpha_id=self._alpha.alpha_id,
                competition_id=competition_id,
                error=str(e),
                exc_info=True,  # 记录异常堆栈
            )
        await logger.ainfo(
            "获取数据前后性能表现流程结束",
            emoji="🏁",
            alpha_id=self._alpha.alpha_id,
            competition_id=competition_id,
        )
