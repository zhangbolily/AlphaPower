"""Alpha 数据获取器 (Fetcher) 的抽象基类定义。

此模块定义了 `AbstractAlphaFetcher` 抽象基类 (Abstract Base Class, ABC)，
负责定义如何筛选和从数据源异步获取 Alpha 实体。

子类必须实现此基类中定义的所有抽象方法，以提供具体的业务逻辑。
"""

from __future__ import annotations  # 解决类型前向引用问题

import abc
from typing import Any, AsyncGenerator, Dict

# SQLAlchemy 列元素，用于构建数据库查询条件
from sqlalchemy import Select

from alphapower.dal.alphas import AlphaDAL, SampleDAL, SettingDAL
from alphapower.entity import Alpha


class AbstractAlphaFetcher(abc.ABC):
    """Alpha 数据获取器的抽象基类 (基于实例)。

    定义了筛选和异步获取 Alpha 数据的标准接口。
    通过构造函数注入所需的 DAL (Data Access Layer, 数据访问层) 对象。
    子类需要实现具体的筛选条件生成逻辑和数据获取逻辑。
    """

    def __init__(
        self,
        alpha_dal: AlphaDAL,
        sample_dal: SampleDAL,
        setting_dal: SettingDAL,
    ):
        """初始化 AlphaFetcher。

        Args:
            alpha_dal: Alpha 数据访问层对象。
            sample_dal: Sample 数据访问层对象。
            setting_dal: Setting 数据访问层对象。
        """
        self.alpha_dal = alpha_dal
        self.sample_dal = sample_dal
        self.setting_dal = setting_dal

    @abc.abstractmethod
    async def _build_alpha_select_query(
        self,
        **kwargs: Dict[str, Any],
    ) -> Select:
        """构建用于筛选 Alpha 的 SQLAlchemy 查询对象 (Select Object)。

        此方法负责异步整合所有筛选条件（通常由内部辅助方法生成），
        并构建一个完整的 SQLAlchemy `Select` 语句。该语句应包含所有必要的
        连接 (JOINs) 和过滤 (WHERE clauses) 操作。

        Args:
            **kwargs: 传递给筛选条件生成逻辑的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            一个配置好的 SQLAlchemy 查询对象 (`Select`)，用于后续执行以获取 Alpha 数据或计数。
        """
        raise NotImplementedError("子类必须实现 _build_alpha_select_query 方法")

    @abc.abstractmethod
    async def fetch_alphas(
        self,
        **kwargs: Dict[str, Any],
    ) -> AsyncGenerator[Alpha, None]:
        """异步获取符合筛选条件的 Alpha 实体。

        此方法通常会调用 `self._build_alpha_select_query` 来获取查询语句，
        然后使用注入的 `self.alpha_dal` (或其他 DAL) 执行该查询，并异步地产生 (yield)
        查询结果中的 `Alpha` 对象。

        Args:
            **kwargs: 传递给 `self._build_alpha_select_query` 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Yields:
            逐个返回符合筛选条件的 `Alpha` 实体对象。
        """
        # 确保 AsyncGenerator 被正确注解
        if False:  # pylint: disable=W0125
            yield  # pragma: no cover
        raise NotImplementedError("子类必须实现 fetch_alphas 方法")

    @abc.abstractmethod
    async def total_alpha_count(
        self,
        **kwargs: Dict[str, Any],
    ) -> int:
        """获取符合筛选条件的 Alpha 总数量。

        此方法通常会调用 `self._build_alpha_select_query` 获取基础查询，
        然后使用注入的 `self.alpha_dal` 构建并执行一个计数查询 (COUNT query)，
        以确定满足所有筛选条件的 Alpha 总数。

        Args:
            **kwargs: 传递给 `self._build_alpha_select_query` 的参数字典。
                      子类实现应明确文档说明其支持的 `kwargs` 参数。

        Returns:
            符合筛选条件的 Alpha 实体总数。
        """
        raise NotImplementedError("子类必须实现 total_alpha_count 方法")

    @abc.abstractmethod
    async def fetched_alpha_count(
        self,
        **kwargs: Dict[str, Any],
    ) -> int:
        """获取当前已通过 `fetch_alphas` 获取的 Alpha 数量。

        子类需要实现此方法来追踪 `fetch_alphas` 生成器已经产生了多少个 Alpha 对象。
        这对于进度报告或状态监控可能很有用。

        Args:
            **kwargs: 可能影响计数的上下文参数（如果适用）。子类应明确说明。

        Returns:
            到目前为止已获取的 Alpha 对象数量。
        """
        raise NotImplementedError("子类必须实现 fetched_alpha_count 方法")

    @abc.abstractmethod
    async def remaining_alpha_count(
        self,
        **kwargs: Dict[str, Any],
    ) -> int:
        """获取尚未通过 `fetch_alphas` 获取的剩余 Alpha 数量。

        此方法通常可以通过 `total_alpha_count()` 和 `fetched_alpha_count()` 的差值计算得出。
        子类需要实现具体的计算逻辑。

        Args:
            **kwargs: 可能影响计数的上下文参数（如果适用）。子类应明确说明。

        Returns:
            尚未获取的符合条件的 Alpha 对象数量。
        """
        raise NotImplementedError("子类必须实现 remaining_alpha_count 方法")
