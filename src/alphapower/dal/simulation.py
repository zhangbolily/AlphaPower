"""
模拟任务数据访问层模块
提供对模拟任务及其状态的数据访问操作。
"""

from typing import Any, Dict, List, Optional, Type

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Select

from alphapower.dal.base import EntityDAL
from alphapower.entity.simulation import SimulationTask, SimulationTaskStatus


class SimulationTaskDAL(EntityDAL[SimulationTask]):
    """
    SimulationTask 数据访问层类，提供对 SimulationTask 实体的特定操作。

    管理模拟任务的CRUD操作，支持按状态、优先级等多种方式查询任务。
    """

    entity_class: Type[SimulationTask] = SimulationTask

    async def find_by_status(
        self, status: Any, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询特定状态的所有任务。

        Args:
            status: 任务状态值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定状态的所有任务列表。
        """
        return await self.deprecated_find_by(session=session, status=status)

    async def find_by_alpha_id(
        self, alpha_id: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询与特定 Alpha 关联的所有任务。

        Args:
            alpha_id: Alpha的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            与指定Alpha关联的所有任务列表。
        """
        return await self.deprecated_find_by(session=session, alpha_id=alpha_id)

    async def find_by_signature(
        self, signature: str, session: Optional[AsyncSession] = None
    ) -> Optional[SimulationTask]:
        """
        通过签名查询任务。

        任务签名通常是任务配置的哈希值，用于唯一标识特定配置的任务。

        Args:
            signature: 任务签名字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的任务实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, signature=signature)

    async def find_pending_tasks(
        self, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询所有待处理的任务。

        待处理任务是指已创建但尚未开始执行的任务。

        Args:
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            所有待处理任务的列表。
        """
        return await self.deprecated_find_by(
            session=session, status=SimulationTaskStatus.PENDING
        )

    async def find_running_tasks(
        self, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询所有正在运行的任务。

        正在运行的任务是指已开始执行但尚未完成的任务。

        Args:
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            所有正在运行任务的列表。
        """
        return await self.deprecated_find_by(
            session=session, status=SimulationTaskStatus.RUNNING
        )

    async def find_high_priority_tasks(
        self, min_priority: int, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询优先级高于指定值的所有任务。

        优先级越高的任务应该越先被处理，此方法用于任务调度优化。

        Args:
            min_priority: 最小优先级阈值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的任务列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(SimulationTask).where(
            SimulationTask.priority >= min_priority
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_settings_group(
        self, group_key: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询属于特定设置组的所有任务。

        设置组用于对相关的任务进行分组管理，便于批量操作。

        Args:
            group_key: 设置组的键名。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            属于指定设置组的所有任务列表。
        """
        return await self.deprecated_find_by(
            session=session, settings_group_key=group_key
        )

    async def find_tasks_by_date_range(
        self, start_date: str, end_date: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询在指定日期范围内创建的所有任务。

        此方法用于时间范围分析，支持按创建时间筛选任务。

        Args:
            start_date: 开始日期字符串，格式 'YYYY-MM-DD'。
            end_date: 结束日期字符串，格式 'YYYY-MM-DD'。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的任务列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(SimulationTask).where(
            SimulationTask.created_at.between(start_date, end_date)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_filtered(
        self,
        status: Optional[SimulationTaskStatus] = None,
        priority: Optional[int] = None,
        not_in_: Optional[Dict[str, List[int]]] = None,
        in_: Optional[Dict[str, List[int]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        session: Optional[AsyncSession] = None,
    ) -> List[SimulationTask]:
        """
        根据多个条件过滤任务。
        支持按状态、优先级等条件进行组合查询。
        Args:
            status: 任务状态值。
            priority: 任务优先级。
            not_in_: 字典，键为字段名，值为不包含的值列表。
            in_: 字典，键为字段名，值为包含的值列表。
            limit: 返回结果的最大数量。
            offset: 查询结果的偏移量。
            session: 可选的会话对象，若提供则优先使用。
        Returns:
            符合条件的任务列表。
        Raises:
            ValueError: 如果没有提供任何过滤条件。
        """
        actual_session = self._actual_session(session)
        filters = []
        if status is not None:
            filters.append(SimulationTask.status == status)
        if priority is not None:
            filters.append(SimulationTask.priority == priority)
        if not_in_ is not None:
            for key, values in not_in_.items():
                if hasattr(SimulationTask, key):
                    column = getattr(SimulationTask, key)
                    filters.append(column.notin_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")
        if in_ is not None:
            for key, values in in_.items():
                if hasattr(SimulationTask, key):
                    column = getattr(SimulationTask, key)
                    filters.append(column.in_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")

        if not filters:
            raise ValueError("至少需要一个过滤条件")

        query = select(SimulationTask).filter(*filters)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_task_ids_by_filters(
        self,
        status: Optional[SimulationTaskStatus] = None,
        priority: Optional[int] = None,
        notin_: Optional[Dict[str, List[int]]] = None,
        in_: Optional[Dict[str, List[int]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        session: Optional[AsyncSession] = None,
        **kwargs: Any,
    ) -> List[int]:
        """
        根据多个条件过滤任务，返回任务ID列表。
        支持按状态、优先级等条件进行组合查询。
        Args:
            status: 任务状态值。
            priority: 任务优先级。
            not_in_: 字典，键为字段名，值为不包含的值列表。
            in_: 字典，键为字段名，值为包含的值列表。
            limit: 返回结果的最大数量。
            offset: 查询结果的偏移量。
            session: 可选的会话对象，若提供则优先使用。
        Returns:
            符合条件的任务ID列表。
        Raises:
            ValueError: 如果没有提供任何过滤条件。
        """
        actual_session = self._actual_session(session)
        filters = []
        if status is not None:
            filters.append(SimulationTask.status == status)
        if priority is not None:
            filters.append(SimulationTask.priority == priority)
        if notin_ is not None:
            for key, values in notin_.items():
                if hasattr(SimulationTask, key):
                    column = getattr(SimulationTask, key)
                    filters.append(column.notin_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")
        if in_ is not None:
            for key, values in in_.items():
                if hasattr(SimulationTask, key):
                    column = getattr(SimulationTask, key)
                    filters.append(column.in_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")

        for key, value in kwargs.items():
            if hasattr(SimulationTask, key):
                column = getattr(SimulationTask, key)
                filters.append(column == value)
            else:
                raise ValueError(f"无效的字段名: {key}")

        if not filters:
            raise ValueError("至少需要一个过滤条件")

        query = select(SimulationTask.id).filter(*filters)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        result = await actual_session.execute(query)
        return list(result.scalars().all())
