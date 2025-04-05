"""
基础数据访问层 (DAL) 模块
提供通用的 CRUD 操作，支持异步数据库交互。
"""

from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union, cast

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Delete, Select, Update

# 定义泛型类型变量，用于表示实体类型
T = TypeVar("T")


class BaseDAL(Generic[T]):
    """
    基础数据访问层类，提供通用的 CRUD 操作。

    泛型参数 T 表示特定的实体类型，使得该类可以处理不同类型的实体。

    Attributes:
        entity_type: 实体类的类型，用于构建查询语句。
        session: SQLAlchemy 异步会话对象，用于与数据库交互。
    """

    def __init__(self, entity_type: Type[T], session: AsyncSession) -> None:
        """
        初始化 BaseDAL 实例。

        Args:
            entity_type: 实体类的类型。
            session: SQLAlchemy 异步会话对象。
        """
        self.entity_type: Type[T] = entity_type
        self.session: AsyncSession = session

    @classmethod
    def create(
        cls,
        entity_type: Optional[Union[Type[T], AsyncSession]] = None,
        session: Optional[AsyncSession] = None,
    ) -> "BaseDAL[T]":
        """
        创建 DAL 实例的工厂方法。

        支持两种调用方式:
        1. create(entity_type, session) - 标准方式
        2. create(session) - 当子类已明确指定了实体类型时

        Args:
            entity_type: 实体类的类型或会话对象。如果是会话对象，则后面的session参数应为None。
            session: SQLAlchemy 异步会话对象。

        Returns:
            新的 DAL 实例。

        Raises:
            ValueError: 当参数不足或会话对象缺失时。
        """
        # 处理第一个参数可能是会话对象的情况
        if session is None and isinstance(entity_type, AsyncSession):
            session = entity_type
            entity_type = None

        # 检查会话对象
        if not isinstance(session, AsyncSession):
            raise ValueError("Session is required and must be an AsyncSession instance")

        # 处理子类情况 - 特定类型的DAL子类
        if entity_type is None and cls != BaseDAL:
            # 获取子类对应的实体类型 (在子类构造函数中已定义)
            # 这里只需要传入 session 参数
            return cls(session)

        # 处理基类情况 - 通用 BaseDAL
        if entity_type is None or not isinstance(entity_type, type):
            raise ValueError("Entity type is required for BaseDAL")

        # 基类使用需要同时提供实体类型和会话
        return cls(entity_type, session)

    async def create_entity(self, **kwargs: Any) -> T:
        """
        创建一个新的实体记录。

        Args:
            **kwargs: 实体属性键值对。

        Returns:
            新创建的实体对象。
        """
        entity: T = self.entity_type(**kwargs)
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def create_entity_obj(self, entity: T) -> T:
        """
        创建单个实体对象。

        Args:
            entity: 实体对象。

        Returns:
            新创建的实体对象。
        """
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def create_entities_obj(self, entities: List[T]) -> List[T]:
        """
        批量创建实体对象。

        Args:
            entities: 实体对象列表。

        Returns:
            新创建的实体对象列表。
        """
        self.session.add_all(entities)
        await self.session.flush()
        return entities

    async def get_by_id(
        self, entity_id: int, session: Optional[AsyncSession] = None
    ) -> Optional[T]:
        """
        通过 ID 获取单个实体。

        Args:
            entity_id: 实体的 ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的实体对象，如果未找到则返回 None。
        """
        actual_session: AsyncSession = session or self.session
        return await actual_session.get(self.entity_type, entity_id)

    async def get_all(self, session: Optional[AsyncSession] = None) -> List[T]:
        """
        获取所有实体。

        Args:
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            所有实体的列表。
        """
        actual_session: AsyncSession = session or self.session
        result = await actual_session.execute(select(self.entity_type))
        return list(result.scalars().all())

    async def find_by(
        self, session: Optional[AsyncSession] = None, **kwargs: Any
    ) -> List[T]:
        """
        按条件查找实体。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 查询条件的键值对。

        Returns:
            符合条件的实体列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_one_by(
        self, session: Optional[AsyncSession] = None, **kwargs: Any
    ) -> Optional[T]:
        """
        按条件查找单个实体。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 查询条件的键值对。

        Returns:
            符合条件的第一个实体，如果未找到则返回 None。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query.limit(1))
        return result.scalars().first()

    async def update(
        self, entity_id: int, session: Optional[AsyncSession] = None, **kwargs: Any
    ) -> Optional[T]:
        """
        更新实体。

        Args:
            entity_id: 要更新的实体 ID。
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 需要更新的属性键值对。

        Returns:
            更新后的实体对象，如果未找到则返回 None。
        """
        actual_session: AsyncSession = session or self.session
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            for key, value in kwargs.items():
                setattr(entity, key, value)
            await actual_session.flush()
        return entity

    async def update_entity_obj(self, entity: T) -> T:
        """
        更新单个实体对象。

        Args:
            entity: 实体对象。

        Returns:
            更新后的实体对象。
        """
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def update_entities_obj(self, entities: List[T]) -> List[T]:
        """
        批量更新实体对象。

        Args:
            entities: 实体对象列表。

        Returns:
            更新后的实体对象列表。
        """
        self.session.add_all(entities)
        await self.session.flush()
        return entities

    async def update_by_query(
        self,
        filter_kwargs: Dict[str, Any],
        update_kwargs: Dict[str, Any],
        session: Optional[AsyncSession] = None,
    ) -> int:
        """
        通过查询条件批量更新实体。

        Args:
            filter_kwargs: 过滤条件的键值对。
            update_kwargs: 需要更新的属性键值对。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            更新的记录数量。
        """
        actual_session: AsyncSession = session or self.session
        query: Update = update(self.entity_type)
        for key, value in filter_kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        query = query.values(**update_kwargs)
        result = await actual_session.execute(query)
        return result.rowcount

    async def delete(
        self, entity_id: int, session: Optional[AsyncSession] = None
    ) -> bool:
        """
        删除指定 ID 的实体。

        Args:
            entity_id: 要删除的实体 ID。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            如果成功删除返回 True，否则返回 False。
        """
        actual_session: AsyncSession = session or self.session
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            await actual_session.delete(entity)
            await actual_session.flush()
            return True
        return False

    async def delete_entity_obj(self, entity: T) -> bool:
        """
        删除单个实体对象。

        Args:
            entity: 实体对象。

        Returns:
            如果成功删除返回 True。
        """
        await self.session.delete(entity)
        await self.session.flush()
        return True

    async def delete_entities_obj(self, entities: List[T]) -> int:
        """
        批量删除实体对象。

        Args:
            entities: 实体对象列表。

        Returns:
            删除的记录数量。
        """
        for e in entities:
            await self.session.delete(e)
        await self.session.flush()
        return len(entities)

    async def delete_by(
        self, session: Optional[AsyncSession] = None, **kwargs: Any
    ) -> int:
        """
        按条件删除实体。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 删除条件的键值对。

        Returns:
            删除的记录数量。
        """
        actual_session: AsyncSession = session or self.session
        query: Delete = delete(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query)
        return result.rowcount

    async def count(self, session: Optional[AsyncSession] = None, **kwargs: Any) -> int:
        """
        按条件统计实体数量。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 统计条件的键值对。

        Returns:
            符合条件的实体数量。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(select(query.exists()))
        return cast(int, result.scalar())

    def query(self) -> Select:
        """
        获取基础查询对象，用于构建复杂查询。

        Returns:
            SQLAlchemy Select 查询对象。
        """
        return select(self.entity_type)

    async def execute_query(
        self, query: Select, session: Optional[AsyncSession] = None
    ) -> List[T]:
        """
        执行自定义查询。

        Args:
            query: SQLAlchemy Select 查询对象。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            查询结果列表。
        """
        actual_session: AsyncSession = session or self.session
        result = await actual_session.execute(query)
        return list(result.scalars().all())


# 创建一个通用的DAL工厂方法基类
class DALFactory:
    """DAL 工厂类，提供创建各种 DAL 实例的标准方法。"""

    @staticmethod
    def create_dal(dal_class: Type[BaseDAL], session: AsyncSession) -> BaseDAL:
        """
        创建特定类型的 DAL 实例。

        Args:
            dal_class: DAL 类型。
            session: SQLAlchemy 异步会话对象。

        Returns:
            新创建的 DAL 实例。
        """
        return dal_class(session)


# 简化子类的 create 方法实现，使用统一模板
class EntityDAL(BaseDAL[T]):
    """特定实体 DAL 基类，为所有实体特定 DAL 提供统一的创建方法。"""

    @classmethod
    def create(
        cls,
        entity_type: Optional[Union[Type[T], AsyncSession]] = None,
        session: Optional[AsyncSession] = None,
    ) -> "EntityDAL":
        """
        创建实体 DAL 实例的统一工厂方法。

        Args:
            entity_type: 实体类型或会话对象。
            session: SQLAlchemy 异步会话对象。

        Returns:
            特定类型的 DAL 实例。
        """
        if session is None and isinstance(entity_type, AsyncSession):
            session = entity_type
            entity_type = None

        if not isinstance(session, AsyncSession):
            raise ValueError("Session is required and must be an AsyncSession instance")

        return cls(session)
