"""
基础数据访问层 (DAL) 模块
提供通用的 CRUD 操作，支持异步数据库交互。
"""

import traceback
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union, cast

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import MappedColumn
from sqlalchemy.sql.expression import ColumnExpressionArgument, Delete, Select, Update
from structlog.stdlib import BoundLogger
from typing_extensions import Protocol

from alphapower.internal.logging import setup_logging

# pylint: disable=E1102


class HasEntity(Protocol):
    """
    定义一个协议 (Protocol)，约束泛型类型 T 必须包含 id 属性。
    """

    id: MappedColumn[int]


# 修改泛型类型变量 T，使其必须满足 HasID 协议
T = TypeVar("T", bound=HasEntity)


class BaseDAL(Generic[T]):
    """
    基础数据访问层类，提供通用的 CRUD 操作。

    泛型参数 T 表示特定的实体类型，使得该类可以处理不同类型的实体。

    Attributes:
        entity_type: 实体类的类型，用于构建查询语句。
        session: SQLAlchemy 异步会话对象，用于与数据库交互。
        logger: structlog 日志记录器，用于记录 DAL 操作。
    """

    # 实体类型，子类可以重写此类属性
    entity_class: Type[T] = None  # type: ignore

    def __init__(self, entity_type: Type[T], session: AsyncSession) -> None:
        """
        初始化 BaseDAL 实例。

        Args:
            entity_type: 实体类的类型。
            session: SQLAlchemy 异步会话对象。
        """
        self.entity_type: Type[T] = entity_type
        self.session: AsyncSession = session

        # 使用 setup_logging 获取 structlog 的 logger
        self.logger: BoundLogger = setup_logging(
            f"alphapower.dal.{self.__class__.__name__}"
        )
        self.logger.info(
            "初始化DAL实例",
            entity_type=self.entity_type.__name__,
            emoji="✅",
        )

    @classmethod
    def create_dal(
        cls: Type["BaseDAL[T]"],
        entity_type: Optional[Type[T]] = None,
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
        # 使用 setup_logging 获取 structlog 的 logger
        logger = setup_logging(f"alphapower.dal.{cls.__name__}")
        logger.debug(
            "调用DAL工厂方法",
            dal_class=cls.__name__,
            entity_type=entity_type.__name__ if entity_type else None,
            emoji="🏭",
        )

        # 检查会话对象
        if not isinstance(session, AsyncSession):
            logger.critical(
                "会话对象缺失或类型错误",
                session_type=type(session).__name__,
                emoji="❌",
            )
            raise ValueError("会话对象必须提供且必须是AsyncSession实例")

        # 确定实体类型
        actual_entity_type = None
        if isinstance(entity_type, type):
            # 如果明确提供了实体类型，则使用它
            logger.debug(
                "使用提供的实体类型",
                entity_type=entity_type.__name__,
                emoji="✅",
            )
            actual_entity_type = entity_type
        elif cls.entity_class is not None:
            # 如果子类定义了实体类型，则使用它
            logger.debug(
                "使用子类定义的实体类型",
                entity_type=cls.entity_class.__name__,
                emoji="✅",
            )
            actual_entity_type = cls.entity_class
        elif cls != BaseDAL:
            # 对于子类，但没有指定实体类型的情况，报错提示
            logger.error(
                "未定义实体类型",
                dal_class=cls.__name__,
                emoji="❌",
            )
            raise ValueError(f"子类 {cls.__name__} 必须提供实体类型或定义entity_class")
        else:
            # 对于基类，必须提供实体类型
            logger.error(
                "基类需要提供实体类型",
                emoji="❌",
            )
            raise ValueError("BaseDAL需要提供实体类型")

        # 创建实例并返回
        logger.info(
            "创建DAL实例成功",
            dal_class=cls.__name__,
            entity_type=actual_entity_type.__name__,
            emoji="✅",
        )
        return cls(entity_type=actual_entity_type, session=session)

    async def create_entity(self, **kwargs: Any) -> T:
        """
        创建一个新的实体记录。

        Args:
            **kwargs: 实体属性键值对。

        Returns:
            新创建的实体对象。

        Raises:
            SQLAlchemyError: 当数据库操作失败时。
        """
        self.logger.debug(
            "创建实体",
            entity_type=self.entity_type.__name__,
            attributes=kwargs,
            emoji="📦",
        )
        try:
            entity: T = self.entity_type(**kwargs)
            self.session.add(entity)
            await self.session.flush()
            self.logger.info(
                "成功创建实体",
                entity_id=getattr(entity, "id", "unknown"),
                emoji="✅",
            )
            return entity
        except Exception as e:
            self.logger.error(
                "实体创建失败",
                entity_type=self.entity_type.__name__,
                attributes=kwargs,
                error=str(e),
                emoji="❌",
            )

            self.logger.error(
                "错误堆栈",
                traceback=traceback.format_exc(),
                emoji="🛠️",
            )
            raise

    async def create(self, entity: T) -> T:
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

    async def create_all(self, entities: List[T]) -> List[T]:
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

    async def upsert(self, entity: T) -> T:
        """
        插入或更新实体对象。

        Args:
            entity: 实体对象。

        Returns:
            插入或更新后的实体对象。
        """
        existing_entity = await self.get_by_id(entity.id)
        if existing_entity:
            await self.session.merge(entity)
            await self.session.flush()
            return existing_entity
        return await self.create(entity)

    async def upsert_by_unique_key(self, entity: T, unique_key: str) -> T:
        """
        根据唯一键插入或更新实体对象。

        Args:
            entity: 实体对象。
            unique_key: 唯一键的名称。

        Returns:
            插入或更新后的实体对象。
        """
        existing_entity = await self.find_one_by(
            **{unique_key: getattr(entity, unique_key)}
        )
        if existing_entity:
            entity.id = existing_entity.id
            await self.session.merge(entity)
            await self.session.flush()
            return existing_entity
        return await self.create(entity)

    async def bulk_upsert(self, entities: List[T]) -> List[T]:
        """
        批量插入或更新实体对象。
        Args:
            entities: 实体对象列表。
        Returns:
            插入或更新后的实体对象列表。
        """
        if not entities:
            return []

        ids: List[int] = [entity.id for entity in entities]
        existing_entities = await self.find_by(in_={"id": ids})
        existing_ids: List[int] = [entity.id for entity in existing_entities]
        new_entities: List[T] = [
            entity for entity in entities if entity.id not in existing_ids
        ]

        for entity in new_entities:
            self.session.add(entity)

        for entity in entities:
            if entity.id in existing_ids:
                await self.session.merge(entity)

        await self.session.flush()
        return entities

    async def bulk_upsert_by_unique_key(
        self, entities: List[T], unique_key: str
    ) -> List[T]:
        """
        批量插入或更新实体对象，根据唯一键。

        Args:
            entities: 实体对象列表。
            unique_key: 唯一键的名称。

        Returns:
            插入或更新后的实体对象列表。
        """
        if not entities:
            return []

        unique_values: List[Any] = [getattr(entity, unique_key) for entity in entities]
        existing_entities = await self.find_by(in_={unique_key: unique_values})
        existing_unique_values: List[Any] = [
            getattr(entity, unique_key) for entity in existing_entities
        ]
        new_entities: List[T] = [
            entity
            for entity in entities
            if getattr(entity, unique_key) not in existing_unique_values
        ]

        for entity in new_entities:
            self.session.add(entity)

        for entity in entities:
            if getattr(entity, unique_key) in existing_unique_values:
                entity.id = next(
                    (
                        existing_entity.id
                        for existing_entity in existing_entities
                        if getattr(existing_entity, unique_key)
                        == getattr(entity, unique_key)
                    ),
                )
                await self.session.merge(entity)

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
        self.logger.debug(
            "查询实体",
            entity_id=entity_id,
            emoji="🔍",
        )
        actual_session: AsyncSession = session or self.session
        entity = await actual_session.get(self.entity_type, entity_id)
        if entity:
            self.logger.info(
                "查询成功",
                entity_id=entity_id,
                emoji="✅",
            )
        else:
            self.logger.warning(
                "未找到实体",
                entity_id=entity_id,
                emoji="⚠️",
            )
        return entity

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
        self,
        session: Optional[AsyncSession] = None,
        in_: Optional[Dict[str, Any]] = None,
        notin_: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
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
        criteria: List[ColumnExpressionArgument] = []
        if in_:
            for key, values in in_.items():
                if hasattr(self.entity_type, key):
                    column = getattr(self.entity_type, key)
                    criteria.append(column.notin_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")
        if notin_:
            for key, values in notin_.items():
                if hasattr(self.entity_type, key):
                    column = getattr(self.entity_type, key)
                    criteria.append(column.in_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")
        for key, value in kwargs.items():
            if hasattr(self.entity_type, key):
                column = getattr(self.entity_type, key)
                criteria.append(column == value)
            else:
                raise ValueError(f"无效的字段名: {key}")
        if criteria:
            query = select(self.entity_type).filter(*criteria)
            result = await actual_session.execute(query)
            return list(result.scalars().all())
        else:
            raise ValueError("没有提供任何过滤条件")

    async def find_one_by(
        self,
        session: Optional[AsyncSession] = None,
        order_by: Optional[Union[str, ColumnExpressionArgument]] = None,
        **kwargs: Any,
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
        if order_by:
            query = query.order_by(order_by)
        result = await actual_session.execute(query.limit(1))
        return result.scalars().first()

    async def update_by_id(
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
        self.logger.debug(
            "更新实体",
            entity_id=entity_id,
            update_fields=kwargs,
            emoji="✏️",
        )
        actual_session: AsyncSession = session or self.session
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            for key, value in kwargs.items():
                setattr(entity, key, value)
            await actual_session.flush()
            self.logger.info(
                "更新成功",
                entity_id=entity_id,
                updated_fields=kwargs,
                emoji="✅",
            )
            return entity
        self.logger.warning(
            "更新失败，实体不存在",
            entity_id=entity_id,
            emoji="⚠️",
        )
        return None

    async def update(self, entity: T) -> T:
        """
        更新单个实体对象。

        Args:
            entity: 实体对象。

        Returns:
            更新后的实体对象。
        """

        await self.session.merge(entity)
        await self.session.flush()
        return entity

    async def update_all(self, entities: List[T]) -> List[T]:
        """
        批量更新实体对象。

        Args:
            entities: 实体对象列表。

        Returns:
            更新后的实体对象列表。
        """
        for entity in entities:
            await self.session.merge(entity)
        await self.session.flush()
        return entities

    async def update_by_filter(
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

    async def delete_by_id(
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
        self.logger.debug(
            "删除实体",
            entity_id=entity_id,
            emoji="🗑️",
        )
        actual_session: AsyncSession = session or self.session
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            await actual_session.delete(entity)
            await actual_session.flush()
            self.logger.info(
                "删除成功",
                entity_id=entity_id,
                emoji="✅",
            )
            return True
        self.logger.warning(
            "删除失败，实体不存在",
            entity_id=entity_id,
            emoji="⚠️",
        )
        return False

    async def delete(self, entity: T) -> bool:
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

    async def delete_all(self, entities: List[T]) -> int:
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

    async def delete_by_filter(
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
        self.logger.debug(
            "按条件删除实体",
            filter_conditions=kwargs,
            emoji="🗑️",
        )
        actual_session: AsyncSession = session or self.session
        query: Delete = delete(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query)
        deleted_count = result.rowcount
        if deleted_count > 0:
            self.logger.info(
                "删除成功",
                deleted_count=deleted_count,
                filter_conditions=kwargs,
                emoji="✅",
            )
        else:
            self.logger.warning(
                "未删除任何实体",
                filter_conditions=kwargs,
                emoji="⚠️",
            )
        return deleted_count

    async def count(
        self,
        session: Optional[AsyncSession] = None,
        in_: Optional[Dict[str, Any]] = None,
        notin_: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> int:
        """
        按条件统计实体数量。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 统计条件的键值对。

        Returns:
            符合条件的实体数量。
        """
        self.logger.debug(
            "统计实体数量",
            filter_conditions=kwargs,
            in_conditions=in_,
            notin_conditions=notin_,
            emoji="📊",
        )
        actual_session: AsyncSession = session or self.session
        query: Select = select(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        if in_:
            for key, value in in_.items():
                query = query.where(getattr(self.entity_type, key).in_(value))
        if notin_:
            for key, value in notin_.items():
                query = query.where(getattr(self.entity_type, key).notin_(value))

        count_query = select(func.count()).select_from(query.subquery())
        result = await actual_session.execute(count_query)
        count = cast(int, result.scalar())
        self.logger.info(
            "统计完成",
            count=count,
            filter_conditions=kwargs,
            emoji="✅",
        )
        return count

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


# 为 DALFactory 创建的泛型类型变量
D = TypeVar("D", bound=BaseDAL)


# 创建一个通用的DAL工厂方法基类
class DALFactory:
    """DAL 工厂类，提供创建各种 DAL 实例的标准方法。"""

    @staticmethod
    def create_dal(dal_class: Type[D], session: AsyncSession) -> D:
        """
        创建特定类型的 DAL 实例。

        Args:
            dal_class: DAL 类型。
            session: SQLAlchemy 异步会话对象。

        Returns:
            新创建的 DAL 实例，类型与传入的 dal_class 一致。
        """
        logger = setup_logging(f"alphapower.dal.{dal_class.__name__}")
        logger.debug(
            "创建 DAL 实例",
            dal_class=dal_class.__name__,
            session_type=type(session).__name__,
            emoji="🏭",
        )

        if not isinstance(session, AsyncSession):
            logger.critical(
                "会话对象缺失或类型错误",
                session_type=type(session).__name__,
                emoji="❌",
            )
            raise ValueError("会话对象必须提供且必须是AsyncSession实例")

        dal_instance = cast(D, dal_class.create_dal(session=session))
        logger.info(
            "DAL 实例创建成功",
            dal_class=dal_class.__name__,
            emoji="✅",
        )
        return dal_instance


# 简化子类的 create 方法实现，使用统一模板
class EntityDAL(BaseDAL[T]):
    """特定实体 DAL 基类，为所有实体特定 DAL 提供统一的创建方法。"""

    # 实体类型，子类需要重写此类属性
    entity_class: Type[T] = None  # type: ignore

    def __init__(self, session: AsyncSession) -> None:
        """
        初始化 EntityDAL 实例。

        Args:
            session: SQLAlchemy 异步会话对象。
        """
        super().__init__(self.entity_class, session)
        self.logger.info(
            "初始化实体 DAL 实例",
            entity_class=self.entity_class.__name__ if self.entity_class else None,
            emoji="✅",
        )

    @classmethod
    def create_dal(
        cls: Type["EntityDAL[T]"],
        entity_type: Optional[Union[Type[T]]] = None,
        session: Optional[AsyncSession] = None,
    ) -> "EntityDAL[T]":
        """
        创建实体 DAL 实例的统一工厂方法。

        Args:
            entity_type: 实体类型或会话对象。
            session: SQLAlchemy 异步会话对象。

        Returns:
            特定类型的 DAL 实例。
        """
        logger = setup_logging(f"alphapower.dal.{cls.__name__}")
        logger.debug(
            "调用实体 DAL 工厂方法",
            dal_class=cls.__name__,
            entity_type=entity_type.__name__ if entity_type else None,
            emoji="🏭",
        )

        if not isinstance(session, AsyncSession):
            logger.error(
                "会话对象缺失或类型错误",
                session_type=type(session).__name__,
                emoji="❌",
            )
            raise ValueError("会话对象必须提供且必须是AsyncSession实例")

        if isinstance(entity_type, type):
            logger.debug(
                "使用提供的实体类型",
                entity_type=entity_type.__name__,
                emoji="✅",
            )
            return cls(session=session)

        if cls.entity_class is None:
            logger.error(
                "未定义实体类型",
                dal_class=cls.__name__,
                emoji="❌",
            )
            raise ValueError(f"子类 {cls.__name__} 必须提供实体类型或定义 entity_class")

        logger.info(
            "实体 DAL 实例创建成功",
            dal_class=cls.__name__,
            entity_class=cls.entity_class.__name__,
            emoji="✅",
        )
        return cls(session=session)
