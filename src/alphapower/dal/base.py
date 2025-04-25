"""
基础数据访问层 (DAL) 模块
提供通用的 CRUD 操作，支持异步数据库交互。
"""

import traceback
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import MappedColumn
from sqlalchemy.sql.expression import ColumnExpressionArgument, Delete, Select, Update
from structlog.stdlib import BoundLogger
from typing_extensions import Protocol

from alphapower.internal.logging import get_logger

# pylint: disable=E1102


class HasEntity(Protocol):
    """约束泛型类型 T 必须包含 id 属性。"""

    id: MappedColumn[int]


# 修改泛型类型变量 T，使其必须满足 HasID 协议
T = TypeVar("T", bound=HasEntity)


class BaseDAL(Generic[T]):
    """
    基础数据访问层类，提供通用的 CRUD 操作。
    """

    entity_class: Type[T] = None  # type: ignore

    def __init__(self, entity_type: Type[T], session: Optional[AsyncSession]) -> None:
        self.entity_type: Type[T] = entity_type
        self.session: Optional[AsyncSession] = session
        self.log: BoundLogger = get_logger(f"alphapower.dal.{self.__class__.__name__}")
        self.log.info(
            "初始化DAL实例", entity_type=self.entity_type.__name__, emoji="✅"
        )

    def _actual_session(self, session: Optional[AsyncSession]) -> AsyncSession:
        actual_session: AsyncSession
        if session is not None:
            actual_session = session
        elif self.session is not None:
            actual_session = self.session
        else:
            self.log.error("会话对象缺失", emoji="❌")
            raise ValueError("会话对象缺失")
        if not isinstance(actual_session, AsyncSession):
            self.log.error(
                "会话对象类型错误",
                session_type=type(actual_session).__name__,
                emoji="❌",
            )
            raise ValueError("会话对象必须是AsyncSession实例")
        return actual_session

    @classmethod
    def create_dal(
        cls: Type["BaseDAL[T]"], session: Optional[AsyncSession]
    ) -> "BaseDAL[T]":
        logger = get_logger(f"alphapower.dal.{cls.__name__}")
        logger.debug("调用DAL工厂方法", dal_class=cls.__name__, emoji="🏭")
        actual_entity_type: Optional[Type[T]] = None
        if cls.entity_class is not None:
            logger.debug(
                "使用子类定义的实体类型",
                entity_type=cls.entity_class.__name__,
                emoji="✅",
            )
            actual_entity_type = cls.entity_class
        elif cls != BaseDAL:
            logger.error("未定义实体类型", dal_class=cls.__name__, emoji="❌")
            raise ValueError(f"子类 {cls.__name__} 必须提供实体类型或定义entity_class")
        else:
            logger.error("BaseDAL 不能直接创建实例，请使用子类", emoji="❌")
            raise TypeError("BaseDAL 不能直接创建实例，请使用子类")
        logger.info(
            "创建DAL实例成功",
            dal_class=cls.__name__,
            entity_type=actual_entity_type.__name__,
            emoji="✅",
        )
        return cls(entity_type=actual_entity_type, session=session)

    async def create_entity(
        self, session: Optional[AsyncSession] = None, **kwargs: Any
    ) -> T:
        """
        创建一个新的实体记录。

        Args:
            **kwargs: 实体属性键值对。

        Returns:
            新创建的实体对象。

        Raises:
            SQLAlchemyError: 当数据库操作失败时。
        """
        self.log.debug(
            "创建实体",
            entity_type=self.entity_type.__name__,
            attributes=kwargs,
            emoji="📦",
        )
        try:
            actual_session: AsyncSession = self._actual_session(session)

            entity: T = self.entity_type(**kwargs)
            actual_session.add(entity)
            await actual_session.flush()
            self.log.info(
                "成功创建实体",
                entity_id=getattr(entity, "id", "unknown"),
                emoji="✅",
            )
            return entity
        except Exception as e:
            self.log.error(
                "实体创建失败",
                entity_type=self.entity_type.__name__,
                attributes=kwargs,
                error=str(e),
                emoji="❌",
            )

            self.log.error(
                "错误堆栈",
                traceback=traceback.format_exc(),
                emoji="🛠️",
            )
            raise

    async def create(self, entity: T, session: Optional[AsyncSession] = None) -> T:
        """
        创建单个实体对象。

        Args:
            entity: 实体对象。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            新创建的实体对象。
        """
        actual_session: AsyncSession = self._actual_session(session)
        actual_session.add(entity)
        return entity

    async def bulk_create(
        self, entities: List[T], session: Optional[AsyncSession] = None
    ) -> List[T]:
        """
        批量创建实体对象。

        Args:
            entities: 实体对象列表。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            新创建的实体对象列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        actual_session.add_all(entities)
        await actual_session.flush()
        return entities

    async def upsert(self, entity: T, session: Optional[AsyncSession] = None) -> T:
        """
        插入或更新实体对象。

        Args:
            entity: 实体对象。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            插入或更新后的实体对象。
        """
        actual_session: AsyncSession = self._actual_session(session)
        existing_entity = await self.get_by_id(entity.id, session=actual_session)
        if existing_entity:
            await actual_session.merge(entity)
            await actual_session.flush()
            return existing_entity
        return await self.create(entity, session=actual_session)

    async def upsert_by_unique_key(
        self, entity: T, unique_key: str, session: Optional[AsyncSession] = None
    ) -> T:
        """
        根据唯一键插入或更新实体对象。

        Args:
            entity: 实体对象。
            unique_key: 唯一键的名称。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            插入或更新后的实体对象。
        """
        actual_session: AsyncSession = self._actual_session(session)

        existing_entity = await self.find_one_by(
            **{unique_key: getattr(entity, unique_key)}
        )
        if existing_entity:
            entity.id = existing_entity.id
            await actual_session.merge(entity)
            await actual_session.flush()
            return existing_entity
        return await self.create(entity)

    async def bulk_upsert(
        self, entities: List[T], session: Optional[AsyncSession] = None
    ) -> List[T]:
        """
        批量插入或更新实体对象。

        Args:
            entities: 实体对象列表。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            插入或更新后的实体对象列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        if not entities:
            return []

        ids: List[int] = [entity.id for entity in entities]
        existing_entities = await self.find_by(in_={"id": ids})
        existing_entities_map: Dict[Any, T] = {
            entity.id: entity for entity in existing_entities
        }
        new_entities: List[T] = []

        for entity in entities:
            if entity.id not in existing_entities_map:
                new_entities.append(entity)
            else:
                entity.id = existing_entities_map[entity.id].id
                await actual_session.merge(entity)

        actual_session.add_all(new_entities)
        await actual_session.flush()
        return entities

    async def bulk_upsert_by_unique_key(
        self, entities: List[T], unique_key: str, session: Optional[AsyncSession] = None
    ) -> List[T]:
        """
        批量插入或更新实体对象，根据唯一键。

        Args:
            entities: 实体对象列表。
            unique_key: 唯一键的名称。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            插入或更新后的实体对象列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        if not entities:
            return []

        unique_values: List[Any] = [getattr(entity, unique_key) for entity in entities]
        existing_entities = await self.find_by(in_={unique_key: unique_values})
        existing_entities_map: Dict[Any, T] = {
            getattr(entity, unique_key): entity for entity in existing_entities
        }
        new_entities: List[T] = []

        for entity in entities:
            unique_value = getattr(entity, unique_key)
            if unique_value not in existing_entities_map:
                new_entities.append(entity)
            else:
                entity.id = existing_entities_map[unique_value].id
                await actual_session.merge(entity)

        actual_session.add_all(new_entities)
        await actual_session.flush()
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
        self.log.debug(
            "查询实体",
            entity_id=entity_id,
            emoji="🔍",
        )
        actual_session: AsyncSession = self._actual_session(session)
        entity = await actual_session.get(self.entity_type, entity_id)
        if entity:
            self.log.info(
                "查询成功",
                entity_id=entity_id,
                emoji="✅",
            )
        else:
            self.log.warning(
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
        actual_session: AsyncSession = self._actual_session(session)
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
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(self.entity_type)
        criteria: List[ColumnExpressionArgument] = []
        if in_:
            for key, values in in_.items():
                if hasattr(self.entity_type, key):
                    column = getattr(self.entity_type, key)
                    criteria.append(column.in_(values))
                else:
                    raise ValueError(f"无效的字段名: {key}")
        if notin_:
            for key, values in notin_.items():
                if hasattr(self.entity_type, key):
                    column = getattr(self.entity_type, key)
                    criteria.append(column.notin_(values))
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
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        if order_by is not None:  # 必须加上 is not None
            query = query.order_by(order_by)
        result = await actual_session.execute(query.limit(1))
        return result.scalars().first()

    async def find_ids_by(
        self,
        session: Optional[AsyncSession] = None,
        **kwargs: Any,
    ) -> Optional[int]:
        """
        按条件查找单个实体的 ID。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 查询条件的键值对。

        Returns:
            符合条件的第一个实体的 ID，如果未找到则返回 None。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(self.entity_type.id)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query.limit(1))
        return result.scalars().first()

    async def find_one_id_by(
        self,
        session: Optional[AsyncSession] = None,
        **kwargs: Any,
    ) -> Optional[int]:
        """
        按条件查找单个实体的 ID。

        Args:
            session: 可选的会话对象，若提供则优先使用。
            **kwargs: 查询条件的键值对。

        Returns:
            符合条件的第一个实体的 ID，如果未找到则返回 None。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(self.entity_type.id)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
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
        self.log.debug(
            "更新实体",
            entity_id=entity_id,
            update_fields=kwargs,
            emoji="✏️",
        )
        actual_session: AsyncSession = self._actual_session(session)
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            for key, value in kwargs.items():
                setattr(entity, key, value)
            await actual_session.flush()
            self.log.info(
                "更新成功",
                entity_id=entity_id,
                updated_fields=kwargs,
                emoji="✅",
            )
            return entity
        self.log.warning(
            "更新失败，实体不存在",
            entity_id=entity_id,
            emoji="⚠️",
        )
        return None

    async def update(self, entity: T, session: Optional[AsyncSession] = None) -> T:
        """
        更新单个实体对象。

        Args:
            entity: 实体对象。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            更新后的实体对象。
        """
        actual_session: AsyncSession = self._actual_session(session)
        await actual_session.merge(entity)
        return entity

    async def update_all(
        self, entities: List[T], session: Optional[AsyncSession] = None
    ) -> List[T]:
        """
        批量更新实体对象。

        Args:
            entities: 实体对象列表。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            更新后的实体对象列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        for entity in entities:
            await actual_session.merge(entity)
        await actual_session.flush()
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
        actual_session: AsyncSession = self._actual_session(session)
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
        self.log.debug(
            "删除实体",
            entity_id=entity_id,
            emoji="🗑️",
        )
        actual_session: AsyncSession = self._actual_session(session)
        entity: Optional[T] = await self.get_by_id(entity_id, session=actual_session)
        if entity:
            await actual_session.delete(entity)
            await actual_session.flush()
            self.log.info(
                "删除成功",
                entity_id=entity_id,
                emoji="✅",
            )
            return True
        self.log.warning(
            "删除失败，实体不存在",
            entity_id=entity_id,
            emoji="⚠️",
        )
        return False

    async def delete(self, entity: T, session: Optional[AsyncSession] = None) -> bool:
        """
        删除单个实体对象。

        Args:
            entity: 实体对象。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            如果成功删除返回 True。
        """
        actual_session: AsyncSession = self._actual_session(session)
        await actual_session.delete(entity)
        await actual_session.flush()
        return True

    async def delete_all(
        self, entities: List[T], session: Optional[AsyncSession] = None
    ) -> int:
        """
        批量删除实体对象。

        Args:
            entities: 实体对象列表。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            删除的记录数量。
        """
        actual_session: AsyncSession = self._actual_session(session)
        for e in entities:
            await actual_session.delete(e)
        await actual_session.flush()
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
        self.log.debug(
            "按条件删除实体",
            filter_conditions=kwargs,
            emoji="🗑️",
        )
        actual_session: AsyncSession = self._actual_session(session)
        query: Delete = delete(self.entity_type)
        for key, value in kwargs.items():
            query = query.where(getattr(self.entity_type, key) == value)
        result = await actual_session.execute(query)
        deleted_count = result.rowcount
        if deleted_count > 0:
            self.log.info(
                "删除成功",
                deleted_count=deleted_count,
                filter_conditions=kwargs,
                emoji="✅",
            )
        else:
            self.log.warning(
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
        self.log.debug(
            "统计实体数量",
            filter_conditions=kwargs,
            in_conditions=in_,
            notin_conditions=notin_,
            emoji="📊",
        )
        actual_session: AsyncSession = self._actual_session(session)
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
        self.log.info(
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
        actual_session: AsyncSession = self._actual_session(session)
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def execute_stream_query(
        self, query: Select, session: Optional[AsyncSession] = None
    ) -> AsyncGenerator[T, None]:
        """
        执行自定义查询，返回异步生成器。

        Args:
            query: SQLAlchemy Select 查询对象。
            session: 可选的会话对象，若提供则优先使用。

        Yields:
            查询结果列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        result = await actual_session.stream_scalars(query)
        async for entity in result:
            yield entity


# 为 DALFactory 创建的泛型类型变量
D = TypeVar("D", bound=BaseDAL)


# 创建一个通用的DAL工厂方法基类
class DALFactory:
    """DAL 工厂类，提供创建各种 DAL 实例的方法。"""

    @staticmethod
    def create_dal(dal_class: Type[D], session: Optional[AsyncSession] = None) -> D:
        logger = get_logger(f"alphapower.dal.{dal_class.__name__}")
        logger.debug(
            "创建 DAL 实例",
            dal_class=dal_class.__name__,
            session_type=type(session).__name__,
            emoji="🏭",
        )
        dal_instance: D = dal_class.create_dal(session=session)  # type: ignore[assignment]
        logger.info("DAL 实例创建成功", dal_class=dal_class.__name__, emoji="✅")
        return dal_instance


# 简化子类的 create 方法实现，使用统一模板
class EntityDAL(BaseDAL[T]):
    """特定实体 DAL 基类。"""

    entity_class: Type[T] = None  # type: ignore

    def __init__(self, session: Optional[AsyncSession] = None) -> None:
        if self.entity_class is None:
            logger = get_logger(f"alphapower.dal.{self.__class__.__name__}")
            logger.error(
                "未定义实体类型", dal_class=self.__class__.__name__, emoji="❌"
            )
            raise ValueError(f"子类 {self.__class__.__name__} 必须定义 entity_class")
        super().__init__(self.entity_class, session)
        self.log.info(
            "初始化实体 DAL 实例", entity_class=self.entity_class.__name__, emoji="✅"
        )

    @classmethod
    def create_dal(
        cls: Type["EntityDAL[T]"], session: Optional[AsyncSession] = None
    ) -> "EntityDAL[T]":
        logger = get_logger(f"alphapower.dal.{cls.__name__}")
        logger.debug("调用实体 DAL 工厂方法", dal_class=cls.__name__, emoji="🏭")
        if cls.entity_class is None:
            logger.error("未定义实体类型", dal_class=cls.__name__, emoji="❌")
            raise ValueError(f"子类 {cls.__name__} 必须定义 entity_class")
        logger.info(
            "实体 DAL 实例创建成功",
            dal_class=cls.__name__,
            entity_class=cls.entity_class.__name__,
            emoji="✅",
        )
        return cls(session=session)
