"""
dal.py

数据访问层模块，提供对实体模型的增删改查操作接口。
此模块封装了数据库操作，简化了应用程序与数据库的交互。
"""

from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union, cast

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Delete, Select, Update

from alphapower.constants import Regoin  # 添加枚举类型导入
from alphapower.entity.alphas import (
    Alpha,
    Classification,
    Competition,
    Regular,
    Sample,
    SampleCheck,
    Setting,
)
from alphapower.entity.data import (
    Category,
    DataField,
    Dataset,
    Pyramid,
    ResearchPaper,
    StatsData,
)
from alphapower.entity.simulation import SimulationTask, SimulationTaskStatus

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


# 重构后的 AlphaDAL 类，继承自 EntityDAL
class AlphaDAL(EntityDAL[Alpha]):
    """Alpha 数据访问层类，提供对 Alpha 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """
        初始化 AlphaDAL 实例。

        Args:
            session: SQLAlchemy 异步会话对象。
        """
        super().__init__(Alpha, session)

    async def find_by_alpha_id(
        self, alpha_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Alpha]:
        """通过 alpha_id 查询 Alpha 实体。"""
        return await self.find_one_by(session=session, alpha_id=alpha_id)

    async def find_by_author(
        self, author: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """查询指定作者的所有 Alpha。"""
        return await self.find_by(session=session, author=author)

    async def find_by_status(
        self, status: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """查询指定状态的所有 Alpha。"""
        return await self.find_by(session=session, status=status)

    async def find_favorites(
        self, author: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """查询指定作者的收藏 Alpha。"""
        return await self.find_by(session=session, author=author, favorite=True)

    async def find_by_classification(
        self, classification_id: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询属于特定分类的所有 Alpha。

        Args:
            classification_id: 分类 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的 Alpha 列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = (
            select(Alpha)
            .join(Alpha.classifications)
            .where(Classification.classification_id == classification_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_competition(
        self, competition_id: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询参与特定比赛的所有 Alpha。

        Args:
            competition_id: 比赛 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的 Alpha 列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = (
            select(Alpha)
            .join(Alpha.competitions)
            .where(Competition.competition_id == competition_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class SettingDAL(EntityDAL[Setting]):
    """Setting 数据访问层类，提供对 Setting 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 SettingDAL 实例。"""
        super().__init__(Setting, session)


class RegularDAL(EntityDAL[Regular]):
    """Regular 数据访问层类，提供对 Regular 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 RegularDAL 实例。"""
        super().__init__(Regular, session)

    async def find_similar_code(
        self, code_fragment: str, session: Optional[AsyncSession] = None
    ) -> List[Regular]:
        """
        查询包含特定代码片段的所有规则。

        Args:
            code_fragment: 要搜索的代码片段。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的规则列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(Regular).where(Regular.code.contains(code_fragment))
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class ClassificationDAL(EntityDAL[Classification]):
    """Classification 数据访问层类，提供对 Classification 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 ClassificationDAL 实例。"""
        super().__init__(Classification, session)

    async def find_by_classification_id(
        self, classification_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Classification]:
        """通过 classification_id 查询分类。"""
        return await self.find_one_by(
            session=session, classification_id=classification_id
        )


class CompetitionDAL(EntityDAL[Competition]):
    """Competition 数据访问层类，提供对 Competition 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 CompetitionDAL 实例。"""
        super().__init__(Competition, session)

    async def find_by_competition_id(
        self, competition_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Competition]:
        """通过 competition_id 查询比赛。"""
        return await self.find_one_by(session=session, competition_id=competition_id)


class SampleDAL(EntityDAL[Sample]):
    """Sample 数据访问层类，提供对 Sample 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 SampleDAL 实例。"""
        super().__init__(Sample, session)

    async def find_by_performance(
        self, min_sharpe: float, session: Optional[AsyncSession] = None
    ) -> List[Sample]:
        """查询 sharpe 比率大于指定值的所有样本。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(Sample).where(Sample.sharpe >= min_sharpe)
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class SampleCheckDAL(EntityDAL[SampleCheck]):
    """SampleCheck 数据访问层类，提供对 SampleCheck 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 SampleCheckDAL 实例。"""
        super().__init__(SampleCheck, session)


class DatasetDAL(EntityDAL[Dataset]):
    """Dataset 数据访问层类，提供对 Dataset 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 DatasetDAL 实例。"""
        super().__init__(Dataset, session)

    async def find_by_dataset_id(
        self, dataset_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Dataset]:
        """通过 dataset_id 查询数据集。"""
        return await self.find_one_by(session=session, dataset_id=dataset_id)

    async def find_by_region(
        self, region: Regoin, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询特定区域的所有数据集。

        Args:
            region: 区域枚举值
            session: 可选的会话对象，若提供则优先使用

        Returns:
            符合条件的数据集列表
        """
        return await self.find_by(session=session, region=region.name)

    async def find_high_value_datasets(
        self, min_value: float, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """查询价值分数高于指定值的数据集。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(Dataset).where(Dataset.value_score >= min_value)
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_category(
        self, category_id: str, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """
        查询属于特定分类的所有数据集。

        Args:
            category_id: 分类 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的数据集列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = (
            select(Dataset)
            .join(Dataset.categories)
            .where(Category.category_id == category_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_with_fields_count(
        self, min_count: int, session: Optional[AsyncSession] = None
    ) -> List[Dataset]:
        """查询包含至少指定数量字段的数据集。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(Dataset).where(Dataset.field_count >= min_count)
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class CategoryDAL(EntityDAL[Category]):
    """Category 数据访问层类，提供对 Category 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 CategoryDAL 实例。"""
        super().__init__(Category, session)

    async def find_by_category_id(
        self, category_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Category]:
        """通过 category_id 查询分类。"""
        return await self.find_one_by(session=session, category_id=category_id)

    async def find_top_level_categories(
        self, session: Optional[AsyncSession] = None
    ) -> List[Category]:
        """查询所有顶级分类（没有父分类的分类）。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(Category).where(Category.parent_id.is_(None))
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_children_categories(
        self, parent_id: int, session: Optional[AsyncSession] = None
    ) -> List[Category]:
        """查询特定父分类的所有子分类。"""
        return await self.find_by(session=session, parent_id=parent_id)


class DataFieldDAL(EntityDAL[DataField]):
    """DataField 数据访问层类，提供对 DataField 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 DataFieldDAL 实例。"""
        super().__init__(DataField, session)

    async def find_by_field_id(
        self, field_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[DataField]:
        """通过 field_id 查询数据字段。"""
        return await self.find_one_by(session=session, field_id=field_id)

    async def find_by_dataset(
        self, dataset_id: int, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """查询属于特定数据集的所有字段。"""
        return await self.find_by(session=session, dataset_id=dataset_id)

    async def find_by_type(
        self, field_type: str, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """查询特定类型的所有字段。"""
        return await self.find_by(session=session, type=field_type)

    async def find_high_coverage_fields(
        self, min_coverage: float, session: Optional[AsyncSession] = None
    ) -> List[DataField]:
        """查询覆盖率高于指定值的字段。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(DataField).where(DataField.coverage >= min_coverage)
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class StatsDataDAL(EntityDAL[StatsData]):
    """StatsData 数据访问层类，提供对 StatsData 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 StatsDataDAL 实例。"""
        super().__init__(StatsData, session)

    async def find_by_dataset_id(
        self, dataset_id: int, session: Optional[AsyncSession] = None
    ) -> List[StatsData]:
        """查询与特定数据集关联的所有统计数据。"""
        return await self.find_by(session=session, data_set_id=dataset_id)

    async def find_by_data_field_id(
        self, data_field_id: int, session: Optional[AsyncSession] = None
    ) -> List[StatsData]:
        """查询与特定数据字段关联的所有统计数据。"""
        return await self.find_by(session=session, data_field_id=data_field_id)


class ResearchPaperDAL(EntityDAL[ResearchPaper]):
    """ResearchPaper 数据访问层类，提供对 ResearchPaper 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 ResearchPaperDAL 实例。"""
        super().__init__(ResearchPaper, session)

    async def find_by_type(
        self, paper_type: str, session: Optional[AsyncSession] = None
    ) -> List[ResearchPaper]:
        """查询特定类型的所有研究论文。"""
        return await self.find_by(session=session, type=paper_type)


class PyramidDAL(EntityDAL[Pyramid]):
    """Pyramid 数据访问层类，提供对 Pyramid 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 PyramidDAL 实例。"""
        super().__init__(Pyramid, session)

    async def find_by_region(
        self, region: Regoin, session: Optional[AsyncSession] = None
    ) -> List[Pyramid]:
        """
        查询特定区域的所有金字塔。

        Args:
            region: 区域枚举值
            session: 可选的会话对象，若提供则优先使用

        Returns:
            符合条件的金字塔列表
        """
        return await self.find_by(session=session, region=region.name)

    async def find_by_category(
        self, category_id: int, session: Optional[AsyncSession] = None
    ) -> List[Pyramid]:
        """查询与特定分类关联的所有金字塔。"""
        return await self.find_by(session=session, category_id=category_id)


class SimulationTaskDAL(EntityDAL[SimulationTask]):
    """SimulationTask 数据访问层类，提供对 SimulationTask 实体的特定操作。"""

    def __init__(self, session: AsyncSession) -> None:
        """初始化 SimulationTaskDAL 实例。"""
        super().__init__(SimulationTask, session)

    async def find_by_status(
        self, status: Any, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询特定状态的所有任务。"""
        return await self.find_by(session=session, status=status)

    async def find_by_alpha_id(
        self, alpha_id: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询与特定 Alpha 关联的所有任务。"""
        return await self.find_by(session=session, alpha_id=alpha_id)

    async def find_by_signature(
        self, signature: str, session: Optional[AsyncSession] = None
    ) -> Optional[SimulationTask]:
        """通过签名查询任务。"""
        return await self.find_one_by(session=session, signature=signature)

    async def find_pending_tasks(
        self, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询所有待处理的任务。"""
        return await self.find_by(session=session, status=SimulationTaskStatus.PENDING)

    async def find_running_tasks(
        self, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询所有正在运行的任务。"""
        return await self.find_by(session=session, status=SimulationTaskStatus.RUNNING)

    async def find_high_priority_tasks(
        self, min_priority: int, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询优先级高于指定值的所有任务。"""
        actual_session: AsyncSession = session or self.session
        query: Select = select(SimulationTask).where(
            SimulationTask.priority >= min_priority
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_settings_group(
        self, group_key: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """查询属于特定设置组的所有任务。"""
        return await self.find_by(session=session, settings_group_key=group_key)

    async def find_tasks_by_date_range(
        self, start_date: str, end_date: str, session: Optional[AsyncSession] = None
    ) -> List[SimulationTask]:
        """
        查询在指定日期范围内创建的所有任务。

        Args:
            start_date: 开始日期字符串，格式 'YYYY-MM-DD'。
            end_date: 结束日期字符串，格式 'YYYY-MM-DD'。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的任务列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(SimulationTask).where(
            SimulationTask.created_at.between(start_date, end_date)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())


# 添加一个 DAL 注册表，方便创建和查找 DAL 实例
class DALRegistry:
    """DAL 注册表，用于管理和获取各种 DAL 实例。"""

    _dals: Dict[Type, Type[BaseDAL]] = {
        Alpha: AlphaDAL,
        Setting: SettingDAL,
        Regular: RegularDAL,
        Classification: ClassificationDAL,
        Competition: CompetitionDAL,
        Sample: SampleDAL,
        SampleCheck: SampleCheckDAL,
        Dataset: DatasetDAL,
        Category: CategoryDAL,
        DataField: DataFieldDAL,
        StatsData: StatsDataDAL,
        ResearchPaper: ResearchPaperDAL,
        Pyramid: PyramidDAL,
        SimulationTask: SimulationTaskDAL,
    }

    @classmethod
    def get_dal(cls, entity_type: Type, session: AsyncSession) -> BaseDAL:
        """
        获取特定实体类型的 DAL 实例。

        Args:
            entity_type: 实体类型。
            session: SQLAlchemy 异步会话对象。

        Returns:
            对应的 DAL 实例。

        Raises:
            ValueError: 当找不到对应的 DAL 类型时。
        """
        if entity_type not in cls._dals:
            raise ValueError(
                f"No DAL registered for entity type: {entity_type.__name__}"
            )

        dal_class: Type[BaseDAL] = cls._dals[entity_type]
        return dal_class.create(session=session)
