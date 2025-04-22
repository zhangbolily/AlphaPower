"""
DAL 注册表模块
用于管理和获取各种 DAL 实例，简化DAL实例的创建和访问过程。
"""

from typing import Dict, Type

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.dal.alphas import (
    AggregateDataDAL,
    AlphaDAL,
    ClassificationDAL,
    CompetitionDAL,
    RegularDAL,
    SampleCheckDAL,
    SettingDAL,
)
from alphapower.dal.base import BaseDAL
from alphapower.dal.data import (
    CategoryDAL,
    DataFieldDAL,
    DatasetDAL,
    PyramidDAL,
    ResearchPaperDAL,
    StatsDataDAL,
)
from alphapower.dal.simulation import SimulationTaskDAL
from alphapower.entity import (
    AggregateData,
    Alpha,
    Category,
    Classification,
    Competition,
    DataField,
    Dataset,
    Expression,
    Pyramid,
    ResearchPaper,
    Setting,
    SimulationTask,
    StatsData,
)


class DALRegistry:
    """
    DAL 注册表，用于管理和获取各种 DAL 实例。

    提供了一个集中的位置来存储和访问所有DAL类型，
    使得代码可以根据实体类型动态选择正确的DAL类。

    Attributes:
        _dals: 存储实体类型到DAL类的映射字典。
    """

    # 实体类型到DAL类的映射
    _dals: Dict[Type, Type[BaseDAL]] = {
        Alpha: AlphaDAL,
        Setting: SettingDAL,
        Expression: RegularDAL,
        Classification: ClassificationDAL,
        Competition: CompetitionDAL,
        AggregateData: AggregateDataDAL,
        Check: SampleCheckDAL,
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

        根据提供的实体类型，查找对应的DAL类并创建实例。

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
        return dal_class.create_dal(session=session)
