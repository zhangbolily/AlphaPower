from typing import Dict, Type, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.dal.alphas import (
    AlphaDAL,
    ClassificationDAL,
    CompetitionDAL,
    RegularDAL,
    SampleCheckDAL,
    SampleDAL,
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
    Alpha,
    Category,
    Check,
    Classification,
    Competition,
    DataField,
    Dataset,
    Pyramid,
    Regular,
    ResearchPaper,
    Sample,
    Setting,
    SimulationTask,
    StatsData,
)

# 定义泛型类型变量，用于表示实体类型
T = TypeVar("T")


class DALRegistry:
    """DAL 注册表，用于管理和获取各种 DAL 实例。"""

    _dals: Dict[Type, Type[BaseDAL]] = {
        Alpha: AlphaDAL,
        Setting: SettingDAL,
        Regular: RegularDAL,
        Classification: ClassificationDAL,
        Competition: CompetitionDAL,
        Sample: SampleDAL,
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
