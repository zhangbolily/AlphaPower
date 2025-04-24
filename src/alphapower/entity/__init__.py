"""实体模块，包含 AlphaPower 项目的所有数据模型类。

这个模块包含的实体类被组织成三个主要类别：
1. alphas - 用于 Alpha 策略及相关分类的实体
2. data - 用于数据集、研究论文和统计数据的实体
3. simulation - 用于模拟任务和状态管理的实体

所有实体类在导入时会自动注册到对应的数据库。
"""

__all__ = [
    "Alpha",
    "AlphaBase",
    "Category",
    "CheckRecord",
    "EvaluateBase",
    "Competition",
    "Correlation",
    "DataBase",
    "DataField",
    "dataset_research_papers",
    "Dataset",
    "EvaluateRecord",
    "Pyramid",
    "RecordSet",
    "ResearchPaper",
    "AggregateData",
    "SimulationBase",
    "SimulationTask",
    "SimulationTaskStatus",
    "StatsData",
]

# 数据库常量和会话管理工具
from typing import Dict, Final, Type

from sqlalchemy.orm import DeclarativeBase

from alphapower.constants import Database

# Alpha 策略相关实体
from .alphas import (
    AggregateData,
    Alpha,
)
from .alphas import Base as AlphaBase
from .alphas import (
    Competition,
)

# 数据相关实体
from .data import Base as DataBase
from .data import (
    Category,
    DataField,
    Dataset,
    Pyramid,
    ResearchPaper,
    StatsData,
    dataset_research_papers,
)
from .evaluate import Base as EvaluateBase
from .evaluate import CheckRecord, Correlation, EvaluateRecord, RecordSet

# 模拟相关实体
from .simulation import Base as SimulationBase
from .simulation import (
    SimulationTask,
    SimulationTaskStatus,
)

DATABASE_BASE_CLASS_MAP: Final[Dict[Database, Type[DeclarativeBase]]] = {
    Database.ALPHAS: AlphaBase,
    Database.DATA: DataBase,
    Database.SIMULATION: SimulationBase,
    Database.EVALUATE: EvaluateBase,
}
