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
    "Classification",
    "Competition",
    "Regular",
    "alphas_classifications",
    "alphas_competitions",
    "Sample",
    "Check",
    "Correlation",
    "ChecksBase",
    "Setting",
    "DataBase",
    "Category",
    "DataField",
    "dataset_research_papers",
    "Dataset",
    "ResearchPaper",
    "SimulationBase",
    "SimulationTask",
    "SimulationTaskStatus",
    "StatsData",
    "Pyramid",
    "CheckRecord",
    "RecordSet",
]

# 数据库常量和会话管理工具
from typing import Any, List, Tuple

from alphapower.constants import ENV_DEV, ENV_TEST, Database
from alphapower.internal.db_session import sync_register_db
from alphapower.settings import settings

# Alpha 策略相关实体
from .alphas import (
    Alpha,
)
from .alphas import Base as AlphaBase
from .alphas import (
    Check,
    Classification,
    Competition,
    Regular,
    Sample,
    Setting,
    alphas_classifications,
    alphas_competitions,
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
from .evaluate import Base as ChecksBase
from .evaluate import CheckRecord, Correlation, RecordSet

# 模拟相关实体
from .simulation import Base as SimulationBase
from .simulation import (
    SimulationTask,
    SimulationTaskStatus,
)


def register_all_entities() -> None:
    """
    将所有实体注册到对应的数据库

    此函数集中管理所有实体的注册逻辑，便于维护和扩展
    """
    entity_mappings: List[Tuple[Any, Database]] = [
        (AlphaBase, Database.ALPHAS),
        (DataBase, Database.DATA),
        (SimulationBase, Database.SIMULATION),
        (ChecksBase, Database.EVALUATE),
    ]

    force_recreate_db: bool = settings.environment in [ENV_DEV, ENV_TEST]

    for base, db in entity_mappings:
        sync_register_db(base, db, settings.databases[db], force_recreate_db)


# 在模块导入时自动注册所有实体
register_all_entities()
