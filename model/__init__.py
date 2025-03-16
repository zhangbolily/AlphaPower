from ._session import get_db, engine, Session
from .entity._alphas import Base as AlphasBase
from .entity._alphas import (
    Alphas,
    Alphas_Settings,
    Alphas_Regular,
    Alphas_Sample,
    Alphas_Sample_Check,
    Alphas_Classification,
    Alphas_Competition,
)
from .entity._data import Base as DataBase
from .entity._data import (
    DataSet,
    Data_Category,
    Data_Subcategory,
    StatsData,
    ResearchPaper,
    DataField,
)

# 创建所有表
AlphasBase.metadata.create_all(engine)
DataBase.metadata.create_all(engine)
