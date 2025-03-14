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

# 创建所有表
AlphasBase.metadata.create_all(bind=engine)
