from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os

# 从环境变量中获取数据库连接字符串
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///example.db")

# 创建数据库引擎
engine = create_engine(DATABASE_URL, echo=True)

# 创建一个配置类，用于生成会话
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建一个线程安全的会话
Session = scoped_session(SessionLocal)

def get_db():
    """
    获取数据库会话
    """
    db = Session()
    try:
        yield db
    finally:
        db.close()