from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os
import json

# 从环境变量中获取数据库连接字符串
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///example.db")

# 创建数据库引擎
engine = create_engine(DATABASE_URL, echo=True)

# 创建一个配置类，用于生成会话
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建一个线程安全的会话
Session = scoped_session(SessionLocal)

# 从 config.json 文件中读取数据库连接参数
def get_database_url(db_name):
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config['db'][db_name]['url']

# 根据入参返回不同的数据库连接
def get_db(db_name='alphas'):
    """
    获取数据库会话
    """
    db_url = get_database_url(db_name)
    engine = create_engine(db_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = scoped_session(SessionLocal)()
    try:
        yield db
    finally:
        db.close()