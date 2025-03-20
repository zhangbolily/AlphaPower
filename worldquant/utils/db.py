from contextlib import contextmanager
from worldquant.storage.session import get_db


@contextmanager
def get_db_session(db_name: str):
    """
    上下文管理器，用于获取数据库会话。

    参数:
    db_name (str): 数据库名称。

    返回:
    sqlalchemy.orm.Session: 数据库会话。
    """
    db_generator = get_db(db_name)
    session = next(db_generator)
    try:
        yield session
    finally:
        session.close()


def with_session(db_name="default"):
    """
    装饰器，用于在函数中注入数据库会话。

    参数:
    db_name (str): 数据库名称，默认为 "default"。

    返回:
    function: 包装后的函数。
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            with get_db_session(db_name) as session:
                return func(session, *args, **kwargs)

        return wrapper

    return decorator
