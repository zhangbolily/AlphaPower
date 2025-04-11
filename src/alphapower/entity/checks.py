"""
此模块定义了与数据库交互的 ORM 模型类，用于存储和管理相关的实体数据。

模块功能：
- 定义基础 ORM 模型类 `Base`，提供异步属性访问功能。
- 定义 `Correlation` 类，用于存储相关性分析的结果。

注意事项：
- 所有 ORM 模型类必须继承自 `Base` 类。
- 使用 SQLAlchemy 的异步功能以支持高并发场景。
- 数据库字段类型使用 SQLAlchemy 提供的类型映射，确保与数据库兼容。
"""

from datetime import datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    Integer,
    func,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, MappedColumn, mapped_column


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。

    提供功能：
    - 异步属性访问：支持异步操作以提高性能。
    - SQLAlchemy 的基本 ORM 功能：包括映射、查询等。

    使用方法：
    - 继承此类以定义具体的 ORM 模型。
    """


class Correlation(Base):
    """相关性分析结果的 ORM 模型类。

    用于存储相关性分析的结果数据，包括最大值、最小值、表结构和记录等。

    属性：
        id (int): 主键，自增。
        alpha_id (int): Alpha 策略的唯一标识符。
        correlation_max (float): 相关性最大值。
        correlation_min (float): 相关性最小值。
        table_schema (dict): 表结构的 JSON 描述。
        records (list): 相关性分析的记录，存储为 JSON 格式。
        created_at (datetime): 记录的创建时间，默认为当前时间。

    注意事项：
    - `table_schema` 和 `records` 使用 JSON 格式存储复杂数据结构。
    - `created_at` 字段自动填充为记录创建时的时间戳。
    """

    __tablename__ = "correlations"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[int] = mapped_column(
        Integer,
        nullable=False,
    )
    correlation_max: MappedColumn[float] = mapped_column(
        Float,
        nullable=False,
    )
    correlation_min: MappedColumn[float] = mapped_column(
        Float,
        nullable=False,
    )
    table_schema: MappedColumn[dict] = mapped_column(JSON, nullable=False)
    records: MappedColumn[list] = mapped_column(JSON, nullable=False)
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
    )
