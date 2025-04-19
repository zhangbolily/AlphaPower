"""
此模块定义了与数据库交互的 ORM 模型类，用于存储和管理相关的实体数据。

模块功能：
- 定义基础 ORM 模型类 `Base`，提供异步属性访问功能。
- 定义 `Correlation` 类，用于存储两个 Alpha 策略之间的相关性分析结果。
- 定义 `CheckRecord` 类，用于存储 Alpha 策略的检查记录。

注意事项：
- 所有 ORM 模型类必须继承自 `Base` 类。
- 使用 SQLAlchemy 的异步功能以支持高并发场景。
- 数据库字段类型使用 SQLAlchemy 提供的类型映射，确保与数据库兼容。
"""

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    func,
)
from sqlalchemy.ext.asyncio import AsyncAttrs

# 导入 validates 装饰器
from sqlalchemy.orm import (
    DeclarativeBase,
    MappedColumn,
    mapped_column,
)

from alphapower.constants import (
    ALPHA_ID_LENGTH,
    CheckRecordType,
    CorrelationCalcType,
    RecordSetType,
)


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。

    提供功能：
    - 异步属性访问：支持异步操作以提高性能。
    - SQLAlchemy 的基本 ORM 功能：包括映射、查询等。

    使用方法：
    - 继承此类以定义具体的 ORM 模型。
    """


class Correlation(Base):
    """两个 Alpha 策略之间相关性分析结果的 ORM 模型类。

    用于存储两个 Alpha 策略 (`alpha_id_a`, `alpha_id_b`) 之间的相关性计算结果。
    为了确保查询的一致性，`alpha_id_a` 总是存储字典序较小的 ID，`alpha_id_b` 存储字典序较大的 ID。

    属性：
        id (int): 主键，自增。
        alpha_id_a (str): 第一个 Alpha 策略的唯一标识符（字典序较小者）。
        alpha_id_b (str): 第二个 Alpha 策略的唯一标识符（字典序较大者）。
        correlation (float): 计算得到的相关性数值。
        calc_type (CorrelationCalcType): 相关性计算的具体类型（例如皮尔逊、斯皮尔曼等）。
        created_at (datetime): 记录的创建时间，默认为当前时间。

    注意事项：
    - 在对象创建或更新时，`alpha_id_a` 和 `alpha_id_b` 会自动排序。
    - `created_at` 字段自动填充为记录创建时的时间戳。
    """

    __tablename__ = "correlations"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id_a: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        comment="第一个 Alpha ID (字典序较小)",  # 添加字段注释
    )
    alpha_id_b: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        comment="第二个 Alpha ID (字典序较大)",  # 添加字段注释
    )
    correlation: MappedColumn[float] = mapped_column(
        Float,
        nullable=False,
        comment="相关性数值",  # 添加字段注释
    )
    calc_type: MappedColumn[CorrelationCalcType] = mapped_column(
        Enum(CorrelationCalcType),
        nullable=False,
        comment="相关性计算类型",  # 添加字段注释
    )
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        comment="创建时间",  # 添加字段注释
    )

    def __init__(
        self,
        alpha_id_a: str,
        alpha_id_b: str,
        correlation: float,
        calc_type: CorrelationCalcType,
        **kw: Any,  # 允许传递其他 SQLAlchemy 可能需要的参数
    ):
        """初始化 Correlation 对象并确保 alpha_id 排序。

        Args:
            alpha_id_a (str): 第一个 Alpha 策略 ID。
            alpha_id_b (str): 第二个 Alpha 策略 ID。
            correlation (float): 相关性数值。
            calc_type (CorrelationCalcType): 相关性计算类型。
            **kw: 其他关键字参数。
        """
        # 在调用父类构造函数之前排序 alpha_id
        # 这样传递给 SQLAlchemy 核心的值已经是排序好的
        # 避免了在 validates 中需要再次排序和 setattr 的复杂性
        if alpha_id_a > alpha_id_b:
            alpha_id_a, alpha_id_b = alpha_id_b, alpha_id_a

        # 调用父类构造函数或 SQLAlchemy 的处理逻辑
        super().__init__(
            alpha_id_a=alpha_id_a,
            alpha_id_b=alpha_id_b,
            correlation=correlation,
            calc_type=calc_type,
            **kw,
        )


class CheckRecord(Base):
    """Alpha 策略检查记录的 ORM 模型类。

    用于存储对特定 Alpha 策略执行的各种检查（如回测、IC 分析等）的结果或状态。

    属性：
        id (int): 主键，自增。
        alpha_id (str): 被检查的 Alpha 策略的唯一标识符。
        record_type (CheckRecordType): 检查记录的类型（例如 BACKTEST, IC_ANALYSIS）。
        content (dict): 检查的具体内容或结果，存储为 JSON 格式。
        created_at (datetime): 记录的创建时间，默认为当前时间。

    注意事项：
    - `content` 使用 JSON 格式存储灵活的检查结果数据结构。
    - `created_at` 字段自动填充为记录创建时的时间戳。
    """

    __tablename__ = "check_records"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # 修正类型注解 int -> str
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        comment="Alpha ID",  # 添加字段注释
    )
    record_type: MappedColumn[CheckRecordType] = mapped_column(
        Enum(CheckRecordType),
        nullable=False,
        comment="记录类型",  # 添加字段注释
    )
    content: MappedColumn[Dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        comment="检查内容 (JSON)",  # 添加字段注释
    )
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        comment="创建时间",  # 添加字段注释
    )


class RecordSet(Base):
    __tablename__ = "record_sets"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        comment="Alpha ID",  # 添加字段注释
    )
    set_type: MappedColumn[RecordSetType] = mapped_column(
        Enum(RecordSetType),
        nullable=False,
        comment="记录集类型",  # 添加字段注释
    )
    content: MappedColumn[JSON] = mapped_column(
        JSON,
        nullable=False,
        comment="记录集内容 (JSON)",  # 添加字段注释
    )
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        comment="创建时间",  # 添加字段注释
    )
