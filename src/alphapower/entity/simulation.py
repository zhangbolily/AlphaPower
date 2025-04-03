# -*- coding: utf-8 -*-
"""模拟任务实体类模块。

此模块定义了与模拟任务相关的数据库模型，包括任务的各种属性和状态。
这些模型用于在数据库中表示和跟踪模拟任务的生命周期。

Example:
    创建一个新的模拟任务：

    ```python
    task = SimulationTask(
        type=SimulationTaskType.REGULAR,
        settings={"param1": "value1"},
        settings_group_key="group1",
        regular="regular_value",
        status=SimulationTaskStatus.PENDING,
        signature="unique_signature"
    )
    ```

Attributes:
    Base (DeclarativeBase): 基础数据库模型类，支持异步操作。
"""

import enum
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import JSON, Boolean, DateTime, Enum, Float, Integer, String, func
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, validates

from alphapower.constants import (
    Decay,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    Switch,
    Truncation,
    UnitHandling,
    Universe,
)


class Base(AsyncAttrs, DeclarativeBase):
    """基础数据库模型类。

    为所有实体模型提供异步操作支持的基础类。
    继承此类的模型可以使用异步方法与数据库交互。
    """


class SimulationTaskStatus(enum.Enum):
    """模拟任务的状态枚举。

    定义了模拟任务在其生命周期中可能处于的不同状态。
    这些状态反映了任务从创建到完成的整个过程。

    Attributes:
        PENDING: 任务已创建但尚未计划执行。
        NOT_SCHEDULABLE:
            任务无法调度，可以根据任务执行结果反馈信息，\n
            并由调度提示去干预阻止任务调度。
        SCHEDULED: 任务已被计划在特定时间执行。
        RUNNING: 任务当前正在执行中。
        COMPLETE: 任务已成功完成。
        ERROR: 任务执行过程中遇到错误。
        CANCELLED: 任务被用户或系统取消。
    """

    DEFAULT = "DEFAULT"
    PENDING = "PENDING"
    NOT_SCHEDULABLE = "NOT_SCHEDULABLE"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


class SimulationTaskType(enum.Enum):
    """模拟任务的类型枚举。

    定义了系统支持的不同类型的模拟任务。
    每种类型可能具有特定的处理逻辑和行为。

    Attributes:
        REGULAR: 标准因子模拟回测任务类型。
        SUPER: 超级因子模拟回测任务类型。
    """

    REGULAR = "REGULAR"
    SUPER = "SUPER"


class SimulationTask(Base):
    """模拟任务的数据库实体模型。

    该模型表示系统中的一个模拟计算任务，记录任务的完整生命周期和相关属性。
    模型定义了任务的状态、配置、优先级以及时间戳等信息，用于追踪和管理模拟过程。

    Attributes:
        id (int): 主键，模拟任务的自增标识符。
        type (SimulationTaskType): 任务类型枚举，表示模拟任务的类别。
        settings (dict): JSON 字段，包含任务的配置参数和设置。
        settings_group_key (str): 任务分组键，用于相关任务的分组查询，已建立索引。
        regular (str): 模拟任务中特定用途的字符串字段。
        parent_progress_id (str, optional): 父级进度的标识符，用于多级任务结构。
        child_progress_id (str, optional): 子级进度的标识符，用于多级任务结构。
        status (SimulationTaskStatus): 表示任务当前状态的枚举值。
        alpha_id (str, optional): 关联 alpha 组件的标识符（如适用）。
        priority (int): 任务的执行优先级，值越高优先级越高，默认为 0。
        result (dict, optional): JSON 字段，存储任务执行结果。
        signature (str): 任务的唯一签名，用于标识和去重。
        created_at (datetime): 任务创建时间。
        scheduled_at (datetime, optional): 任务计划执行的时间。
        updated_at (datetime): 任务上次更新的时间。
        deleted_at (datetime, optional): 任务软删除的时间（如适用）。
        description (str, optional): 任务的描述文本。
        tags (str, optional): 与任务关联的标签，用于分类和筛选。
        dependencies (dict, optional): JSON 字段，描述任务的依赖关系。
        completed_at (datetime, optional): 任务完成的时间。
        instrument_type (InstrumentType): 使用的金融工具类型，定义了任务处理的金融产品类别。
        region (Region): Alpha 应用的市场区域，指定了任务所针对的地理市场范围。
        universe (Universe): Alpha 选用的股票范围，确定了模拟中包含的证券集合。
        delay (Delay): 信号延迟时间（单位：天），模拟交易执行中的实际延迟。
        decay (Optional[int]): 信号衰减参数，控制信号随时间的衰减速率。
        neutralization (Neutralization): 中性化方法，用于降低特定风险因子的敞口。
        truncation (Optional[float]): 截断阈值，控制异常值对模型的影响程度。
        pasteurization (Switch): 巴氏化处理方法，用于数据清洗和预处理。
        unit_handling (UnitHandling): 单位处理策略，定义了如何处理不同度量单位的数据。
    """

    __tablename__ = "simulation_tasks"

    # 标识符字段
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    type: Mapped[SimulationTaskType] = mapped_column(
        Enum(SimulationTaskType), nullable=False
    )
    status: Mapped[SimulationTaskStatus] = mapped_column(
        Enum(SimulationTaskStatus), nullable=False, default=SimulationTaskStatus.DEFAULT
    )
    instrument_type: Mapped[InstrumentType] = mapped_column(
        Enum(InstrumentType), nullable=False, default=InstrumentType.DEFAULT
    )
    region: Mapped[Region] = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )
    universe: Mapped[Universe] = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )
    delay: Mapped[Delay] = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )
    neutralization: Mapped[Neutralization] = mapped_column(
        Enum(Neutralization), nullable=False, default=Neutralization.DEFAULT
    )
    pasteurization: Mapped[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )
    unit_handling: Mapped[UnitHandling] = mapped_column(
        Enum(UnitHandling), nullable=False, default=UnitHandling.DEFAULT
    )
    max_trade: Mapped[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )

    # 基础数据类型字段
    settings_group_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    regular: Mapped[str] = mapped_column(String, nullable=False)
    alpha_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    signature: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    _tags: Mapped[Optional[str]] = mapped_column(String, nullable=True, name="tags")
    parent_progress_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    child_progress_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    language: Mapped[str] = mapped_column(String, nullable=False, default="python")
    test_period: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # 数值类型字段
    decay: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    truncation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 布尔类型字段
    visualization: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # JSON类型字段
    result: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    dependencies: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)

    # 时间字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
    )
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        onupdate=func.now(),  # pylint: disable=E1102
        insert_default=func.now(),  # pylint: disable=E1102
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __init__(self, **kwargs: Any) -> None:
        """初始化模拟任务对象，处理特殊属性。

        Args:
            **kwargs: 包含所有模型属性的关键字参数。
        """
        # 处理 tags 属性 (如果存在)
        tags = kwargs.pop("tags", None)

        # 调用父类的 __init__ 处理其他属性
        super().__init__(**kwargs)

        # 手动设置 _tags 属性
        if tags is not None:
            # 过滤空标签，并使用逗号连接
            self._tags = ",".join(
                filter(
                    None,
                    [tag.strip() if isinstance(tag, str) else str(tag) for tag in tags],
                )
            )

    # 验证器部分
    @validates("decay")
    def validate_decay(self, key: str, value: int) -> int:
        """验证 decay 字段的值是否在有效范围内"""
        if not Decay.MIN.value <= value <= Decay.MAX.value:
            raise ValueError(
                f"{key} 必须在 {Decay.MIN.value} 到 {Decay.MAX.value} 之间"
            )
        return value

    @validates("truncation")
    def validate_truncation(self, key: str, value: float) -> float:
        """验证截断阈值是否在合理范围内"""
        if not Truncation.MIN.value <= value <= Truncation.MAX.value:
            raise ValueError(
                f"{key} 必须在 {Truncation.MIN.value} 到 {Truncation.MAX.value} 之间"
            )
        return value

    @hybrid_property
    def tags(self) -> Optional[List[str]]:
        """获取标签列表"""
        if self._tags is None:
            return None
        return [tag.strip() for tag in self._tags.split(",") if tag.strip()]

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: Optional[List[str]]) -> None:
        """设置标签列表"""
        if value is None:
            self._tags = None
        else:
            # 过滤空标签，并使用逗号连接
            self._tags = ",".join(
                filter(
                    None,
                    [
                        tag.strip() if isinstance(tag, str) else str(tag)
                        for tag in value
                    ],
                )
            )

    def add_tag(self, tag: str) -> None:
        """添加单个标签到标签列表"""
        if not tag or not tag.strip():
            return

        current_tags = self.tags or []
        if tag.strip() not in current_tags:
            current_tags.append(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]

    def remove_tag(self, tag: str) -> None:
        """从标签列表中移除单个标签"""
        if not tag or not tag.strip() or not self.tags:
            return

        current_tags = self.tags
        if tag.strip() in current_tags:
            current_tags.remove(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]
