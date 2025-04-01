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
from typing import Dict, Optional

from sqlalchemy import JSON, DateTime, Enum, Integer, String, func
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


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
    """

    __tablename__ = "simulation_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[SimulationTaskType] = mapped_column(
        Enum(SimulationTaskType), nullable=False
    )
    settings: Mapped[Dict] = mapped_column(JSON, nullable=False)
    settings_group_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    regular: Mapped[str] = mapped_column(String, nullable=False)
    parent_progress_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    child_progress_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    status: Mapped[SimulationTaskStatus] = mapped_column(
        Enum(SimulationTaskStatus), nullable=False
    )
    alpha_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    result: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    signature: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, insert_default=func.now
    )
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, onupdate=func.now, insert_default=func.now
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tags: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    dependencies: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
