import enum

from sqlalchemy import Column, DateTime, Enum, Integer, JSON, String
from sqlalchemy.orm import DeclarativeBase


class SimulationTaskStatus(enum.Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


class SimulationTaskType(enum.Enum):
    REGULAR = "REGULAR"


class SimulationTask(DeclarativeBase):
    """
    SimulationTask 表示用于管理模拟任务的数据库实体。

    属性:
        id (int):
            主键，模拟任务的自增标识符。
        type (SimulationTaskType):
            枚举，表示模拟任务的类型。
        settings (dict):
            JSON 字段，包含任务的配置设置。
        settings_group_key (str):
            用于任务分组相关设置的键，REGION、DELAY、LANGUAGE 和 INSTRUMENT TYPE
            在一个分组中必须相同，已索引以加快查询速度。
        regular (str):
            模拟任务中特定用途的字符串字段。
        parent_progress_id (str, optional):
            父进度的标识符（如果适用）。
        child_progress_id (str, optional):
            子进度的标识符（如果适用）。
        status (SimulationTaskStatus):
            枚举，表示任务的当前状态。
        alpha_id (str, optional):
            关联 alpha 的标识符（如果有）。
        priority (int):
            任务的优先级，默认为 0。
        result (dict, optional):
            JSON 字段，包含模拟任务的结果。
        signature (str):
            模拟任务的唯一签名。
        created_at (datetime):
            任务创建时的时间戳。
        scheduled_at (datetime, optional):
            任务计划运行时的时间戳。
        updated_at (datetime):
            任务最后更新时的时间戳。
        deleted_at (datetime, optional):
            任务被删除时的时间戳（如果适用）。
        description (str, optional):
            模拟任务的描述。
        tags (str, optional):
            与模拟任务关联的标签。
        dependencies (dict, optional):
            JSON 字段，包含任务依赖关系。
        completed_at (datetime, optional):
            任务完成时的时间戳。
    """

    __tablename__ = "simulation_task"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type: Column[str] = Column(Enum(SimulationTaskType), nullable=False)
    settings = Column(JSON, nullable=False)
    settings_group_key = Column(String, nullable=False, index=True)
    regular = Column(String, nullable=False)
    parent_progress_id = Column(String, nullable=True)
    child_progress_id = Column(String, nullable=True)
    status: Column[str] = Column(Enum(SimulationTaskStatus), nullable=False)
    alpha_id = Column(String, nullable=True)
    priority = Column(Integer, nullable=False, default=0)
    result = Column(JSON, nullable=True)
    signature = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    scheduled_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    description = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    dependencies = Column(JSON, nullable=True)
    completed_at = Column(DateTime, nullable=True)
