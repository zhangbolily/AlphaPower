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
    __tablename__ = "simulation_task"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type: Column[str] = Column(Enum(SimulationTaskType), nullable=False)
    settings = Column(JSON, nullable=False)
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
