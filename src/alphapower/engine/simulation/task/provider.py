from typing import List, Optional

from alphapower.engine.simulation.task.core import get_simulation_tasks_by
from alphapower.internal.entity import SimulationTask, SimulationTaskStatus
from sqlalchemy.ext.asyncio import AsyncSession

from .provider_abc import AbstractTaskProvider


class DatabaseTaskProvider(AbstractTaskProvider):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def fetch_tasks(
        self,
        count: int = 10,  # 设置默认值
        priority: Optional[int] = None,
    ) -> List[SimulationTask]:
        """
        从数据库中获取任务。
        """
        return await get_simulation_tasks_by(
            session=self.session,
            status=SimulationTaskStatus.PENDING,
            priority=priority,
            limit=count,
            offset=0,
        )
