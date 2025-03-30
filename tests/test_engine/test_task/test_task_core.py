"""
Test the core simulation task functions.
"""

from datetime import datetime
from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest

from alphapower.engine.simulation.task.core import (
    create_simulation_task,
    create_simulation_tasks,
    update_simulation_task_scheduled_info,
    update_simulation_task_status,
)
from alphapower.entity import (
    SimulationTask,
    SimulationTaskStatus,
    SimulationTaskType,
)
from alphapower.client import SimulationSettingsView


@pytest.mark.asyncio
async def test_create_simulation_task() -> None:
    """
    Test the creation of a simulation task.
    """
    session: AsyncMock = AsyncMock()
    settings: SimulationSettingsView = SimulationSettingsView.model_construct(
        region="USA",
        delay=1,
        language="FASTEXPRESSION",
        instrument_type="EQUITY",
    )
    task: SimulationTask = await create_simulation_task(
        session, "regular_1", settings, priority=10
    )

    assert task.type == SimulationTaskType.REGULAR
    assert task.settings_group_key == "USA_1_FASTEXPRESSION_EQUITY"
    assert task.regular == "regular_1"
    assert task.status == SimulationTaskStatus.PENDING
    assert task.priority == 10
    session.add.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_create_simulation_tasks() -> None:
    """
    Test the creation of multiple simulation tasks.
    """
    session: AsyncMock = AsyncMock()
    settings_list: List[SimulationSettingsView] = [
        SimulationSettingsView.model_construct(
            region="USA", delay=1, language="FASTEXPRESSION", instrument_type="EQUITY"
        ),
        SimulationSettingsView.model_construct(
            region="EU", delay=0, language="FASTEXPRESSION", instrument_type="EQUITY"
        ),
    ]
    regular_list: List[str] = ["regular_1", "regular_2"]
    priority_list: List[int] = [10, 20]

    tasks: List[SimulationTask] = await create_simulation_tasks(
        session, regular_list, settings_list, priority_list
    )

    assert len(tasks) == 2
    assert tasks[0].settings_group_key == "USA_1_FASTEXPRESSION_EQUITY"
    assert tasks[1].settings_group_key == "EU_0_FASTEXPRESSION_EQUITY"
    session.add_all.assert_called_once_with(tasks)


@pytest.mark.asyncio
async def test_update_simulation_task_status() -> None:
    """
    Test the update of a simulation task's status.
    """
    session: AsyncMock = AsyncMock()
    task: MagicMock = MagicMock(spec=SimulationTask)
    session.get.return_value = task

    updated_task: SimulationTask = await update_simulation_task_status(
        session, 1, SimulationTaskStatus.COMPLETE
    )

    assert updated_task.status == SimulationTaskStatus.COMPLETE
    session.merge.assert_called_once_with(task)


@pytest.mark.asyncio
async def test_update_simulation_task_scheduled_time() -> None:
    """
    Test the update of a simulation task's scheduled time.
    """
    session: AsyncMock = AsyncMock()
    task: MagicMock = MagicMock(spec=SimulationTask)
    session.get.return_value = task
    scheduled_time: datetime = datetime(2023, 1, 1, 12, 0, 0)

    updated_task: SimulationTask = await update_simulation_task_scheduled_info(
        session, 1, scheduled_time, SimulationTaskStatus.SCHEDULED
    )

    assert updated_task.scheduled_at == scheduled_time
    session.merge.assert_called_once_with(task)
