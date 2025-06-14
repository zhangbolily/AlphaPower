"""
Test the core simulation task functions.
"""

from datetime import datetime
from typing import List, Optional
from unittest.mock import AsyncMock, patch

import pytest

from alphapower.client import SimulationSettingsView
from alphapower.constants import (
    AlphaType,
    CodeLanguage,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.engine.simulation.task.core import (
    create_simulation_task,
    create_simulation_tasks,
    update_simulation_task_scheduled_info,
    update_simulation_task_status,
)
from alphapower.entity import (
    SimulationTask,
    SimulationTaskStatus,
)


@pytest.mark.asyncio
async def test_create_simulation_task() -> None:
    """
    Test the creation of a simulation task.
    """
    session: AsyncMock = AsyncMock()
    settings: SimulationSettingsView = SimulationSettingsView.model_construct(
        region=Region.USA.value,
        delay=Delay.ONE.value,
        language=CodeLanguage.FASTEXPR.value,
        instrument_type=InstrumentType.EQUITY.value,
        universe=Universe.TOP1000.value,
        neutralization=Neutralization.INDUSTRY.value,
        pasteurization=Switch.ON.value,
        unit_handling=UnitHandling.VERIFY.value,
        max_trade=Switch.OFF.value,
        decay=10,
        truncation=0.5,
        visualization=False,
        test_period="2020-01-01:2021-01-01",
    )
    task: SimulationTask = await create_simulation_task(
        session, "regular_1", settings, priority=10
    )

    assert task.type == AlphaType.REGULAR
    assert task.settings_group_key == "USA_1_FASTEXPR_EQUITY"
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
            region=Region.USA.name,
            delay=Delay.ONE.value,
            language=CodeLanguage.FASTEXPR.value,
            instrument_type=InstrumentType.EQUITY.value,
            universe=Universe.TOP3000.value,
            neutralization=Neutralization.INDUSTRY.value,
            pasteurization=Switch.ON.value,
            unit_handling=UnitHandling.VERIFY.value,
            max_trade=Switch.OFF.value,
            decay=10,
            truncation=0.5,
            visualization=False,
            test_period="2020-01-01:2021-01-01",
        ),
        SimulationSettingsView.model_construct(
            region=Region.EUR.name,
            delay=Delay.ZERO.value,
            language=CodeLanguage.FASTEXPR.value,
            instrument_type=InstrumentType.EQUITY.value,
            universe=Universe.TOP1200.value,
            neutralization=Neutralization.INDUSTRY.value,
            pasteurization=Switch.ON.value,
            unit_handling=UnitHandling.VERIFY.value,
            max_trade=Switch.OFF.value,
            decay=10,
            truncation=0.5,
            visualization=False,
            test_period="2020-01-01:2021-01-01",
        ),
    ]
    regular_list: List[str] = ["regular_1", "regular_2"]
    priority_list: List[int] = [10, 20]
    tags_list: List[Optional[List[str]]] = [["tag1"], ["tag2", "tag3"]]

    tasks: List[SimulationTask] = await create_simulation_tasks(
        session, regular_list, settings_list, priority_list, tags_list
    )

    assert len(tasks) == 2
    assert tasks[0].settings_group_key == "USA_1_FASTEXPR_EQUITY"
    assert tasks[1].settings_group_key == "EUR_0_FASTEXPR_EQUITY"
    session.add_all.assert_called_once_with(tasks)


@pytest.mark.asyncio
async def test_update_simulation_task_status() -> None:
    """
    Test the update of a simulation task's status.
    """
    session: AsyncMock = AsyncMock()
    task: AsyncMock = AsyncMock(
        spec=SimulationTask, id=1, status=SimulationTaskStatus.PENDING
    )

    with patch(
        "alphapower.dal.simulation.SimulationTaskDAL.find_one_by"
    ) as mock_find_one_by:
        mock_find_one_by.return_value = task
        updated_task: SimulationTask = await update_simulation_task_status(
            session, 1, SimulationTaskStatus.COMPLETE
        )

        assert updated_task.status == SimulationTaskStatus.COMPLETE
        session.flush.assert_called_once()


@pytest.mark.asyncio
async def test_update_simulation_task_scheduled_time() -> None:
    """
    Test the update of a simulation task's scheduled time.
    """
    session: AsyncMock = AsyncMock()
    task: AsyncMock = AsyncMock(
        spec=SimulationTask, id=1, status=SimulationTaskStatus.PENDING
    )
    scheduled_time: datetime = datetime(2023, 1, 1, 12, 0, 0)

    with patch(
        "alphapower.dal.simulation.SimulationTaskDAL.find_one_by"
    ) as mock_find_one_by:
        mock_find_one_by.return_value = task
        updated_task: SimulationTask = await update_simulation_task_scheduled_info(
            session, 1, scheduled_time, SimulationTaskStatus.SCHEDULED
        )

        assert updated_task.scheduled_at == scheduled_time
        assert updated_task.status == SimulationTaskStatus.SCHEDULED
        session.flush.assert_called_once()
