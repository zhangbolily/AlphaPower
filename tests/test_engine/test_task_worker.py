"""
测试 Worker 类的功能。
该测试文件使用 pytest 框架和 unittest.mock 库来创建模拟对象和异步测试。
"""

import asyncio
from typing import Callable, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from alphapower.client import (
    AuthenticationView,
    MultiSimulationResultView,
    SingleSimulationResultView,
    WorldQuantClient,
)
from alphapower.constants import ROLE_CONSULTANT, ROLE_USER
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.engine.simulation.task.worker import Worker
from alphapower.entity import SimulationTask, SimulationTaskStatus, SimulationTaskType


@pytest.fixture(name="mock_user_client")
def magic_mock_client() -> MagicMock:
    """
    创建一个模拟的 WorldQuantClient 实例。
    """
    client: MagicMock = MagicMock(spec=WorldQuantClient)
    client.authentication_info = MagicMock(
        spec=AuthenticationView, permissions=[ROLE_USER]
    )
    client.create_single_simulation = AsyncMock()
    client.create_single_simulation.return_value = (True, "progress_id_0", 2.5)
    client.get_single_simulation_progress = AsyncMock()
    client.get_single_simulation_progress.return_value = (
        True,
        SingleSimulationResultView(
            id="progress_id_0",
            type=SimulationTaskType.REGULAR.value,
            status=SimulationTaskStatus.COMPLETE.value,
        ),
        0.0,
    )
    client.delete_simulation = AsyncMock()
    client.delete_simulation.return_value = True
    return client


@pytest.fixture(name="mock_consultant_client")
def magic_mock_consultant_client() -> MagicMock:
    """
    创建一个模拟的 WorldQuantClient 实例。
    """
    client: MagicMock = MagicMock(spec=WorldQuantClient)
    client.authentication_info = MagicMock(
        spec=AuthenticationView, permissions=[ROLE_CONSULTANT]
    )
    client.create_multi_simulation = AsyncMock()
    client.create_multi_simulation.return_value = (True, "progress_id_1", 2.5)
    client.get_multi_simulation_progress = AsyncMock()
    client.get_multi_simulation_progress.return_value = (
        True,
        MultiSimulationResultView(
            children=["child_id_0", "child_id_1"],
            type=SimulationTaskType.REGULAR.value,
            status=SimulationTaskStatus.COMPLETE.value,
        ),
        0.0,
    )
    client.get_single_simulation_progress = AsyncMock()
    client.get_single_simulation_progress.return_value = (
        True,
        SingleSimulationResultView(
            id="child_id_0",
            type=SimulationTaskType.REGULAR.value,
            status=SimulationTaskStatus.COMPLETE.value,
        ),
        0.0,
    )
    client.delete_simulation = AsyncMock()
    client.delete_simulation.return_value = True
    client.delete_multi_simulation = AsyncMock()
    client.delete_multi_simulation.return_value = True
    return client


@pytest.fixture(name="user_worker")
def fixture_user_worker(mock_user_client: MagicMock) -> Worker:
    """
    创建一个 Worker 实例，使用模拟的 WorldQuantClient。
    """
    return Worker(client=mock_user_client)


@pytest.fixture(name="consultant_worker")
def fixture_consultant_worker(mock_consultant_client: MagicMock) -> Worker:
    """
    创建一个 Worker 实例，使用模拟的 WorldQuantClient。
    """
    return Worker(client=mock_consultant_client)


@pytest.mark.asyncio
async def test_worker_run_with_no_scheduler(user_worker: Worker) -> None:
    """
    测试在未设置调度器的情况下运行工作者。
    """
    setattr(user_worker, "_scheduler", None)  # 使用 setattr 方法访问受保护成员
    with pytest.raises(Exception, match="调度器未设置，无法执行工作"):
        await user_worker.run()


@pytest.mark.asyncio
async def test_worker_run_with_scheduler(user_worker: Worker) -> None:
    """
    测试在设置调度器的情况下运行工作者。
    """
    mock_scheduler: AsyncMock = AsyncMock()
    mock_scheduler.schedule.return_value = []
    setattr(
        user_worker, "_scheduler", mock_scheduler
    )  # 使用 setattr 方法访问受保护成员

    async def shutdown() -> None:
        """
        模拟关闭工作者的异步函数。
        """
        await asyncio.sleep(1)
        setattr(user_worker, "_shutdown", True)

    shutdown_task: asyncio.Task = asyncio.create_task(shutdown())
    await user_worker.run()
    await shutdown_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_handle_single_simulation_task(user_worker: Worker) -> None:
    """
    测试处理单个模拟任务的方法。
    """
    task: MagicMock = MagicMock(spec=SimulationTask)
    task.id = 1
    setattr(user_worker, "_shutdown", True)  # 使用 setattr 方法访问受保护成员
    m_func: Callable = getattr(user_worker, "_handle_single_simulation_task")
    await m_func(task)  # 假设此方法为私有且无法更改


@pytest.mark.asyncio
async def test_handle_multi_simulation_task(user_worker: Worker) -> None:
    """
    测试处理多个模拟任务的方法。
    """
    setattr(
        user_worker, "_user_role", ROLE_CONSULTANT
    )  # 使用 setattr 方法访问受保护成员
    tasks: List[SimulationTask] = [
        SimulationTask() for _ in range(2)
    ]  # 使用 SimulationTask 实例
    setattr(user_worker, "_shutdown", True)  # 使用 setattr 方法访问受保护成员
    m_func: Callable = getattr(user_worker, "_handle_multi_simulation_task")
    await m_func(tasks)  # 假设此方法为私有且无法更改


@pytest.mark.asyncio
async def test_worker_stop(user_worker: Worker) -> None:
    """
    测试停止工作者的方法。
    """
    setattr(
        user_worker,
        "_post_handler_tasks",
        [asyncio.create_task(asyncio.sleep(1))],
    )  # 使用 setattr 方法访问受保护成员
    await user_worker.stop(cancel_tasks=True)
    assert getattr(user_worker, "_shutdown") is True  # 使用 getattr 方法访问受保护成员
    assert (
        getattr(user_worker, "_cancel_tasks") is True
    )  # 使用 getattr 方法访问受保护成员


@pytest.mark.asyncio
async def test_single_simulation_task(user_worker: Worker) -> None:
    """
    测试单个模拟任务的处理。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0]

    await user_worker.set_scheduler(mock_scheduler)

    async def stop() -> None:
        """
        模拟关闭工作者的异步函数。
        """
        await asyncio.sleep(2)
        await user_worker.stop(cancel_tasks=False)

    stop_task: asyncio.Task = asyncio.create_task(stop())
    await user_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_multi_simulation_task(consultant_worker: Worker) -> None:
    """
    测试多个模拟任务的处理。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    task1: SimulationTask = SimulationTask(
        id=2,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0, task1]

    await consultant_worker.set_scheduler(mock_scheduler)

    async def stop() -> None:
        """
        模拟关闭工作者的异步函数。
        """
        await asyncio.sleep(2)
        await consultant_worker.stop(cancel_tasks=False)

    stop_task: asyncio.Task = asyncio.create_task(stop())
    await consultant_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_single_simulation_task_cancel(
    user_worker: Worker, mock_user_client: MagicMock
) -> None:
    """
    测试取消单个模拟任务的处理。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0]

    await user_worker.set_scheduler(mock_scheduler)

    async def stop() -> None:
        """
        模拟关闭工作者的异步函数。
        """
        await asyncio.sleep(1)
        await user_worker.stop(cancel_tasks=True)

    stop_task: asyncio.Task = asyncio.create_task(stop())
    await user_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()
    mock_user_client.create_single_simulation.assert_called_once()
    mock_user_client.delete_simulation.assert_called_once()
    mock_user_client.get_single_simulation_progress.assert_not_called()


@pytest.mark.asyncio
async def test_multi_simulation_task_cancel(
    consultant_worker: Worker, mock_consultant_client: MagicMock
) -> None:
    """
    测试取消多个模拟任务的处理。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    task1: SimulationTask = SimulationTask(
        id=2,
        type=SimulationTaskType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        settings={},
        regular="rank(-returns)",
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0, task1]

    await consultant_worker.set_scheduler(mock_scheduler)

    async def stop() -> None:
        """
        模拟关闭工作者的异步函数。
        """
        await asyncio.sleep(1)
        await consultant_worker.stop(cancel_tasks=True)

    stop_task: asyncio.Task = asyncio.create_task(stop())
    await consultant_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()
    mock_consultant_client.create_multi_simulation.assert_called_once()
    mock_consultant_client.delete_simulation.assert_called_once()
    mock_consultant_client.get_multi_simulation_progress.assert_not_called()
