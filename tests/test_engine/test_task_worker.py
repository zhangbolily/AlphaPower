"""
测试模块: test_task_worker

此模块包含针对 Worker 类的单元测试和集成测试，主要用于验证其在不同场景下的行为和功能。
测试使用了 pytest 框架和 unittest.mock 库，支持异步测试和模拟对象的创建。

模块功能:
- 测试 Worker 类的初始化逻辑，包括无效客户端、未认证客户端和无效用户角色的处理。
- 验证 Worker 在不同用户角色 (USER 和 CONSULTANT) 下的任务处理逻辑。
- 测试 Worker 的调度器功能，包括任务调度和任务取消。
- 模拟多任务处理场景，验证部分任务失败时的处理逻辑。
- 测试 Worker 的关闭逻辑，确保任务正确取消或完成。

使用的库:
- pytest: 用于编写和运行测试。
- unittest.mock: 用于创建模拟对象和异步方法。
- asyncio: 用于处理异步操作。
- alphapower.client: 包含 Worker 类依赖的客户端和视图模型。
- alphapower.constants: 定义了用户角色常量。
- alphapower.engine.simulation.task.scheduler: 提供任务调度器接口。
- alphapower.engine.simulation.task.worker: 被测试的 Worker 类。
- alphapower.entity: 定义了任务和任务状态的实体类。

测试范围:
- Worker 类的初始化和运行逻辑。
- 单任务和多任务的处理与取消。
- 调度器的设置和任务调度。
- 异常处理和错误场景的覆盖。
"""

import asyncio
from typing import AsyncGenerator, Callable, List
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from alphapower.client import (
    AuthenticationView,
    MultiSimulationResultView,
    SimulationSettingsView,
    SingleSimulationResultView,
    WorldQuantClient,
)
from alphapower.constants import (
    ROLE_CONSULTANT,
    ROLE_USER,
    AlphaType,
    Database,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Switch,
    UnitHandling,
    Universe,
)
from alphapower.engine.simulation.task.core import create_simulation_task
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.engine.simulation.task.worker import Worker
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="session")
async def fixture_session() -> AsyncGenerator[AsyncSession, None]:
    """创建数据库会话用于测试。

    创建与真实数据库的连接会话，用于测试实体类的数据库操作。
    测试完成后会自动清理会话。

    Yields:
        AsyncSession: SQLAlchemy 异步会话对象。
    """
    async with get_db_session(Database.SIMULATION) as session:
        yield session
        # 注意：在生产环境测试中可能需要更复杂的数据清理策略
        # 当前会话在上下文管理器结束时会自动回滚未提交的更改


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
            type=AlphaType.REGULAR,
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
            type=AlphaType.REGULAR,
            status=SimulationTaskStatus.COMPLETE.value,
        ),
        0.0,
    )
    client.get_single_simulation_progress = AsyncMock()
    client.get_single_simulation_progress.return_value = (
        True,
        SingleSimulationResultView(
            id="child_id_0",
            type=AlphaType.REGULAR,
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


async def simulate_worker_shutdown(
    worker: Worker, delay: float, cancel_tasks: bool
) -> None:
    """
    通用辅助函数：模拟关闭工作者的异步函数。
    """
    await asyncio.sleep(delay)
    await worker.stop(cancel_tasks=cancel_tasks)


@pytest.mark.asyncio
async def test_run_without_scheduler_raises_exception(user_worker: Worker) -> None:
    """
    测试 Worker 在未设置调度器时运行会抛出异常。
    """
    setattr(user_worker, "_scheduler", None)  # 使用 setattr 方法访问受保护成员
    with pytest.raises(Exception, match="调度器未设置，无法执行工作"):
        await user_worker.run()


@pytest.mark.asyncio
async def test_run_with_scheduler_executes_tasks(user_worker: Worker) -> None:
    """
    测试 Worker 在设置调度器时能够正常运行并执行任务。
    """
    mock_scheduler: AsyncMock = AsyncMock()
    mock_scheduler.schedule.return_value = []
    setattr(
        user_worker, "_scheduler", mock_scheduler
    )  # 使用 setattr 方法访问受保护成员

    shutdown_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(user_worker, 1, False)
    )
    await user_worker.run()
    await shutdown_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_process_single_simulation_task_executes_correctly(
    user_worker: Worker,
) -> None:
    """
    测试 Worker 处理单个模拟任务的逻辑是否正确。
    """
    task: MagicMock = MagicMock(spec=SimulationTask)
    task.id = 1
    setattr(user_worker, "_shutdown_flag", True)  # 使用 setattr 方法访问受保护成员
    m_func: Callable = getattr(user_worker, "_process_single_simulation_task")
    await m_func(task)  # 假设此方法为私有且无法更改


@pytest.mark.asyncio
async def test_process_multi_simulation_task_executes_correctly(
    user_worker: Worker,
) -> None:
    """
    测试 Worker 处理多个模拟任务的逻辑是否正确。
    """
    setattr(
        user_worker, "_user_role", ROLE_CONSULTANT
    )  # 使用 setattr 方法访问受保护成员
    tasks: List[SimulationTask] = [
        SimulationTask(
            type=AlphaType.REGULAR,
            status=SimulationTaskStatus.PENDING,
            regular="rank(-returns)",
            settings_group_key="test_group",
            signature="test_signature",
            region=Region.USA,
            delay=Delay.ONE,  # 修正：从 Delay.D1 修改为 Delay.ONE
            instrument_type=InstrumentType.EQUITY,
            universe=Universe.TOP500,  # 修正：从 Universe.ALL 修改为 Universe.TOP500
            neutralization=Neutralization.INDUSTRY,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,  # 修正：从 UnitHandling.RAW 修改为 UnitHandling.VERIFY
            max_trade=Switch.OFF,
            language=RegularLanguage.FASTEXPR,  # 修正：从 "FASTEXPRESSION" 修改为 "python"
            visualization=False,
        )
        for _ in range(2)
    ]  # 使用 SimulationTask 实例
    setattr(user_worker, "_shutdown_flag", True)  # 使用 setattr 方法访问受保护成员
    m_func: Callable = getattr(user_worker, "_process_multi_simulation_task")
    await m_func(tasks)  # 假设此方法为私有且无法更改


@pytest.mark.asyncio
async def test_stop_worker_cancels_tasks(user_worker: Worker) -> None:
    """
    测试 Worker 停止时是否正确取消任务。
    """
    setattr(
        user_worker,
        "_post_handler_tasks",
        [asyncio.create_task(asyncio.sleep(1))],
    )  # 使用 setattr 方法访问受保护成员
    await user_worker.stop(cancel_tasks=True)
    assert (
        getattr(user_worker, "_shutdown_flag") is True
    )  # 使用 getattr 方法访问受保护成员
    assert (
        getattr(user_worker, "_is_task_cancel_requested") is True
    )  # 使用 getattr 方法访问受保护成员


@pytest.mark.asyncio
async def test_handle_single_simulation_task(
    user_worker: Worker, session: AsyncSession
) -> None:
    """
    测试 Worker 能否正确处理单个模拟任务。
    """
    settings: SimulationSettingsView = SimulationSettingsView.model_construct(
        region=Region.USA.value,
        delay=Delay.ONE.value,
        language=RegularLanguage.FASTEXPR.value,
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
    task0: SimulationTask = await create_simulation_task(
        session, "regular_1", settings, priority=10
    )
    await session.close_all()
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0]

    await user_worker.set_scheduler(mock_scheduler)

    stop_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(user_worker, 2, False)
    )
    await user_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_handle_multi_simulation_tasks(consultant_worker: Worker) -> None:
    """
    测试 Worker 能否正确处理多个模拟任务。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )
    task1: SimulationTask = SimulationTask(
        id=2,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0, task1]

    await consultant_worker.set_scheduler(mock_scheduler)

    stop_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(consultant_worker, 2, False)
    )
    await consultant_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_single_simulation_task(
    user_worker: Worker, mock_user_client: MagicMock
) -> None:
    """
    测试 Worker 能否正确取消单个模拟任务。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0]

    await user_worker.set_scheduler(mock_scheduler)

    stop_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(user_worker, 1, True)
    )
    await user_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()
    mock_user_client.create_single_simulation.assert_called_once()
    mock_user_client.delete_simulation.assert_called_once()
    mock_user_client.get_single_simulation_progress.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_multi_simulation_tasks(
    consultant_worker: Worker, mock_consultant_client: MagicMock
) -> None:
    """
    测试 Worker 能否正确取消多个模拟任务。
    """
    task0: SimulationTask = SimulationTask(
        id=1,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )
    task1: SimulationTask = SimulationTask(
        id=2,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )
    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task0, task1]

    await consultant_worker.set_scheduler(mock_scheduler)

    stop_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(consultant_worker, 1, True)
    )
    await consultant_worker.run()
    await stop_task
    mock_scheduler.schedule.assert_called_once()
    mock_consultant_client.create_multi_simulation.assert_called_once()
    mock_consultant_client.delete_simulation.assert_called_once()
    mock_consultant_client.get_multi_simulation_progress.assert_not_called()


@pytest.mark.asyncio
async def test_worker_initialization_with_invalid_client_raises_error() -> None:
    """
    测试 Worker 初始化时传入无效客户端会抛出异常。
    """
    mock_client = MagicMock(spec=str)
    with pytest.raises(
        ValueError, match="Client must be an instance of WorldQuantClient."
    ):
        Worker(client=mock_client)


@pytest.mark.asyncio
async def test_worker_initialization_with_unauthenticated_client_raises_error() -> None:
    """
    测试 Worker 初始化时传入未认证客户端会抛出异常。
    """
    mock_client = MagicMock(spec=WorldQuantClient)
    mock_client.authentication_info = None
    with pytest.raises(
        ValueError, match="Client must be authenticated with valid credentials."
    ):
        Worker(client=mock_client)


@pytest.mark.asyncio
async def test_worker_initialization_with_invalid_user_role_raises_error() -> None:
    """
    测试 Worker 初始化时传入无效用户角色会抛出异常。
    """
    mock_client = MagicMock(spec=WorldQuantClient)
    mock_client.authentication_info = MagicMock(
        spec=AuthenticationView, permissions=["INVALID_ROLE"]
    )
    with pytest.raises(
        ValueError, match="Client must have a valid user role \\(CONSULTANT or USER\\)."
    ):
        Worker(client=mock_client)


@pytest.mark.asyncio
async def test_cancel_task_failure_handling(
    user_worker: Worker, mock_user_client: MagicMock
) -> None:
    """
    测试 Worker 在任务取消失败时的处理逻辑。
    """
    setattr(user_worker, "_shutdown_flag", True)
    setattr(user_worker, "_is_task_cancel_requested", True)
    mock_user_client.delete_simulation.return_value = False

    task = SimulationTask(
        id=1,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )

    test_func = getattr(user_worker, "_cancel_task_if_possible")
    result = await test_func(progress_id="invalid_progress_id", tasks=[task])
    assert result is False
    mock_user_client.delete_simulation.assert_called_once_with(
        progress_id="invalid_progress_id"
    )


@pytest.mark.asyncio
async def test_do_work_with_unknown_user_role_raises_error(
    mock_user_client: WorldQuantClient,
) -> None:
    """
    测试 Worker 在处理未知用户角色时会抛出异常。
    """
    user_worker: Worker = Worker(client=mock_user_client)
    task_0: SimulationTask = SimulationTask(
        id=1,
        type=AlphaType.REGULAR,
        status=SimulationTaskStatus.PENDING,
        regular="rank(-returns)",
        settings_group_key="test_group",
        signature="test_signature",
        region=Region.USA,
        delay=Delay.ONE,  # 修正
        instrument_type=InstrumentType.EQUITY,
        universe=Universe.TOP500,  # 修正
        neutralization=Neutralization.INDUSTRY,
        pasteurization=Switch.ON,
        unit_handling=UnitHandling.VERIFY,  # 修正
        max_trade=Switch.OFF,
        language=RegularLanguage.EXPRESSION,  # 修正
        visualization=False,
    )

    mock_scheduler: AsyncMock = AsyncMock(spec=PriorityScheduler)
    mock_scheduler.schedule.return_value = [task_0]
    setattr(user_worker, "_shutdown_flag", False)
    setattr(user_worker, "_user_role", "UNKNOWN_ROLE")
    await user_worker.set_scheduler(mock_scheduler)

    stop_task: asyncio.Task = asyncio.create_task(
        simulate_worker_shutdown(user_worker, 1, False)
    )

    with pytest.raises(Exception, match="未知用户角色 UNKNOWN_ROLE，无法处理任务"):
        await user_worker.run()
    await stop_task


@pytest.mark.asyncio
async def test_handle_multi_task_completion_with_partial_failures(
    consultant_worker: Worker, mock_consultant_client: MagicMock
) -> None:
    """
    测试 Worker 在处理多个任务完成时，部分任务失败的情况。
    """
    tasks = [
        SimulationTask(
            id=1,
            type=AlphaType.REGULAR,
            status=SimulationTaskStatus.PENDING,
            regular="rank(-returns)",
            signature="test_signature",
            settings_group_key="test_group",
            region=Region.USA,
            delay=Delay.ONE,  # 修正
            instrument_type=InstrumentType.EQUITY,
            universe=Universe.TOP500,  # 修正
            neutralization=Neutralization.INDUSTRY,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,  # 修正
            max_trade=Switch.OFF,
            language=RegularLanguage.EXPRESSION,  # 修正
            visualization=False,
        ),
        SimulationTask(
            id=2,
            type=AlphaType.REGULAR,
            status=SimulationTaskStatus.PENDING,
            regular="rank(-returns)",
            signature="test_signature",
            settings_group_key="test_group",
            region=Region.USA,
            delay=Delay.ONE,  # 修正
            instrument_type=InstrumentType.EQUITY,
            universe=Universe.TOP500,  # 修正
            neutralization=Neutralization.INDUSTRY,
            pasteurization=Switch.ON,
            unit_handling=UnitHandling.VERIFY,  # 修正
            max_trade=Switch.OFF,
            language=RegularLanguage.EXPRESSION,  # 修正
            visualization=False,
        ),
    ]
    result = MultiSimulationResultView(
        children=["child_id_0", "child_id_1"],
        type=AlphaType.REGULAR.value,
        status=SimulationTaskStatus.COMPLETE.value,
    )

    mock_consultant_client.get_multi_simulation_child_result = AsyncMock()
    mock_consultant_client.get_multi_simulation_child_result.side_effect = [
        (
            True,
            SingleSimulationResultView(
                id="child_id_0",
                type=AlphaType.REGULAR.value,
                status=SimulationTaskStatus.COMPLETE.value,
            ),
        ),
        (False, None),  # 模拟获取子任务结果失败
    ]

    test_func = getattr(consultant_worker, "_handle_multi_task_completion")
    await test_func(tasks, result)

    assert len(getattr(consultant_worker, "_post_handler_futures")) == 2
    await asyncio.gather(*getattr(consultant_worker, "_post_handler_futures"))

    mock_consultant_client.get_multi_simulation_child_result.assert_any_call(
        child_progress_id="child_id_0"
    )
    mock_consultant_client.get_multi_simulation_child_result.assert_any_call(
        child_progress_id="child_id_1"
    )
