"""
测试 WorkerPool 类的功能和行为。

该测试模块包含针对 WorkerPool 类的单元测试和集成测试，验证其在不同场景下的行为、
资源管理能力和与其他组件的交互。测试内容包括：

- 工作池初始化及配置设置
- 工作者的创建、管理和回收
- 扩容和缩容功能
- 工作池状态报告和监控
- 工作者健康检查和自动恢复
- 任务回调处理
- 异常情况处理

使用 pytest 异步测试框架和 unittest.mock 库来隔离测试环境并模拟依赖组件。
"""

import asyncio
import time
from datetime import datetime
from typing import Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alphapower.client import SingleSimulationResultView, WorldQuantClient
from alphapower.constants import ROLE_USER, AlphaType
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.engine.simulation.task.worker import AbstractWorker, Worker
from alphapower.engine.simulation.task.worker_pool import WorkerPool
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.logging import setup_logging

# 禁用日志以避免测试输出混乱
logger = setup_logging(__name__)

# pylint: disable=protected-access


@pytest.fixture(name="mock_client")
def fixture_mock_client() -> MagicMock:
    """
    创建一个模拟的 WorldQuantClient 实例。
    """
    client: MagicMock = MagicMock(spec=WorldQuantClient)
    client.authentication_info = MagicMock(permissions=[ROLE_USER])
    client.create_single_simulation = AsyncMock(return_value=(True, "progress_id", 0.1))
    client.get_single_simulation_progress = AsyncMock(
        return_value=(
            True,
            SingleSimulationResultView(
                id="progress_id",
                status=SimulationTaskStatus.COMPLETE.value,
                alpha=None,
                type=AlphaType.REGULAR,
            ),
            0.0,
        )
    )
    return client


@pytest.fixture(name="mock_client_factory")
def fixture_mock_client_factory(mock_client: MagicMock) -> Callable[[], MagicMock]:
    """
    创建一个返回模拟 WorldQuantClient 的工厂函数。
    """
    return lambda: mock_client


@pytest.fixture(name="mock_scheduler")
def fixture_mock_scheduler() -> AsyncMock:
    """
    创建一个模拟的调度器。
    """
    scheduler = AsyncMock(spec=PriorityScheduler)
    scheduler.schedule = AsyncMock(return_value=[])
    return scheduler


@pytest.fixture(name="mock_task")
def fixture_mock_task() -> SimulationTask:
    """
    创建一个模拟的模拟任务。
    """
    task = MagicMock(spec=SimulationTask)
    task.id = 1
    task.status = SimulationTaskStatus.PENDING
    task.scheduled_at = datetime.now()
    return task


@pytest.fixture(name="worker_pool")
def fixture_worker_pool(
    mock_scheduler: AsyncMock, mock_client_factory: Callable[[], MagicMock]
) -> WorkerPool:
    """
    创建一个 WorkerPool 实例，用于测试。
    """
    return WorkerPool(
        scheduler=mock_scheduler,
        client_factory=mock_client_factory,
        initial_workers=1,
        dry_run=True,
        worker_timeout=5,  # 设置较短的超时以加速测试
    )


@pytest.mark.asyncio
async def test_worker_pool_initialization(
    mock_scheduler: AsyncMock, mock_client_factory: Callable[[], MagicMock]
) -> None:
    """
    测试工作池初始化过程。
    """
    # 测试正常初始化
    pool = WorkerPool(
        scheduler=mock_scheduler,
        client_factory=mock_client_factory,
        initial_workers=2,
        dry_run=True,
    )

    # 验证初始配置
    assert pool._scheduler == mock_scheduler
    assert pool._client_factory == mock_client_factory
    assert pool._initial_workers == 2
    assert pool._dry_run is True
    assert not pool._running
    assert len(pool._workers) == 0

    # 测试工作者数量边界值
    pool_zero = WorkerPool(
        scheduler=mock_scheduler,
        client_factory=mock_client_factory,
        initial_workers=0,  # 不合法值
        dry_run=True,
    )
    assert pool_zero._initial_workers == 1  # 应自动纠正为1


@pytest.mark.asyncio
async def test_worker_pool_start(worker_pool: WorkerPool) -> None:
    """
    测试工作池的启动功能。
    """
    # 模拟 _create_worker 方法，避免实际创建客户端
    mock_worker = AsyncMock(spec=AbstractWorker)
    with patch.object(
        worker_pool, "_create_worker", AsyncMock(return_value=mock_worker)
    ) as mock_create_worker:
        # 测试启动
        await worker_pool.start()

        # 验证状态
        assert worker_pool._running is True
        assert worker_pool._started_at is not None
        assert len(worker_pool._workers) == 1

        # 验证工作者创建和配置
        mock_create_worker.assert_called_once()
        mock_worker.run.assert_called_once()

        # 验证健康检查任务已启动
        assert worker_pool._health_check_task is not None

        # 测试重复启动
        await worker_pool.start()
        # 应该仍然只有一次创建调用
        mock_create_worker.assert_called_once()

        # 清理
        await worker_pool.stop()


@pytest.mark.asyncio
async def test_worker_pool_stop(worker_pool: WorkerPool) -> None:
    """
    测试工作池的停止功能。
    """
    mock_worker = AsyncMock(spec=AbstractWorker)

    with patch.object(
        worker_pool, "_create_worker", AsyncMock(return_value=mock_worker)
    ):
        # 启动工作池
        await worker_pool.start()
        assert worker_pool._running is True

        # 停止工作池
        await worker_pool.stop()

        # 验证状态
        assert worker_pool._running is False
        assert worker_pool._health_check_task is None
        assert len(worker_pool._workers) == 0
        assert len(worker_pool._worker_tasks) == 0

        # 验证工作者调用
        mock_worker.stop.assert_called_once_with(cancel_tasks=False)

        # 测试未启动状态下停止
        await worker_pool.stop()  # 不应引发异常


@pytest.mark.asyncio
async def test_worker_pool_scale_up(worker_pool: WorkerPool) -> None:
    """
    测试工作池的扩容功能。
    """
    mock_worker1: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_worker2: AsyncMock = AsyncMock(spec=AbstractWorker)

    # 模拟连续创建两个不同的工作者
    with patch.object(
        worker_pool,
        "_create_worker",
        new_callable=AsyncMock,
        side_effect=[mock_worker1, mock_worker2],
    ) as mock_create_worker:
        # 启动工作池且初始不创建工作者
        worker_pool._initial_workers = 0
        await worker_pool.start()
        mock_create_worker.reset_mock()  # 重置调用计数

        # 测试扩容
        await worker_pool.scale_up(2)

        # 验证工作者创建
        assert mock_create_worker.call_count == 2
        assert len(worker_pool._workers) == 2
        assert mock_worker1 in worker_pool._workers
        assert mock_worker2 in worker_pool._workers

        # 验证工作者任务启动
        mock_worker1.run.assert_called_once()
        mock_worker2.run.assert_called_once()

        # 测试无效扩容数量
        await worker_pool.scale_up(0)
        # 工作者数量应保持不变
        assert len(worker_pool._workers) == 2

        # 测试未运行状态下扩容
        await worker_pool.stop()
        await worker_pool.scale_up(1)
        # 工作者数量不应变化
        assert len(worker_pool._workers) == 0


@pytest.mark.asyncio
async def test_worker_pool_scale_down(worker_pool: WorkerPool) -> None:
    """
    测试工作池的缩容功能。
    """
    mock_workers = [AsyncMock(spec=AbstractWorker) for _ in range(3)]

    # 模拟创建多个工作者
    with patch.object(
        worker_pool, "_create_worker", AsyncMock(side_effect=mock_workers)
    ):
        # 启动工作池
        worker_pool._initial_workers = 3
        await worker_pool.start()
        assert len(worker_pool._workers) == 3

        # 测试缩容
        await worker_pool.scale_down(1)

        # 验证结果
        assert len(worker_pool._workers) == 2
        # 最旧的工作者应被移除
        assert mock_workers[0] not in worker_pool._workers
        assert mock_workers[1] in worker_pool._workers
        assert mock_workers[2] in worker_pool._workers

        # 验证工作者停止调用
        mock_workers[0].stop.assert_called_once_with(cancel_tasks=True)

        # 测试缩容全部（应保留一个）
        await worker_pool.scale_down(2)
        assert len(worker_pool._workers) == 1
        # 最新的工作者应保留
        assert mock_workers[2] in worker_pool._workers

        # 测试无效缩容数量
        await worker_pool.scale_down(0)
        assert len(worker_pool._workers) == 1  # 不变

        # 测试未运行状态下缩容
        await worker_pool.stop()
        await worker_pool.scale_down(1)
        # 不应执行任何操作
        assert len(worker_pool._workers) == 0


@pytest.mark.asyncio
async def test_worker_health_check(worker_pool: WorkerPool) -> None:
    """
    测试工作者健康检查功能。
    """
    # 设置更短的健康检查参数以加速测试
    worker_pool._worker_timeout = 1
    worker_pool._health_check_interval = 1

    mock_healthy: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_unhealthy: AsyncMock = AsyncMock(spec=AbstractWorker)

    with (
        patch.object(
            worker_pool,
            "_create_worker",
            AsyncMock(side_effect=[mock_unhealthy, mock_healthy]),
        ),
        patch.object(
            worker_pool, "_restart_worker", new_callable=AsyncMock
        ) as mock_restart_worker,
    ):
        # 启动工作池
        await worker_pool.start()

        # 模拟一个工作者超时
        worker_pool._worker_last_active[mock_unhealthy] = time.time() - 3  # 设置为3秒前
        worker_pool._worker_last_active[mock_healthy] = time.time()  # 当前时间（健康）

        # 等待健康检查循环
        await asyncio.sleep(2)

        # 验证结果
        mock_restart_worker.assert_called_once_with(mock_unhealthy)


@pytest.mark.asyncio
async def test_restart_worker(worker_pool: WorkerPool) -> None:
    """
    测试重启工作者功能。
    """
    mock_old_worker: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_new_worker: AsyncMock = AsyncMock(spec=AbstractWorker)

    with patch.object(
        worker_pool, "_create_worker", AsyncMock(return_value=mock_new_worker)
    ):
        # 启动工作池并手动添加一个工作者
        await worker_pool.start()
        worker_pool._workers.append(mock_old_worker)
        worker_pool._worker_tasks[mock_old_worker] = asyncio.create_task(
            asyncio.sleep(0)
        )
        worker_pool._worker_last_active[mock_old_worker] = time.time()

        # 重启工作者
        await worker_pool._restart_worker(mock_old_worker)

        # 验证结果
        assert mock_old_worker not in worker_pool._workers
        assert mock_old_worker not in worker_pool._worker_tasks
        assert mock_old_worker not in worker_pool._worker_last_active

        assert mock_new_worker in worker_pool._workers
        assert mock_new_worker in worker_pool._worker_tasks

        # 验证调用
        mock_old_worker.stop.assert_called_once_with(cancel_tasks=True)


@pytest.mark.asyncio
async def test_on_task_completed(worker_pool: WorkerPool, mock_task: MagicMock) -> None:
    """
    测试任务完成回调功能。
    """
    # 准备测试数据
    result = SingleSimulationResultView(
        id="progress_id",
        status=SimulationTaskStatus.COMPLETE.value,
        alpha=None,
        type=AlphaType.REGULAR,
    )

    mock_worker = AsyncMock(spec=AbstractWorker)
    mock_worker.get_current_tasks = AsyncMock(return_value=[mock_task])

    # 手动设置工作池状态
    worker_pool._workers = [mock_worker]
    worker_pool._worker_last_active[mock_worker] = time.time() - 10
    worker_pool._last_status_log_time = time.time() - 120  # 确保会触发状态日志

    # 调用回调方法
    await worker_pool._on_task_completed(mock_task, result)

    # 验证统计信息更新
    assert worker_pool._processed_tasks == 1
    assert worker_pool._failed_tasks == 0
    assert len(worker_pool._task_durations) == 1

    # 验证工作者活跃时间更新
    assert worker_pool._worker_last_active[mock_worker] > time.time() - 1

    # 测试失败任务
    result_failed = SingleSimulationResultView(
        id="progress_id",
        status=SimulationTaskStatus.ERROR.value,
        alpha=None,
        type=AlphaType.REGULAR,
    )

    await worker_pool._on_task_completed(mock_task, result_failed)

    # 验证失败任务计数
    assert worker_pool._processed_tasks == 2
    assert worker_pool._failed_tasks == 1


@pytest.mark.asyncio
async def test_get_status(worker_pool: WorkerPool) -> None:
    """
    测试获取工作池状态功能。
    """
    # 模拟部分数据
    worker_pool._running = True
    worker_pool._started_at = datetime.now()
    worker_pool._processed_tasks = 10
    worker_pool._failed_tasks = 2
    worker_pool._task_durations = [1.5, 2.0, 1.8]
    worker_pool._workers = [AsyncMock() for _ in range(3)]

    # 获取状态
    status = worker_pool.get_status()

    # 验证状态字段
    assert status["running"] is True
    assert status["worker_count"] == 3
    assert status["processed_tasks"] == 10
    assert status["failed_tasks"] == 2
    assert status["success_rate"] == 0.8  # (10-2)/10
    assert (
        round(status["avg_task_duration"], 3) == 1.767
    )  # (1.5+2.0+1.8)/3 rounded to 3 decimal places
    assert status["dry_run"] is True
    assert "started_at" in status
    assert "uptime_seconds" in status
    assert "tasks_per_minute" in status


@pytest.mark.asyncio
async def test_worker_count(worker_pool: WorkerPool) -> None:
    """
    测试获取工作者数量功能。
    """
    # 初始状态应为0
    assert worker_pool.worker_count() == 0

    # 添加模拟工作者
    worker_pool._workers = [AsyncMock() for _ in range(3)]
    assert worker_pool.worker_count() == 3


@pytest.mark.asyncio
async def test_worker_pool_exception_handling(worker_pool: WorkerPool) -> None:
    """
    测试工作池的异常处理能力。
    """
    # 设置 _create_worker 抛出异常
    with patch.object(
        worker_pool,
        "_create_worker",
        AsyncMock(side_effect=Exception("创建工作者失败")),
    ):
        # 测试扩容时的异常
        await worker_pool.start()  # 应该成功启动但没有工作者
        await worker_pool.scale_up(1)  # 应该处理异常

        # 验证没有工作者被创建
        assert len(worker_pool._workers) == 0

        # 清理
        await worker_pool.stop()


@pytest.mark.asyncio
async def test_find_worker_for_task(
    worker_pool: WorkerPool, mock_task: MagicMock
) -> None:
    """
    测试查找处理特定任务的工作者功能。
    """
    # 准备测试工作者
    mock_worker1: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_worker1.get_current_tasks = AsyncMock(
        return_value=[MagicMock()]
    )  # 不含目标任务

    mock_worker2: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_worker2.get_current_tasks = AsyncMock(return_value=[mock_task])  # 含目标任务

    mock_worker3: AsyncMock = AsyncMock(spec=AbstractWorker)
    mock_worker3.get_current_tasks = AsyncMock(side_effect=Exception("无法获取任务"))

    # 设置工作池状态
    worker_pool._workers = [mock_worker1, mock_worker2, mock_worker3]

    # 查找工作者
    found_workers: list[AbstractWorker] = []
    async for worker in worker_pool._find_worker_for_task(mock_task):
        found_workers.append(worker)

    # 验证结果
    assert len(found_workers) == 1
    assert found_workers[0] == mock_worker2


@pytest.mark.asyncio
async def test_worker_pool_with_real_worker(
    mock_scheduler: AsyncMock, mock_client_factory: Callable[[], MagicMock]
) -> None:
    """
    测试工作池与真实 Worker 类的集成。

    这个测试用例模拟更接近真实场景的集成情况，使用实际的 Worker 类。
    """
    # 创建工作池
    pool: WorkerPool = WorkerPool(
        scheduler=mock_scheduler,
        client_factory=mock_client_factory,
        initial_workers=1,
        dry_run=True,  # 使用 dry_run 避免实际网络请求
        worker_timeout=5,
    )

    # 提供一个可以被调度的任务
    mock_task: MagicMock = MagicMock(spec=SimulationTask)
    mock_task.id = 1
    mock_task.status = SimulationTaskStatus.PENDING
    mock_task.scheduled_at = datetime.now()
    mock_scheduler.schedule.return_value = [mock_task]

    async def patched_create_worker() -> AbstractWorker:
        """
        创建一个真实的工作者实例用于测试。

        覆盖原有的工作者创建方法，使用 `AsyncMock` 模拟 Worker 类，
        以便更真实地测试工作池与工作者的交互。

        Returns:
            AsyncMock: 模拟的工作者实例
        """
        worker: Worker = Worker(
            client=mock_client_factory(),
            dry_run=True,
        )
        await worker.set_scheduler(mock_scheduler)
        return worker

    with patch.object(pool, "_create_worker", patched_create_worker):
        # 启动工作池
        await pool.start()

        # 等待任务处理
        await asyncio.sleep(1)

        # 验证调度器被调用
        mock_scheduler.schedule.assert_awaited()

        # 停止工作池
        await pool.stop()


@pytest.mark.asyncio
async def test_log_pool_status(worker_pool: WorkerPool) -> None:
    """
    测试工作池状态日志记录功能。
    """
    # 设置测试状态数据
    worker_pool._running = True
    worker_pool._started_at = datetime.now()
    worker_pool._processed_tasks = 5
    worker_pool._failed_tasks = 1
    worker_pool._task_durations = [2.0, 3.0]
    worker_pool._workers = [AsyncMock() for _ in range(2)]

    # 使用 patch 检查日志调用
    with patch(
        "alphapower.engine.simulation.task.worker_pool.logger", new_callable=AsyncMock
    ) as mock_logger:
        await worker_pool._log_pool_status()

        # 验证日志调用
        mock_logger.ainfo.assert_awaited()
        # 日志内容应包含工作者数量、处理任务数等关键信息
    worker_pool._task_durations = [2.0, 3.0]
    worker_pool._workers = [AsyncMock() for _ in range(2)]

    # 使用 patch 检查日志调用
    with patch(
        "alphapower.engine.simulation.task.worker_pool.logger", new_callable=AsyncMock
    ) as mock_logger:
        await worker_pool._log_pool_status()

        # 验证日志调用
        mock_logger.ainfo.assert_awaited()
        # 日志内容应包含工作者数量、处理任务数等关键信息

        # 验证日志调用
        mock_logger.ainfo.assert_awaited()
        # 日志内容应包含工作者数量、处理任务数等关键信息
