"""
@file
@brief 工作池实现
@details
    该模块提供工作池实现，管理多个工作者以并行处理模拟任务。
@note
    该模块是 AlphaPower 引擎的一部分
"""

import asyncio
import time
from datetime import datetime
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from alphapower.client import SingleSimulationResultView, WorldQuantClient
from alphapower.entity import SimulationTask
from alphapower.internal.logging import setup_logging

from .scheduler_abc import AbstractScheduler
from .worker import Worker
from .worker_abc import AbstractWorker
from .worker_pool_abc import AbstractWorkerPool

logger = setup_logging(__name__)

# 类型变量定义
T = TypeVar("T")
ClientFactory = Callable[[], WorldQuantClient]
TaskCompleteCallback = Union[
    Callable[[SimulationTask, SingleSimulationResultView], None],
    Callable[[SimulationTask, SingleSimulationResultView], Awaitable[None]],
]


class WorkerPool(AbstractWorkerPool):
    """
    工作池实现类，管理多个工作者以并行处理模拟任务。

    提供工作者数量动态管理、状态监控和优雅退出功能。

    Attributes:
        _scheduler: 任务调度器实例
        _workers: 当前活跃的工作者列表
        _worker_tasks: 与工作者关联的异步任务映射
        _running: 工作池是否正在运行的标志
        _client_factory: 创建新客户端实例的工厂函数
        _dry_run: 是否以仿真模式运行的标志
        _started_at: 工作池启动时间
        _processed_tasks: 已处理任务总数
        _failed_tasks: 失败任务总数
    """

    def __init__(
        self,
        scheduler: AbstractScheduler,
        client_factory: ClientFactory,
        initial_workers: int = 1,
        dry_run: bool = False,
        worker_timeout: int = 300,  # 工作者健康检查超时时间（秒）
    ) -> None:
        """
        初始化工作池。

        Args:
            scheduler: 任务调度器实例
            client_factory: 创建新客户端实例的工厂函数
            initial_workers: 初始工作者数量，默认为1
            dry_run: 是否以仿真模式运行，默认为False
            worker_timeout: 工作者健康检查超时时间（秒）
        """
        self._scheduler: AbstractScheduler = scheduler
        self._workers: List[AbstractWorker] = []
        self._worker_tasks: Dict[AbstractWorker, asyncio.Task[Any]] = {}
        self._worker_last_active: Dict[AbstractWorker, float] = (
            {}
        )  # 记录工作者最后活跃时间
        self._running: bool = False
        self._client_factory: ClientFactory = client_factory
        self._dry_run: bool = dry_run
        self._initial_workers: int = max(1, initial_workers)  # 确保至少有1个工作者

        # 工作者健康检查配置
        self._worker_timeout: int = worker_timeout
        self._health_check_task: Optional[asyncio.Task[None]] = None
        self._health_check_interval: int = min(
            30, max(5, worker_timeout // 10)
        )  # 智能化健康检查间隔

        # 状态统计
        self._started_at: Optional[datetime] = None
        self._processed_tasks: int = 0
        self._failed_tasks: int = 0
        self._task_durations: List[float] = []  # 记录任务处理时间
        self._last_status_log_time: float = 0  # 上次状态日志记录时间

        # 创建锁以保证工作者管理的线程安全
        self._workers_lock: asyncio.Lock = asyncio.Lock()

    async def _create_worker(self) -> AbstractWorker:
        """
        创建并初始化一个新的工作者实例。

        Returns:
            AbstractWorker: 初始化后的工作者实例
        """
        try:
            client: WorldQuantClient = self._client_factory()
            await asyncio.sleep(5)
            worker: Worker = Worker(client, dry_run=self._dry_run)
            await worker.set_scheduler(self._scheduler)
            await worker.add_task_complete_callback(self._on_task_completed)
            await worker.add_heartbeat_callback(self._on_worker_heartbeat)
            # 记录工作者创建时间作为最后活跃时间
            self._worker_last_active[worker] = time.time()
            await logger.adebug(f"成功创建新工作者: {id(worker)}")
            return worker
        except Exception as e:
            await logger.aerror(f"创建工作者失败: {str(e)}")
            raise

    async def _on_task_completed(
        self, task: SimulationTask, result: SingleSimulationResultView
    ) -> None:
        """
        任务完成回调函数。

        更新工作池的任务处理统计信息。

        Args:
            task: 完成的任务
            result: 任务结果
        """
        self._processed_tasks += 1
        if result.status != "COMPLETE":
            self._failed_tasks += 1

        # 记录任务处理时间（如果任务有开始时间）
        if task.scheduled_at:
            duration = time.time() - task.scheduled_at.timestamp()
            self._task_durations.append(duration)
            # 只保留最近100个任务的数据以限制内存使用
            if len(self._task_durations) > 100:
                self._task_durations.pop(0)

        # 更新工作者活跃时间
        async for worker in self._find_worker_for_task(task):
            self._worker_last_active[worker] = time.time()
            break

        # 定期记录工作池状态
        current_time = time.time()
        if current_time - self._last_status_log_time > 60:  # 每分钟记录一次状态
            self._last_status_log_time = current_time
            await self._log_pool_status()

        await logger.adebug(
            f"任务完成: {task.id}, 状态: {result.status}, "
            f"已处理任务总数: {self._processed_tasks}, 失败任务总数: {self._failed_tasks}"
        )

    async def _on_worker_heartbeat(self, worker: AbstractWorker) -> None:
        """
        工作者心跳回调函数。

        更新工作者的最后活跃时间，用于健康状态监控。

        Args:
            worker: 发送心跳的工作者实例
        """
        if worker in self._worker_last_active:
            # 更新工作者最后活跃时间
            self._worker_last_active[worker] = time.time()

            # 定期记录心跳信息（避免日志过多，只在调试级别记录）
            if logger.isEnabledFor(10):  # DEBUG level
                await logger.adebug(f"收到工作者 {id(worker)} 心跳")
        else:
            await logger.awarning(
                f"收到未知工作者 {id(worker)} 的心跳，可能是新创建的工作者"
            )

    async def _find_worker_for_task(
        self, task: SimulationTask
    ) -> AsyncIterable[AbstractWorker]:
        """查找正在处理指定任务的工作者"""
        for worker in self._workers:
            try:
                current_tasks = await worker.get_current_tasks()
                if task in current_tasks:
                    yield worker
            except Exception as e:
                await logger.awarning(f"获取工作者任务列表失败: {str(e)}")

    async def _log_pool_status(self) -> None:
        """记录工作池当前状态"""
        status = self.get_status()
        avg_duration = status.get("avg_task_duration", 0)
        success_rate = status.get("success_rate", 0)
        if success_rate is not None:
            success_rate = f"{success_rate * 100:.2f}%"

        await logger.ainfo(
            f"工作池状态: 工作者数量={status['worker_count']}, "
            f"处理任务={status['processed_tasks']}, "
            f"成功率={success_rate}, "
            f"平均处理时间={avg_duration:.2f}秒"
        )

    async def start(self) -> None:
        """
        启动工作池。

        创建初始工作者并开始处理任务。
        """
        if self._running:
            await logger.awarning("工作池已经在运行中，忽略启动请求")
            return

        self._running = True
        self._started_at = datetime.now()
        await logger.ainfo(f"启动工作池，初始工作者数量: {self._initial_workers}")

        # 创建初始工作者
        await self.scale_up(self._initial_workers)

        # 启动健康检查任务
        if self._worker_timeout > 0:
            self._health_check_task = asyncio.create_task(self._worker_health_check())
            await logger.adebug(
                f"已启动工作者健康检查，超时时间: {self._worker_timeout}秒, 检查间隔: {self._health_check_interval}秒"
            )

        # 记录初始状态
        self._last_status_log_time = time.time()
        await self._log_pool_status()

    async def stop(self) -> None:
        """
        停止工作池，并回收所有工作者相关资源。

        等待所有工作者优雅退出，并清理相关资源。
        """
        if not self._running:
            await logger.awarning("工作池未运行，忽略停止请求")
            return

        await logger.ainfo(f"停止工作池，当前工作者数量: {len(self._workers)}")
        self._running = False

        # 停止健康检查任务
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None

        async with self._workers_lock:
            # 停止所有工作者
            stop_tasks: List[Awaitable[None]] = [
                worker.stop(cancel_tasks=True) for worker in self._workers
            ]
            if stop_tasks:
                try:
                    await asyncio.gather(*stop_tasks)
                except Exception as e:
                    await logger.aerror(f"停止工作者时发生错误: {str(e)}")

            # 等待所有工作者任务完成
            if self._worker_tasks:
                pending_tasks: List[asyncio.Task[Any]] = list(
                    self._worker_tasks.values()
                )
                if pending_tasks:
                    done: Set[asyncio.Task[Any]]
                    pending: Set[asyncio.Task[Any]]
                    try:
                        done, pending = await asyncio.wait(
                            pending_tasks,
                            timeout=30,  # 设置超时时间，避免无限等待
                            return_when=asyncio.ALL_COMPLETED,
                        )

                        await logger.adebug(
                            f"工作者任务完成: {len(done)}, "
                            f"未完成任务: {len(pending)}"
                        )
                        if pending:
                            await logger.awarning(
                                f"有 {len(pending)} 个工作者任务未能在超时时间内完成"
                            )
                            for task in pending:
                                task.cancel()
                    except Exception as e:
                        await logger.aerror(f"等待工作者任务完成时出错: {str(e)}")

            # 清理资源
            self._workers.clear()
            self._worker_tasks.clear()
            self._worker_last_active.clear()

        await logger.ainfo("工作池已停止")

    async def scale_up(self, count: int) -> None:
        """
        向上扩容指定数量的工作者。

        Args:
            count: 要增加的工作者数量
        """
        if count <= 0:
            await logger.awarning(f"无效的扩容数量: {count}，必须大于0")
            return

        if not self._running:
            await logger.awarning("工作池未运行，无法扩容")
            return

        await logger.ainfo(f"开始向上扩容 {count} 个工作者")
        created_count = 0

        async with self._workers_lock:
            for _ in range(count):
                try:
                    # 创建新工作者
                    worker: AbstractWorker = await self._create_worker()

                    # 创建并启动工作者任务
                    task: asyncio.Task[Any] = asyncio.create_task(worker.run())

                    # 保存工作者和任务引用
                    self._workers.append(worker)
                    self._worker_tasks[worker] = task
                    created_count += 1
                except Exception as e:
                    await logger.aerror(f"创建工作者失败: {str(e)}")

        await logger.ainfo(
            f"扩容完成，成功创建 {created_count} 个工作者，当前总数: {len(self._workers)}"
        )

    async def scale_down(self, count: int) -> None:
        """
        向下缩容指定数量的工作者。

        Args:
            count: 要减少的工作者数量
        """
        if count <= 0:
            await logger.awarning(f"无效的缩容数量: {count}，必须大于0")
            return

        if not self._running:
            await logger.awarning("工作池未运行，无法缩容")
            return

        async with self._workers_lock:
            # 确保缩容数量不超过现有工作者数量，并保留至少一个工作者
            max_removable = max(0, len(self._workers) - 1)
            actual_count: int = min(count, max_removable)
            if actual_count == 0:
                await logger.awarning("无法缩容，必须保留至少一个工作者")
                return

            await logger.ainfo(f"开始向下缩容 {actual_count} 个工作者")
            stopped_count = 0

            # 选择要停止的工作者 - 优先选择不活跃的工作者
            workers_by_activity = sorted(
                self._workers, key=lambda w: self._worker_last_active.get(w, 0)
            )
            workers_to_stop = workers_by_activity[:actual_count]

            # 停止选中的工作者
            for worker in workers_to_stop:
                try:
                    # 从列表中移除工作者
                    self._workers.remove(worker)

                    # 获取工作者任务
                    task: asyncio.Task[Any] = self._worker_tasks.pop(worker)

                    # 停止工作者
                    await worker.stop(cancel_tasks=True)

                    # 清除活跃时间记录
                    self._worker_last_active.pop(worker, None)

                    # 取消任务
                    if not task.done():
                        task.cancel()
                        try:
                            await asyncio.wait_for(task, 5.0)  # 设置超时避免长时间等待
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass

                    stopped_count += 1
                    await logger.adebug(f"已停止一个工作者，剩余: {len(self._workers)}")
                except Exception as e:
                    await logger.aerror(f"停止工作者失败: {str(e)}")

        await logger.ainfo(
            f"缩容完成，已停止 {stopped_count} 个工作者，当前剩余: {len(self._workers)}"
        )

    # 工作者健康检查方法
    async def _worker_health_check(self) -> None:
        """定期检查工作者健康状态，重启或替换不活跃的工作者"""
        try:
            while self._running:
                await asyncio.sleep(self._health_check_interval)

                if not self._running:
                    break

                current_time = time.time()
                workers_to_restart = []

                async with self._workers_lock:
                    for worker in self._workers:
                        # 如果工作者超过超时时间未活跃，标记为需要重启
                        last_active = self._worker_last_active.get(worker, 0)
                        if current_time - last_active > self._worker_timeout:
                            workers_to_restart.append(worker)
                            await logger.awarning(
                                f"检测到工作者 {id(worker)} 不活跃，"
                                f"最后活跃时间: {datetime.fromtimestamp(last_active).isoformat()}"
                            )

                # 重启不健康的工作者
                if workers_to_restart:
                    await logger.awarning(
                        f"检测到 {len(workers_to_restart)} 个工作者不活跃，准备重启"
                    )
                    for worker in workers_to_restart:
                        await self._restart_worker(worker)

        except asyncio.CancelledError:
            await logger.adebug("工作者健康检查任务已取消")
        except Exception as e:
            await logger.aerror(f"工作者健康检查出错: {str(e)}")

    # 重启单个工作者
    async def _restart_worker(self, worker: AbstractWorker) -> None:
        """
        重启不健康的工作者

        Args:
            worker: 需要重启的工作者
        """
        try:
            async with self._workers_lock:
                if worker not in self._workers:
                    return

                # 从列表中移除工作者
                self._workers.remove(worker)
                await logger.adebug(f"准备重启工作者 {id(worker)}")

                # 获取并取消工作者任务
                task = self._worker_tasks.pop(worker, None)
                if task and not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=5)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

                # 尝试停止工作者
                try:
                    await worker.stop(cancel_tasks=True)
                except Exception as e:
                    await logger.awarning(f"停止不健康工作者时出错: {str(e)}")

                # 删除活跃时间记录
                self._worker_last_active.pop(worker, None)

                # 创建新工作者替代
                new_worker = await self._create_worker()
                new_task = asyncio.create_task(new_worker.run())
                self._workers.append(new_worker)
                self._worker_tasks[new_worker] = new_task

                await logger.ainfo(f"已成功重启一个不健康的工作者 {id(worker)}")

        except Exception as e:
            await logger.aerror(f"重启工作者失败: {str(e)}")

    def get_status(self) -> dict:
        """
        获取工作池的运行状态和各项参数。

        Returns:
            dict: 包含工作池状态信息的字典
        """
        uptime: Optional[float] = None
        if self._started_at:
            uptime = (datetime.now() - self._started_at).total_seconds()

        # 计算平均任务处理时间
        avg_task_duration = (
            sum(self._task_durations) / len(self._task_durations)
            if self._task_durations
            else 0
        )

        # 计算任务处理速率（每分钟）
        tasks_per_minute = 0
        if uptime and uptime > 60:
            tasks_per_minute = int((self._processed_tasks / uptime) * 60)

        return {
            "running": self._running,
            "worker_count": len(self._workers),
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "uptime_seconds": uptime,
            "processed_tasks": self._processed_tasks,
            "failed_tasks": self._failed_tasks,
            "success_rate": (
                (self._processed_tasks - self._failed_tasks) / self._processed_tasks
                if self._processed_tasks > 0
                else None
            ),
            "dry_run": self._dry_run,
            # 任务统计
            "avg_task_duration": avg_task_duration,
            "tasks_per_minute": tasks_per_minute,
            "health_check_enabled": self._worker_timeout > 0,
            "health_check_interval": self._health_check_interval,
        }

    def worker_count(self) -> int:
        """
        获取当前工作者数量。

        Returns:
            int: 当前活跃的工作者数量
        """
        return len(self._workers)
