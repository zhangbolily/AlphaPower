"""
@file
@brief 工作池实现
@details
    该模块提供工作池实现，管理多个工作者以并行处理模拟任务。
@note
    该模块是 AlphaPower 引擎的一部分
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import (
    Any,
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
            await logger.adebug(
                event="成功创建新工作者",
                worker_id=id(worker),
                message="新工作者已成功创建",
                emoji="🛠️",
            )
            return worker
        except Exception as e:
            await logger.aerror(
                event="创建工作者失败",
                error=str(e),
                message="工作者创建过程中发生错误",
                emoji="❌",
            )
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
        # 注意：此查找效率较低，理想情况下 Worker 实例应直接传递给回调
        worker = await self._find_worker_for_task(task)
        if worker:
            self._worker_last_active[worker] = time.time()

        # 定期记录工作池状态
        current_time = time.time()
        if current_time - self._last_status_log_time > 60:  # 每分钟记录一次状态
            self._last_status_log_time = current_time
            await self._log_pool_status()

        await logger.adebug(
            event="任务完成",
            task_id=task.id,
            status=result.status,
            processed_tasks=self._processed_tasks,
            failed_tasks=self._failed_tasks,
            message="任务已完成，更新统计信息",
            emoji="✅",
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
            if logger.isEnabledFor(logging.DEBUG):  # DEBUG level
                await logger.adebug(
                    event="收到工作者心跳",
                    worker_id=id(worker),
                    emoji="💓",
                )
        else:
            await logger.awarning(
                event="收到未知工作者心跳",
                worker_id=id(worker),
                emoji="❓",
            )

    # 注意：此方法效率较低，尤其在工作者数量多时。
    # 返回类型修改为 Optional，因为 async for + break 行为更像查找单个元素。
    async def _find_worker_for_task(
        self, task: SimulationTask
    ) -> Optional[AbstractWorker]:
        """
        查找正在处理指定任务的工作者。

        Args:
            task: 需要查找的任务实例。

        Returns:
            Optional[AbstractWorker]: 找到的工作者实例，如果未找到则返回 None。

        Note:
            当前实现效率较低，需要遍历所有工作者。
        """
        async with self._workers_lock:  # 访问 _workers 需要加锁
            for worker in self._workers:
                try:
                    current_tasks = await worker.get_current_tasks()
                    if task in current_tasks:
                        return worker
                except Exception as e:
                    await logger.awarning(
                        event="获取工作者任务列表失败",
                        worker_id=id(worker),
                        error=str(e),
                        emoji="⚠️",
                    )
        return None

    async def _log_pool_status(self) -> None:
        """记录工作池当前状态"""
        status = await self.get_status()
        avg_duration = status.get("avg_task_duration", 0)
        success_rate = status.get("success_rate", 0)
        if success_rate is not None:
            success_rate = f"{success_rate * 100:.2f}%"

        await logger.ainfo(
            event="工作池状态",
            worker_count=status["worker_count"],
            processed_tasks=status["processed_tasks"],
            success_rate=success_rate,
            avg_task_duration=f"{avg_duration:.2f}秒",
            message="记录当前工作池状态",
            emoji="📊",
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
        await logger.ainfo(
            event="启动工作池",
            initial_workers=self._initial_workers,
            message="工作池已启动",
            emoji="🚀",
        )

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
            await logger.awarning(
                event="工作池未运行",
                message="工作池未运行，忽略停止请求",
                emoji="🚫",
            )
            return

        await logger.ainfo(
            event="开始停止工作池",
            worker_count=len(self._workers),
            message="正在停止工作池并回收资源",
            emoji="⏳",
        )
        self._running = False

        # 停止健康检查任务
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                await logger.adebug(
                    event="健康检查任务已取消",
                    message="工作者健康检查任务已成功取消",
                    emoji="🩺",
                )
            self._health_check_task = None

        async with self._workers_lock:
            # 停止所有工作者
            stop_tasks: List[Awaitable[None]] = [
                worker.stop(cancel_tasks=True) for worker in self._workers
            ]
            if stop_tasks:
                try:
                    await asyncio.gather(*stop_tasks)
                    await logger.adebug(
                        event="所有工作者已停止",
                        stopped_count=len(stop_tasks),
                        emoji="✅",
                    )
                except Exception as e:
                    await logger.aerror(
                        event="停止工作者时发生错误",
                        error=str(e),
                        message="在停止工作者过程中发生异常",
                        emoji="❌",
                    )

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
                            event="工作者任务等待结果",
                            done_count=len(done),
                            pending_count=len(pending),
                            message="等待工作者任务完成",
                            emoji="⏳",
                        )
                        if pending:
                            await logger.awarning(
                                event="工作者任务超时",
                                pending_count=len(pending),
                                timeout=30,
                                message="部分工作者任务未能在超时时间内完成，将被取消",
                                emoji="⏱️",
                            )
                            for task in pending:
                                task.cancel()
                    except Exception as e:
                        await logger.aerror(
                            event="等待工作者任务完成时出错",
                            error=str(e),
                            message="在等待工作者任务完成过程中发生异常",
                            emoji="❌",
                        )

            # 清理资源
            self._workers.clear()
            self._worker_tasks.clear()
            self._worker_last_active.clear()
            await logger.adebug(
                event="工作池资源已清理",
                message="工作者列表、任务和活跃时间记录已清空",
                emoji="🧹",
            )

        await logger.ainfo(
            event="工作池已完全停止",
            message="所有工作者已停止，资源已清理",
            emoji="🛑",
        )

    async def scale_up(self, count: int) -> None:
        """
        向上扩容指定数量的工作者。

        Args:
            count: 要增加的工作者数量
        """
        if count <= 0:
            await logger.awarning(
                event="无效扩容数量",
                count=count,
                message="扩容数量必须大于0",
                emoji="🔢",
            )
            return

        if not self._running:
            await logger.awarning(
                event="工作池未运行",
                message="无法在停止状态下扩容工作池",
                emoji="🚫",
            )
            return

        await logger.ainfo(
            event="开始扩容",
            count=count,
            message=f"准备向上扩容 {count} 个工作者",
            emoji="📈",
        )
        created_count = 0

        async with self._workers_lock:
            for i in range(count):
                try:
                    # 创建新工作者
                    worker: AbstractWorker = await self._create_worker()

                    # 创建并启动工作者任务
                    task: asyncio.Task[Any] = asyncio.create_task(worker.run())

                    # 保存工作者和任务引用
                    self._workers.append(worker)
                    self._worker_tasks[worker] = task
                    created_count += 1
                    await logger.adebug(
                        event="成功创建并启动工作者",
                        worker_index=i + 1,
                        total_to_create=count,
                        worker_id=id(worker),
                        emoji="✨",
                    )
                except Exception as e:
                    await logger.aerror(
                        event="创建工作者失败",
                        worker_index=i + 1,
                        error=str(e),
                        message="在扩容过程中创建工作者失败",
                        emoji="❌",
                    )

        await logger.ainfo(
            event="扩容完成",
            requested_count=count,
            created_count=created_count,
            total_workers=len(self._workers),
            message="工作池扩容操作完成",
            emoji="➕",
        )

    async def scale_down(self, count: int) -> None:
        """
        向下缩容指定数量的工作者。

        Args:
            count: 要减少的工作者数量。
        """
        if count <= 0:
            await logger.awarning(
                event="无效缩容数量",
                count=count,
                message="缩容数量必须大于0",
                emoji="🔢",
            )
            return

        if not self._running:
            await logger.awarning(
                event="工作池未运行",
                message="无法在停止状态下缩容工作池",
                emoji="🚫",
            )
            return

        removed_count = 0
        async with self._workers_lock:
            current_count = len(self._workers)
            if count > current_count:
                await logger.awarning(
                    event="缩容数量过多",
                    requested_count=count,
                    current_count=current_count,
                    message="请求缩容的数量超过当前工作者总数，将移除所有工作者",
                    emoji="📉",
                )
                count = current_count  # 最多移除所有工作者

            await logger.ainfo(
                event="开始缩容",
                count=count,
                current_workers=current_count,
                message=f"准备向下缩容 {count} 个工作者",
                emoji="📉",
            )

            workers_to_remove = self._workers[:count]  # 选择列表前面的工作者进行移除

            stop_tasks: List[Awaitable[None]] = []
            for worker in workers_to_remove:
                # 从列表中移除
                self._workers.remove(worker)
                self._worker_last_active.pop(worker, None)

                # 获取并取消任务
                task = self._worker_tasks.pop(worker, None)
                if task and not task.done():
                    task.cancel()
                    # 可以选择等待任务取消，但为了快速缩容，这里仅取消
                    # try:
                    #     await asyncio.wait_for(task, timeout=5)
                    # except (asyncio.CancelledError, asyncio.TimeoutError):
                    #     pass # 忽略取消或超时错误
                    # except Exception as e:
                    #     await logger.aerror(f"等待被缩容工作者任务取消时出错: {e}")

                # 添加停止任务
                stop_tasks.append(worker.stop(cancel_tasks=True))
                removed_count += 1
                await logger.adebug(
                    event="标记工作者待移除",
                    worker_id=id(worker),
                    emoji="➖",
                )

            # 并发停止选定的工作者
            if stop_tasks:
                results = await asyncio.gather(*stop_tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        await logger.aerror(
                            event="停止被缩容工作者时出错",
                            worker_id=id(workers_to_remove[i]),
                            error=str(result),
                            message="在缩容过程中停止工作者失败",
                            emoji="❌",
                        )

        await logger.ainfo(
            event="缩容完成",
            requested_count=count,
            removed_count=removed_count,
            remaining_workers=len(self._workers),
            message="工作池缩容操作完成",
            emoji="➖",
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
                                event="检测到工作者不活跃",
                                worker_id=id(worker),
                                last_active=datetime.fromtimestamp(
                                    last_active
                                ).isoformat(),
                                message="工作者长时间未活跃，可能需要重启",
                                emoji="⚠️",
                            )

                # 重启不健康的工作者
                if workers_to_restart:
                    await logger.awarning(
                        f"检测到 {len(workers_to_restart)} 个工作者不活跃，准备重启"
                    )
                    # TODO: 考虑使用 asyncio.gather 并发重启，以提高效率
                    for worker in workers_to_restart:
                        await self._restart_worker(worker)

        except asyncio.CancelledError:
            await logger.adebug("工作者健康检查任务已取消")
        except Exception as e:
            await logger.aerror(f"工作者健康检查出错: {str(e)}")

    # 重启单个工作者
    async def _restart_worker(self, worker: AbstractWorker) -> None:
        """
        重启不健康的工作者。

        此方法会停止并移除指定的工作者，然后创建一个新的工作者来替代它。

        Args:
            worker: 需要重启的工作者实例
        """
        worker_id_to_restart = id(worker)  # 提前获取 ID，以防 worker 对象后续不可用
        try:
            async with self._workers_lock:
                # 再次检查工作者是否仍然存在于列表中，防止并发问题
                if worker not in self._workers:
                    await logger.adebug(
                        event="工作者已不在列表中",
                        worker_id=worker_id_to_restart,
                        message="尝试重启的工作者已不在活跃列表，可能已被处理",  # 缩短消息
                        emoji="🤷",
                    )
                    return

                # 从列表中移除工作者
                self._workers.remove(worker)
                await logger.adebug(
                    event="准备重启工作者",
                    worker_id=worker_id_to_restart,
                    message="从活跃列表中移除不健康的工作者",
                    emoji="🔧",
                )

                # 获取并取消工作者任务
                task = self._worker_tasks.pop(worker, None)
                if task and not task.done():
                    task.cancel()
                    try:
                        # 等待任务取消完成，设置短暂超时
                        await asyncio.wait_for(task, timeout=5)
                        await logger.adebug(
                            event="工作者任务已取消",
                            worker_id=worker_id_to_restart,
                            message="成功取消不健康工作者的关联任务",  # 缩短消息
                            emoji="❌",
                        )
                    except asyncio.CancelledError:
                        await logger.adebug(
                            event="工作者任务取消确认",
                            worker_id=worker_id_to_restart,
                            message="工作者任务已被取消",
                            emoji="👍",
                        )
                    except asyncio.TimeoutError:
                        await logger.awarning(
                            event="取消工作者任务超时",
                            worker_id=worker_id_to_restart,
                            timeout=5,
                            message="取消工作者任务超时未完成",  # 缩短消息
                            emoji="⏱️",
                        )
                    except Exception as e:
                        # 记录等待任务取消时可能出现的其他异常
                        await logger.aerror(
                            event="等待任务取消时发生异常",
                            worker_id=worker_id_to_restart,
                            error=str(e),
                            exc_info=True,  # 添加堆栈信息
                            message="等待工作者任务取消完成时发生错误",  # 缩短消息
                            emoji="💥",
                        )

                # 尝试停止工作者
                try:
                    await worker.stop(cancel_tasks=True)  # 再次确保停止
                    await logger.adebug(
                        event="不健康工作者已停止",
                        worker_id=worker_id_to_restart,
                        message="成功调用不健康工作者的停止方法",
                        emoji="🛑",
                    )
                except Exception as e:
                    await logger.awarning(
                        event="停止不健康工作者时出错",
                        worker_id=worker_id_to_restart,
                        error=str(e),
                        message="尝试停止不健康工作者时发生异常，可能资源未完全释放",
                        emoji="⚠️",
                    )

                self._worker_last_active.pop(worker, None)
                await logger.adebug(
                    event="清理工作者活跃记录",
                    worker_id=worker_id_to_restart,
                    message="已移除不健康工作者的最后活跃时间记录",
                    emoji="🧹",
                )

                await logger.adebug(
                    event="开始创建新工作者",
                    message="准备创建新的工作者以替换不健康的工作者",
                    emoji="🏗️",
                )
                new_worker = await self._create_worker()
                new_task = asyncio.create_task(new_worker.run())
                self._workers.append(new_worker)
                self._worker_tasks[new_worker] = new_task
                new_worker_id = id(new_worker)

                await logger.ainfo(
                    event="成功重启工作者",
                    old_worker_id=worker_id_to_restart,
                    new_worker_id=new_worker_id,
                    message="不健康的工作者已被新的工作者成功替换并启动",
                    emoji="🔄",
                )

        except Exception as e:
            # 捕获重启过程中的任何其他未预料异常
            await logger.aerror(
                event="重启工作者失败",
                worker_id=worker_id_to_restart,  # 记录尝试重启的工作者ID
                error=str(e),
                exc_info=True,
                message="在重启工作者的过程中发生严重错误",
                emoji="🆘",
            )
            # TODO: 考虑是否需要尝试再次添加工作者以维持数量，或者让健康检查下次处理。
            # 当前未添加，以避免无限循环创建失败。

    async def get_status(self) -> dict:
        """
        获取工作池的运行状态和各项参数。

        Returns:
            dict: 包含工作池状态信息的字典
        """
        async with self._workers_lock:  # 读取共享状态需要加锁
            uptime: Optional[float] = None
            if self._started_at:
                uptime = (datetime.now() - self._started_at).total_seconds()

            avg_task_duration = (
                sum(self._task_durations) / len(self._task_durations)
                if self._task_durations
                else 0
            )

            tasks_per_minute = 0
            if uptime and uptime > 60:
                tasks_per_minute = int((self._processed_tasks / uptime) * 60)

            return {
                "running": self._running,
                "worker_count": len(self._workers),
                "started_at": (
                    self._started_at.isoformat() if self._started_at else None
                ),
                "uptime_seconds": uptime,
                "processed_tasks": self._processed_tasks,
                "failed_tasks": self._failed_tasks,
                "success_rate": (
                    (self._processed_tasks - self._failed_tasks) / self._processed_tasks
                    if self._processed_tasks > 0
                    else None
                ),
                "dry_run": self._dry_run,
                "avg_task_duration": avg_task_duration,
                "tasks_per_minute": tasks_per_minute,
                "health_check_enabled": self._worker_timeout > 0,
                "health_check_interval": self._health_check_interval,
            }

    async def worker_count(self) -> int:
        """
        获取当前工作者数量。

        Returns:
            int: 当前活跃的工作者数量
        """
        async with self._workers_lock:  # 读取 worker 数量需要加锁
            return len(self._workers)
