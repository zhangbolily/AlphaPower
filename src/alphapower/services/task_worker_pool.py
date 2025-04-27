"""
@file task_worker_pool.py
"""

import asyncio
import signal
import types
from typing import Optional

from structlog.stdlib import BoundLogger

from alphapower.client import WorldQuantClient, wq_client
from alphapower.engine.simulation.task.provider import DatabaseTaskProvider
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.engine.simulation.task.worker_pool import WorkerPool
from alphapower.internal.logging import get_logger

logger: BoundLogger = get_logger(__name__)


async def task_start_worker_pool(
    initial_workers: int = 1,
    dry_run: bool = False,
    worker_timeout: int = 300,
    task_fetch_size: int = 10,
    low_priority_threshold: int = 10,
    sample_rate: int = 1,
    cursor: int = 0,
) -> None:
    """
    启动工作池以执行模拟任务。

    Args:
        initial_workers (int): 初始工作者数量。
        dry_run (bool): 是否以仿真模式运行。
        worker_timeout (int): 工作者健康检查超时时间（秒）。
        task_fetch_size (int): 每次从任务提供者获取的任务数量。
        low_priority_threshold (int): 低优先级任务提升阈值。

    # TODO(Ball Chang): 新增定时主动垃圾回收机制，提高长时间运行的稳定性
    # TODO(Ball Chang): 优化日志格式，输出内容紧凑高效，日志级别配置合理
    """

    # 创建一个事件来控制优雅关闭
    shutdown_event: asyncio.Event = asyncio.Event()
    worker_pool: Optional[WorkerPool] = None

    # 定义信号处理函数
    def handle_signal(sig: int, _: Optional[types.FrameType]) -> None:
        # 信号处理为同步方法，使用同步日志接口
        logger.info(
            "收到信号，准备优雅关闭...",
            emoji="🛑",
            signal=sig,
        )
        shutdown_event.set()

    # 注册信号处理程序
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        # 初始化任务提供者
        provider: DatabaseTaskProvider = DatabaseTaskProvider(
            sample_rate=sample_rate,
            cursor=cursor,
        )

        # 初始化调度器
        scheduler: PriorityScheduler = PriorityScheduler(
            task_fetch_size=task_fetch_size,
            low_priority_threshold=low_priority_threshold,
            task_provider=provider,
        )

        # 创建客户端工厂函数
        def client_factory() -> WorldQuantClient:
            """
            创建一个新的 WorldQuantClient 实例。
            这里可以根据需要添加身份验证或其他初始化逻辑。
            """
            return wq_client

        # 初始化工作池
        worker_pool = WorkerPool(
            scheduler=scheduler,
            client_factory=client_factory,
            initial_workers=initial_workers,
            dry_run=dry_run,
            worker_timeout=worker_timeout,
        )

        # 启动工作池
        await worker_pool.start()
        await logger.ainfo(
            "工作池已启动",
            emoji="🚀",
            initial_workers=initial_workers,
            dry_run=dry_run,
            worker_timeout=worker_timeout,
            task_fetch_size=task_fetch_size,
            low_priority_threshold=low_priority_threshold,
            sample_rate=sample_rate,
            cursor=cursor,
        )

        # 等待关闭事件
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), 60)
            except asyncio.TimeoutError:
                # 每分钟检查一次状态
                if worker_pool:
                    await logger.ainfo(
                        "工作池状态",
                        emoji="👷",
                        worker_count=await worker_pool.worker_count(),
                    )

    except asyncio.CancelledError:
        await logger.awarning(
            "任务被取消，正在清理资源...",
            emoji="⚠️",
        )
    except Exception as e:
        await logger.aerror(
            "运行过程中发生错误",
            emoji="💥",
            error=str(e),
        )
    finally:
        # 停止工作池并清理资源
        if worker_pool:
            await logger.ainfo(
                "正在停止工作池...",
                emoji="🛑",
            )
            try:
                await worker_pool.stop()
                await logger.ainfo(
                    "工作池已成功停止",
                    emoji="✅",
                )
            except Exception as e:
                await logger.aerror(
                    "停止工作池时发生错误",
                    emoji="💣",
                    error=str(e),
                )

        await logger.ainfo(
            "工作池已停止，程序退出。",
            emoji="👋",
        )
