"""
@file task_worker_pool.py
"""

import asyncio
import signal
import types
from typing import Optional

from alphapower.client import WorldQuantClient, wq_client
from alphapower.engine.simulation.task.provider import DatabaseTaskProvider
from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.engine.simulation.task.worker_pool import WorkerPool
from alphapower.internal.logging import setup_logging

logger = setup_logging(__name__)


async def task_start_worker_pool(
    initial_workers: int = 1,
    dry_run: bool = False,
    worker_timeout: int = 300,
    task_fetch_size: int = 10,
    low_priority_threshold: int = 10,
) -> None:
    """
    启动工作池以执行模拟任务。

    Args:
        initial_workers (int): 初始工作者数量。
        dry_run (bool): 是否以仿真模式运行。
        worker_timeout (int): 工作者健康检查超时时间（秒）。
        task_fetch_size (int): 每次从任务提供者获取的任务数量。
        low_priority_threshold (int): 低优先级任务提升阈值。
    """

    # 创建一个事件来控制优雅关闭
    shutdown_event = asyncio.Event()
    worker_pool = None

    # 定义信号处理函数
    def handle_signal(sig: int, _: Optional[types.FrameType]) -> None:
        logger.info(f"收到信号 {sig}，准备优雅关闭...")
        shutdown_event.set()

    # 注册信号处理程序
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        # 初始化任务提供者
        provider = DatabaseTaskProvider()

        # 初始化调度器
        scheduler = PriorityScheduler(
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

        logger.info(f"工作池已启动，共 {initial_workers} 个工作者")

        # 等待关闭事件
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), 60)
            except asyncio.TimeoutError:
                # 每分钟检查一次状态
                if worker_pool:
                    logger.info(f"工作池状态：活跃工作者 {worker_pool.worker_count()}")

    except asyncio.CancelledError:
        logger.info("任务被取消，正在清理资源...")
    except Exception as e:
        logger.error(f"运行过程中发生错误: {e}")
    finally:
        # 停止工作池并清理资源
        if worker_pool:
            logger.info("正在停止工作池...")
            try:
                await worker_pool.stop()
                logger.info("工作池已成功停止")
            except Exception as e:
                logger.error(f"停止工作池时发生错误: {e}")

        logger.info("工作池已停止，程序退出。")
