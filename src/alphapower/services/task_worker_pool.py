import asyncio

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

    async def main() -> None:
        try:
            provider: DatabaseTaskProvider = DatabaseTaskProvider()

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

            # 持续运行直到手动终止
            while True:
                await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("任务被取消，正在清理资源...")
        except Exception as e:
            logger.error(f"运行过程中发生错误: {e}")
        finally:
            # 确保资源被正确回收
            if "worker_pool" in locals():
                await worker_pool.stop()
            logger.info("工作池已停止，程序退出。")

    # 使用 asyncio.run 启动异步主函数
    try:
        await main()
    except KeyboardInterrupt:
        logger.info("检测到键盘中断，程序退出。")
