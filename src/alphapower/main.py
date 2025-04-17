"""
AlphaPower 主入口文件。

此模块提供了 CLI 命令，用于同步数据集、因子和数据字段，以及启动模拟任务的工作池。

Attributes:
    logger (Logger): 模块级日志记录器，用于记录调试和运行时信息。
"""

import asyncio
import signal
import types
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncclick as click  # 替换为 asyncclick

from alphapower.internal.logging import get_logger
from alphapower.internal.storage import close_resources
from alphapower.internal.utils import safe_async_run
from alphapower.services.sync_alphas import sync_alphas
from alphapower.services.sync_datafields import sync_datafields
from alphapower.services.sync_datasets import sync_datasets
from alphapower.services.task_worker_pool import (
    task_start_worker_pool,
)

logger = get_logger(__name__)


async def handle_exit_signal(signum: int, frame: Optional[types.FrameType]) -> None:
    """
    处理退出信号的异步函数。

    在接收到退出信号时，执行资源清理操作并退出程序。

    Args:
        signum (int): 信号编号。
        frame (Optional[types.FrameType]): 信号处理的当前帧。

    Returns:
        None

    Raises:
        Exception: 如果资源清理过程中发生错误。
    """
    await logger.ainfo(f"接收到信号 {signum}，帧架信息: {frame}", emoji="🚦")
    await close_resources()
    await logger.ainfo("资源清理完成，程序即将退出。", emoji="✅")


# 注册信号处理函数
signal.signal(
    signal.SIGINT, lambda s, f: safe_async_run(handle_exit_signal(s, f))
)  # 处理 Ctrl+C
signal.signal(
    signal.SIGTERM, lambda s, f: safe_async_run(handle_exit_signal(s, f))
)  # 处理终止信号


@click.group()
async def cli() -> None:
    """
    CLI 命令组的入口。

    初始化 CLI 命令组，并记录调试日志。

    Returns:
        None
    """
    await logger.adebug("CLI 初始化完成。", emoji="🚀")


@cli.group()
async def sync() -> None:
    """
    同步命令组。

    提供用于同步数据集、因子和数据字段的子命令。

    Returns:
        None
    """
    await logger.adebug("同步命令组初始化完成。")


@cli.group()
async def simulation() -> None:
    """
    模拟命令组。

    提供用于启动模拟任务的子命令。

    Returns:
        None
    """
    await logger.adebug("模拟命令组初始化完成。")


@sync.command()
@click.option("--region", default=None, help="区域")
@click.option("--universe", default=None, help="股票池")
@click.option("--delay", default=None, help="延迟")
@click.option("--parallel", default=5, help="并行数 默认为5")
async def datasets(
    region: Optional[str], universe: Optional[str], delay: Optional[int], parallel: int
) -> None:
    """
    同步数据集。

    Args:
        region (Optional[str]): 区域。
        universe (Optional[str]): 股票池。
        delay (Optional[int]): 延迟时间。
        parallel (int): 并行数，默认为 5。

    Returns:
        None

    Raises:
        Exception: 如果同步过程中发生错误。
    """
    await logger.ainfo(
        f"开始同步数据集，参数: region={region}, universe={universe}, "
        f"delay={delay}, parallel={parallel}",
        emoji="📊",
    )
    await sync_datasets(
        region=region, universe=universe, delay=delay, parallel=parallel
    )
    await logger.ainfo("数据集同步完成。", emoji="✅")


@sync.command()
@click.option("--start_time", default=None, help="开始时间")
@click.option("--end_time", default=None, help="结束时间")
@click.option(
    "--increamental", is_flag=True, default=False, help="增量同步，默认为全量同步"
)
@click.option("--parallel", default=5, help="并行数 默认为5")
async def alphas(
    start_time: Optional[str],
    end_time: Optional[str],
    increamental: bool = False,
    parallel: int = 5,
) -> None:
    """
    同步因子。

    Args:
        start_time (Optional[str]): 开始时间。
        end_time (Optional[str]): 结束时间。
        increamental (bool): 是否增量同步，默认为 False。
        parallel (int): 并行数，默认为 5。

    Returns:
        None

    Raises:
        ValueError: 如果日期格式不支持。
        Exception: 如果同步过程中发生错误。
    """

    def parse_date(date_str: str) -> datetime:
        """
        解析日期字符串为 datetime 对象。

        Args:
            date_str (str): 日期字符串。

        Returns:
            datetime: 解析后的 datetime 对象。

        Raises:
            ValueError: 如果日期格式不支持。
        """
        est: timezone = timezone(timedelta(hours=-5))  # 定义 EST 时区
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).replace(tzinfo=est)
            except ValueError:
                continue
        raise ValueError(f"日期格式不支持: {date_str}")

    await logger.ainfo(
        f"开始同步因子，参数: start_time={start_time}, end_time={end_time}, "
        f"parallel={parallel}, increamental={increamental}",
        emoji="📈",
    )
    parsed_start_time: datetime = datetime.fromtimestamp(0, tz=timezone.utc)
    parsed_end_time: datetime = datetime.now(tz=timezone.utc)
    if start_time:
        parsed_start_time = parse_date(start_time)
    if end_time:
        parsed_end_time = parse_date(end_time)

    await sync_alphas(
        start_time=parsed_start_time,
        end_time=parsed_end_time,
        increamental=increamental,
        parallel=parallel,
    )
    await logger.ainfo("因子同步完成。", emoji="✅")


@sync.command()
@click.option("--instrument_type", default="EQUITY", help="工具类型")
@click.option("--dataset_id", default=None, help="数据集ID")
@click.option("--parallel", default=5, help="并行数 默认为5")
async def datafields(
    instrument_type: str, dataset_id: Optional[str], parallel: int
) -> None:
    """
    同步数据字段。

    Args:
        instrument_type (str): 工具类型。
        dataset_id (Optional[str]): 数据集 ID。
        parallel (int): 并行数，默认为 5。

    Returns:
        None

    Raises:
        Exception: 如果同步过程中发生错误。
    """
    await logger.ainfo(
        f"开始同步数据字段，参数: instrument_type={instrument_type}, "
        f"dataset_id={dataset_id}, parallel={parallel}",
        emoji="📋",
    )
    await sync_datafields(
        instrument_type=instrument_type,
        dataset_id=dataset_id,
        parallel=parallel,
    )
    await logger.ainfo("数据字段同步完成。", emoji="✅")


@simulation.command()
@click.option("--initial-workers", default=1, help="初始工作者数量")
@click.option("--dry-run", is_flag=True, help="以仿真模式运行，不实际执行任务")
@click.option("--worker-timeout", default=300, help="工作者健康检查超时时间（秒）")
@click.option("--task-fetch-size", default=10, help="每次从任务提供者获取的任务数量")
@click.option("--sample-rate", default=1, help="任务跳采样率")
@click.option("--low-priority-threshold", default=10, help="低优先级任务提升阈值")
async def start_worker_pool(
    initial_workers: int,
    dry_run: bool,
    worker_timeout: int,
    task_fetch_size: int,
    low_priority_threshold: int,
    sample_rate: int,
) -> None:
    """
    启动工作池以执行模拟任务。

    Args:
        initial_workers (int): 初始工作者数量。
        dry_run (bool): 是否以仿真模式运行。
        worker_timeout (int): 工作者健康检查超时时间（秒）。
        task_fetch_size (int): 每次从任务提供者获取的任务数量。
        low_priority_threshold (int): 低优先级任务提升阈值。

    Returns:
        None

    Raises:
        Exception: 如果工作池启动过程中发生错误。
    """
    await logger.ainfo(
        f"启动工作池，参数: initial_workers={initial_workers}, dry_run={dry_run}, "
        f"worker_timeout={worker_timeout}, task_fetch_size={task_fetch_size}, "
        f"low_priority_threshold={low_priority_threshold}",
        emoji="⚙️",
    )
    await task_start_worker_pool(
        initial_workers=initial_workers,
        dry_run=dry_run,
        worker_timeout=worker_timeout,
        task_fetch_size=task_fetch_size,
        low_priority_threshold=low_priority_threshold,
        sample_rate=sample_rate,
    )


if __name__ == "__main__":
    asyncio.run(cli())
