"""
入口文件
"""

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import click

from .internal.logging import setup_logging
from .internal.storage import close_resources
from .services.sync_alphas import sync_alphas
from .services.sync_datafields import sync_datafields
from .services.sync_datasets import sync_datasets

logger = setup_logging(__name__)


async def handle_exit_signal(signum: int, frame: Optional[signal.FrameType]) -> None:
    """
    处理退出信号的异步函数
    :param signum: 信号编号
    :param frame: 信号处理的当前帧
    """
    logger.info("接收到信号 %s，正在进行清理工作...", signum)
    # 在这里执行清理工作，例如关闭数据库连接、释放资源等
    await close_resources()  # 调用关闭资源的函数
    sys.exit(0)


# 注册信号处理函数
signal.signal(signal.SIGINT, handle_exit_signal)  # 处理 Ctrl+C
signal.signal(signal.SIGTERM, handle_exit_signal)  # 处理终止信号


@click.group()
def cli() -> None:
    logger.debug("CLI 初始化完成")


@cli.group()
def sync() -> None:
    """同步命令组"""
    logger.debug("同步命令组初始化完成")


@sync.command()
@click.option("--region", default=None, help="区域")
@click.option("--universe", default=None, help="股票池")
@click.option("--delay", default=None, help="延迟")
@click.option("--parallel", default=5, help="并行数 默认为5")
def datasets(
    region: Optional[str], universe: Optional[str], delay: Optional[int], parallel: int
) -> None:
    """同步数据集"""
    asyncio.run(
        sync_datasets(
            region=region,
            universe=universe,
            delay=delay,
            parallel=parallel,
        )
    )


@sync.command()
@click.option("--start_time", default=None, help="开始时间")
@click.option("--end_time", default=None, help="结束时间")
@click.option("--parallel", default=5, help="并行数 默认为5")
def alphas(start_time: Optional[str], end_time: Optional[str], parallel: int) -> None:
    """同步因子"""

    def parse_date(date_str: str) -> datetime:
        est = timezone(timedelta(hours=-5))  # 定义 EST 时区
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).replace(tzinfo=est)
            except ValueError:
                continue
        raise ValueError(f"日期格式不支持: {date_str}")

    if start_time:
        parsed_start_time: datetime = parse_date(start_time)
    if end_time:
        parsed_start_time: datetime = parse_date(end_time)

    asyncio.run(
        sync_alphas(
            start_time=parsed_start_time, end_time=parsed_start_time, parallel=parallel
        )
    )


@sync.command()
@click.option("--instrument_type", default="EQUITY", help="工具类型")
@click.option("--dataset_id", default=None, help="数据集ID")
@click.option("--parallel", default=5, help="并行数 默认为5")
def datafields(instrument_type: str, dataset_id: Optional[str], parallel: int) -> None:
    """同步数据字段"""
    asyncio.run(
        sync_datafields(
            instrument_type=instrument_type,
            dataset_id=dataset_id,
            parallel=parallel,
        )
    )


if __name__ == "__main__":
    cli()
