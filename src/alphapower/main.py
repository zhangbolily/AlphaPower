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
from typing import List, Optional, Tuple

import asyncclick as click  # 替换为 asyncclick
import pytz

from alphapower.client.worldquant_brain_client import WorldQuantBrainClientFactory
from alphapower.constants import MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY, Status
from alphapower.dal.session_manager import session_manager
from alphapower.internal.logging import get_logger
from alphapower.internal.utils import safe_async_run
from alphapower.manager.alpha_manager import AlphaManagerFactory
from alphapower.manager.alpha_profile_manager import AlphaProfileManagerFactory
from alphapower.manager.data_sets_manager import DataSetsManagerFactory
from alphapower.manager.fast_expression_manager import FastExpressionManagerFactory
from alphapower.manager.options_manager import OptionsManagerFactory
from alphapower.services.alpha import AlphaServiceFactory
from alphapower.services.alpha_abc import AbstractAlphaService
from alphapower.services.alpha_profiles import AlphaProfilesServiceFactory
from alphapower.services.alpha_profiles_abc import AbstractAlphaProfilesService
from alphapower.services.datasets import DatasetsServiceFactory
from alphapower.services.datasets_abc import AbstractDatasetsService
from alphapower.services.sync_alphas import AlphaSyncService
from alphapower.services.sync_datafields import sync_datafields
from alphapower.services.sync_datasets import sync_datasets
from alphapower.services.task_worker_pool import task_start_worker_pool
from alphapower.settings import settings

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
    await session_manager.dispose_all()
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
async def alpha() -> None:
    """
    因子命令组。

    提供用于同步因子的子命令。

    Returns:
        None
    """
    await logger.adebug("因子命令组初始化完成。")


@alpha.group()
async def profiles() -> None:
    """
    因子配置文件命令组。

    提供用于构建和管理因子配置文件的子命令。

    Returns:
        None
    """
    await logger.adebug("因子配置文件命令组初始化完成。")


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
    "--status",
    default=None,
    type=click.Choice(list(Status.__members__.keys())),
    help="阶段",
)
@click.option(
    "--increamental", is_flag=True, default=False, help="增量同步，默认为全量同步"
)
@click.option("--parallel", default=5, type=int, help="并行数 默认为5")
@click.option("--dry_run", is_flag=True, default=False, help="仿真模式，默认为 False")
@click.option(
    "--max_count_per_loop",
    default=MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
    type=int,
    help=f"每次循环的最大计数，默认为 {MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY}",
)
async def alphas(
    start_time: Optional[str],
    end_time: Optional[str],
    status: Optional[Status],
    increamental: bool = False,
    parallel: int = 5,
    dry_run: bool = False,
    max_count_per_loop: int = MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY,
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
    if start_time and start_time != "":
        parsed_start_time = parse_date(start_time)
    if end_time and start_time != "":
        parsed_end_time = parse_date(end_time)

    alpha_sync_service: AlphaSyncService = AlphaSyncService()

    await alpha_sync_service.sync_alphas(
        start_time=parsed_start_time,
        end_time=parsed_end_time,
        status=Status(status) if status else None,
        increamental=increamental,
        parallel=parallel,
        dry_run=dry_run,
        max_count_per_loop=max_count_per_loop,
    )
    await logger.ainfo("因子同步完成。", emoji="✅")


@alpha.command()
async def fix() -> None:
    """
    修复因子属性。

    返回:
        None

    异常:
        Exception: 如果修复过程中发生错误。
    """
    await logger.ainfo("开始修复因子属性。", emoji="🔧")
    brain_client_factory: WorldQuantBrainClientFactory = WorldQuantBrainClientFactory(
        username=settings.credential.username,
        password=settings.credential.password,
    )
    alpha_manager_factory: AlphaManagerFactory = AlphaManagerFactory(
        brain_client_factory=brain_client_factory,
    )
    alpha_service_factory: AlphaServiceFactory = AlphaServiceFactory(
        alpha_manager_factory=alpha_manager_factory,
    )

    alpha_service: AbstractAlphaService = await alpha_service_factory()
    await alpha_service.fix_alphas_properties()
    await logger.ainfo("因子属性修复完成。", emoji="✅")


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


@sync.command()
@click.option("--start-time", type=click.DateTime(), default=None, help="开始时间")
@click.option("--end-time", type=click.DateTime(), default=None, help="结束时间")
@click.option(
    "--status",
    default=None,
    type=click.Choice(list(Status.__members__.keys())),
    help="状态",
)
@click.option(
    "--incremental", is_flag=True, default=False, help="增量同步，默认为全量同步"
)
@click.option(
    "--aggregate-data-only",
    is_flag=True,
    default=False,
    help="仅同步聚合数据，默认为 False",
)
@click.option("--parallel", default=5, type=int, help="并行数 默认为5")
@click.option("--concurrency", default=1, type=int, help="并发数 默认为1")
async def alphas_v1(
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    status: Optional[Status],
    incremental: bool = False,
    aggregate_data_only: bool = False,
    parallel: int = 5,
    concurrency: int = 1,
) -> None:
    brain_client_factory: WorldQuantBrainClientFactory = WorldQuantBrainClientFactory(
        username=settings.credential.username,
        password=settings.credential.password,
    )
    alpha_manager_factory: AlphaManagerFactory = AlphaManagerFactory(
        brain_client_factory=brain_client_factory,
    )
    alpha_service_factory: AlphaServiceFactory = AlphaServiceFactory(
        alpha_manager_factory=alpha_manager_factory,
    )

    alpha_service: AbstractAlphaService = await alpha_service_factory()

    if incremental:
        await logger.ainfo(
            "增量同步模式，开始同步因子。",
            emoji="⚙️",
        )

        await alpha_service.sync_alphas_incremental(
            tz=pytz.timezone("US/Eastern"),
            aggregate_data_only=aggregate_data_only,
            concurrency=concurrency,
        )
        return

    created_time_ranges: List[Tuple[datetime, datetime]] = []
    if start_time and end_time:
        current_start_time = start_time
        while current_start_time < end_time:
            next_start_time = current_start_time + timedelta(days=1)
            current_end_time = min(next_start_time, end_time)
            created_time_ranges.append((current_start_time, current_end_time))
            current_start_time = next_start_time
    else:
        # TODO: 临时解决方案，后续需要改进
        created_time_ranges.append((datetime.fromtimestamp(0), datetime.now()))
        await logger.ainfo(
            "没有提供开始时间和结束时间，使用默认值。",
            emoji="⚠️",
        )

    await alpha_service.sync_alphas_in_ranges(
        tz=pytz.timezone("US/Eastern"),
        created_time_ranges=created_time_ranges,
        status_eq=status,
        parallel=parallel,
        aggregate_data_only=aggregate_data_only,
        concurrency=concurrency,
    )


@sync.command()
@click.option(
    "--parallel",
    default=1,
    type=int,
    help="并行数，默认为 1",
)
async def datasets_v1(parallel: int) -> None:
    """
    同步数据集。

    返回:
        None

    异常:
        Exception: 如果同步过程中发生错误。
    """
    brain_client_factory: WorldQuantBrainClientFactory = WorldQuantBrainClientFactory(
        username=settings.credential.username,
        password=settings.credential.password,
    )
    data_sets_manager_factory: DataSetsManagerFactory = DataSetsManagerFactory(
        brain_client_factory=brain_client_factory,
    )
    options_manager_factory: OptionsManagerFactory = OptionsManagerFactory(
        brain_client_factory=brain_client_factory,
    )
    data_sets_service_factory: DatasetsServiceFactory = DatasetsServiceFactory(
        datasets_manager_factory=data_sets_manager_factory,
        options_manager_factory=options_manager_factory,
    )

    datasets_service: AbstractDatasetsService = await data_sets_service_factory()

    await datasets_service.sync_datasets(
        category=None,
        delay=None,
        instrument_type=None,
        limit=None,
        offset=None,
        region=None,
        universe=None,
        data_sets_manager_factory=data_sets_manager_factory,
        parallel=parallel,
    )


@simulation.command()
@click.option("--initial-workers", default=1, help="初始工作者数量")
@click.option("--dry-run", is_flag=True, help="以仿真模式运行，不实际执行任务")
@click.option("--worker-timeout", default=300, help="工作者健康检查超时时间（秒）")
@click.option("--task-fetch-size", default=10, help="每次从任务提供者获取的任务数量")
@click.option("--sample-rate", default=1, help="任务跳采样率")
@click.option("--low-priority-threshold", default=10, help="低优先级任务提升阈值")
@click.option(
    "--cursor",
    default=0,
    help="任务提供者的游标，默认为 0",
)
async def start_worker_pool(
    initial_workers: int,
    dry_run: bool,
    worker_timeout: int,
    task_fetch_size: int,
    low_priority_threshold: int,
    sample_rate: int,
    cursor: int,
) -> None:
    """
    启动工作池以执行模拟任务。

    参数:
        initial_workers (int): 初始工作者数量。
        dry_run (bool): 是否以仿真模式运行。
        worker_timeout (int): 工作者健康检查超时时间（秒）。
        task_fetch_size (int): 每次从任务提供者获取的任务数量。
        low_priority_threshold (int): 低优先级任务提升阈值。
        sample_rate (int): 跳采样率（sample rate）。
        cursor (int): 任务游标（cursor）。

    返回:
        None

    异常:
        Exception: 如果工作池启动过程中发生错误。
    """
    # INFO 日志，记录方法进入
    await logger.ainfo(
        "启动模拟任务工作池。",
        emoji="⚙️",
        initial_workers=initial_workers,
        dry_run=dry_run,
        worker_timeout=worker_timeout,
        task_fetch_size=task_fetch_size,
        low_priority_threshold=low_priority_threshold,
        sample_rate=sample_rate,
        cursor=cursor,
    )
    try:
        await task_start_worker_pool(
            initial_workers=initial_workers,
            dry_run=dry_run,
            worker_timeout=worker_timeout,
            task_fetch_size=task_fetch_size,
            low_priority_threshold=low_priority_threshold,
            sample_rate=sample_rate,
            cursor=cursor,
        )
        # INFO 日志，记录方法退出
        await logger.ainfo("模拟任务工作池结束。", emoji="✅")
    except Exception as e:
        # ERROR 日志，记录异常和堆栈
        await logger.aerror(
            f"模拟任务工作池启动失败，原因: {e}",
            emoji="❌",
            exc_info=True,
        )
        raise


@profiles.command()
@click.option(
    "--date_created_gt",
    type=click.DateTime(),
    default=None,
    help="创建时间大于等于",
)
@click.option(
    "--date_created_lt",
    type=click.DateTime(),
    default=None,
    help="创建时间小于等于",
)
@click.option(
    "--parallel",
    default=1,
    type=int,
    help="并行数，默认为 1",
)
async def build(
    date_created_gt: Optional[datetime],
    date_created_lt: Optional[datetime],
    parallel: int = 1,
) -> None:
    """
    构建因子配置文件。

    Args:
        date_created_gt (Optional[datetime]): 创建时间大于等于。
        date_created_lt (Optional[datetime]): 创建时间小于等于。
        parallel (int): 并行数，默认为 1。

    Returns:
        None

    Raises:
        Exception: 如果构建过程中发生错误。
    """
    await logger.ainfo(
        f"开始构建因子配置文件，参数: date_created_gt={date_created_gt}, "
        f"date_created_lt={date_created_lt}, parallel={parallel}",
        emoji="🛠️",
    )

    alpha_manager_factory: AlphaManagerFactory = AlphaManagerFactory(
        brain_client_factory=WorldQuantBrainClientFactory(
            username=settings.credential.username,
            password=settings.credential.password,
        )
    )
    fast_expression_manager_factory: FastExpressionManagerFactory = (
        FastExpressionManagerFactory(agent=None)
    )
    alpha_profile_manager_factory: AlphaProfileManagerFactory = (
        AlphaProfileManagerFactory()
    )

    alpha_profile_service: AbstractAlphaProfilesService = (
        await AlphaProfilesServiceFactory(
            alpha_manager_factory=alpha_manager_factory,
            fast_expression_manager_factory=fast_expression_manager_factory,
            alpha_profile_manager_factory=alpha_profile_manager_factory,
        )()
    )

    await alpha_profile_service.build_alpha_profiles(
        fast_expression_manager_factory=fast_expression_manager_factory,
        alpha_profile_manager_factory=alpha_profile_manager_factory,
        date_created_gt=date_created_gt,
        date_created_lt=date_created_lt,
        parallel=parallel,
    )


if __name__ == "__main__":
    asyncio.run(cli())
