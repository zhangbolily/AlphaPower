import click
from command.sync_datasets import sync_datasets
from command.sync_alphas import sync_alphas
from command.sync_datafields import sync_datafields
from datetime import datetime


@click.group()
def cli():
    pass


@cli.group()
def sync():
    """同步命令组"""
    pass


@sync.command()
@click.option("--dataset_id", default=None, help="数据集ID")
@click.option("--region", default=None, help="区域")
@click.option("--universe", default=None, help="宇宙")
@click.option("--delay", default=None, help="延迟")
def datasets(dataset_id, region, universe, delay):
    """同步数据集"""
    sync_datasets(dataset_id=dataset_id, region=region, universe=universe, delay=delay)


@sync.command()
@click.option("--start_time", default=None, help="开始时间")
@click.option("--end_time", default=None, help="结束时间")
def alphas(start_time, end_time):
    """同步因子"""

    def parse_date(date_str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"日期格式不支持: {date_str}")

    if start_time:
        start_time = parse_date(start_time)
    if end_time:
        end_time = parse_date(end_time)

    sync_alphas(start_time=start_time, end_time=end_time)


@sync.command()
@click.option("--instrument_type", default="EQUITY", help="工具类型")
@click.option("--parallel", default=5, help="并行数 默认为5")
def datafields(instrument_type, parallel):
    """同步数据字段"""
    sync_datafields(instrument_type, parallel)


if __name__ == "__main__":
    cli()
