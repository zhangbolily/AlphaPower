"""
提供与模拟回测相关的HTTP API功能。
"""

from typing import Any, List, Union

import aiohttp

from .common import BASE_URL, ENDPOINT_ACTIVITIES_SIMULATION, ENDPOINT_SIMULATION
from .model import (
    MultiSimulationResult,
    SelfSimulationActivities,
    SimulationProgress,
    SingleSimulationResult,
)


async def create_single_simulation(
    session: aiohttp.ClientSession, simulation_data: dict[str, Any]
) -> tuple[bool, str, float]:
    """
    创建单次模拟。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        simulation_data (dict[str, Any]): 模拟所需的数据。

    返回:
        tuple: 包含以下内容的元组:
            - success (bool): 是否成功创建模拟。
            - progress_id (str): 模拟进度的唯一标识符。
            - retry_after (float): 重试前等待的秒数。
    """
    url = f"{BASE_URL}/{ENDPOINT_SIMULATION}"
    async with session.post(url, json=simulation_data) as response:
        response.raise_for_status()
        if response.status == 201:
            # 创建成功，返回模拟进度轮询地址、轮询间隔时间
            progress_id: str = response.headers["Location"].split("/")[-1]
            retry_after: float = float(response.headers["Retry-After"])
            return (
                True,
                progress_id,
                retry_after,
            )
        return (False, "", 0)


async def create_multi_simulation(
    session: aiohttp.ClientSession, simulation_data: List[Any]
) -> tuple[bool, str, float]:
    """
    创建多次模拟。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        simulation_data (List[Any]): 多次模拟所需的数据。

    返回:
        tuple: 包含以下内容的元组:
            - success (bool): 是否成功创建模拟。
            - progress_id (str): 模拟进度的唯一标识符。
            - retry_after (float): 重试前等待的秒数。
    """
    url = f"{BASE_URL}/{ENDPOINT_SIMULATION}"
    async with session.post(url, json=simulation_data) as response:
        response.raise_for_status()
        if response.status == 201:
            # 创建成功，返回模拟进度轮询地址、轮询间隔时间
            progress_id: str = response.headers["Location"].split("/")[-1]
            retry_after: float = float(response.headers["Retry-After"])
            return (
                True,
                progress_id,
                retry_after,
            )
        return (False, "", 0)


async def delete_simulation(session: aiohttp.ClientSession, progress_id: str):
    """
    删除指定的模拟。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        progress_id (str): 要删除的模拟的唯一标识符。
    """
    url = f"{BASE_URL}/{ENDPOINT_SIMULATION}/{progress_id}"
    async with session.delete(url) as response:
        response.raise_for_status()


async def get_simulation_progress(
    session: aiohttp.ClientSession, progress_id: str, is_multi: bool
) -> tuple[
    bool,
    Union[SingleSimulationResult, MultiSimulationResult, SimulationProgress],
    float,
]:
    """
    获取模拟的进度或结果。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        progress_id (str): 模拟进度的唯一标识符。
        is_multi (bool): 是否为多次模拟。

    返回:
        tuple: 包含以下内容的元组:
            - finished (bool): 模拟是否完成。
            - progress_or_result (Union[SingleSimulationResult, MultiSimulationResult, SimulationProgress]):
              模拟的进度或结果。
            - retry_after (float): 如果模拟仍在运行，则为重试前等待的秒数。
    """
    progress_url = f"{BASE_URL}/{ENDPOINT_SIMULATION}/{progress_id}"
    async with session.get(progress_url) as response:
        response.raise_for_status()

        if response.headers.get("Retry-After") is not None:
            # 模拟中，返回模拟进度
            retry_after = float(response.headers["Retry-After"])
            finished = False
            return (
                finished,
                SimulationProgress.from_json(await response.text()),
                retry_after,
            )
        else:
            # 模拟完成，返回模拟结果
            finished = True
            result: Union[SingleSimulationResult, MultiSimulationResult]
            if is_multi:
                result = MultiSimulationResult.from_json(await response.text())
            else:
                result = SingleSimulationResult.from_json(await response.text())
            return (
                finished,
                result,
                0.0,
            )


async def get_self_simulation_activities(session: aiohttp.ClientSession, date: str):
    """
    获取用户的模拟活动。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        date (str): 查询活动的日期。

    返回:
        SelfSimulationActivities: 用户的模拟活动数据。
    """
    url = f"{BASE_URL}/{ENDPOINT_ACTIVITIES_SIMULATION}"
    async with session.get(url, params={"date": date}) as response:
        response.raise_for_status()
        return SelfSimulationActivities.from_json(await response.text())
