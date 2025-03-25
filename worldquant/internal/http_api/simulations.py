import aiohttp
from .common import *
from .model import *


async def create_single_simulation(session: aiohttp.ClientSession, simulation_data):
    url = f"{BASE_URL}/{ENDPOINT_SIMULATION}"
    async with session.post(url, json=simulation_data) as response:
        response.raise_for_status()
        if response.status == 201:
            # 创建成功，返回模拟进度轮询地址、轮询间隔时间
            return (True, response.headers["Location"], response.headers["Retry-After"])
        return (False, "", 0)


async def get_single_simulation_progress(session: aiohttp.ClientSession, progress_url):
    """
    从给定的URL获取单次模拟的进度或结果。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        progress_url (str): 获取模拟进度或结果的URL。

    返回:
        tuple: 包含以下内容的元组:
            - finished (bool): 模拟是否完成。
            - progress_or_result (SingleSimulationProgress 或 SingleSimulationResult):
              如果模拟仍在运行，则为模拟进度；如果模拟已完成，则为模拟结果。
            - retry_after (int): 如果模拟仍在运行，则为重试前等待的秒数。
    """
    async with session.get(progress_url) as response:
        response.raise_for_status()

        if response.headers.get("Retry-After") is not None:
            # 模拟中，返回模拟进度
            retry_after = response.headers["Retry-After"]
            finished = False
            return (
                finished,
                SingleSimulationProgress.from_json(await response.text()),
                retry_after,
                RateLimit.from_headers(response.headers),
            )
        else:
            # 模拟完成，返回模拟结果
            finished = True
            return (
                finished,
                SingleSimulationResult.from_json(await response.text()),
                0,
                RateLimit.from_headers(response.headers),
            )


async def get_self_simulation_activities(session: aiohttp.ClientSession, date: str):
    url = f"{BASE_URL}/{ENDPOINT_ACTIVITIES_SIMULATION}"
    async with session.get(url, params={"date": date}) as response:
        response.raise_for_status()
        return SelfSimulationActivities.from_json(await response.text())
