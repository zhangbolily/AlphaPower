"""
此模块提供与 AlphaPower 服务交互的原始 API 函数。
"""

from aiohttp import ClientSession

from alphapower.client.models import Operators
from alphapower.client.raw_api.common import BASE_URL, ENDPOINT_OPERATORS


async def get_all_operators(session: ClientSession):
    """
    从 AlphaPower API 获取所有运营商。

    参数:
        session (ClientSession): aiohttp.ClientSession 的实例，用于发起 HTTP 请求。

    返回:
        Operators: 一个 Operators 模型实例，包含从 API 响应中获取的数据。

    异常:
        aiohttp.ClientResponseError: 如果 HTTP 请求失败或返回错误状态。
    """
    url = f"{BASE_URL}/{ENDPOINT_OPERATORS}"
    async with session.get(url) as response:
        response.raise_for_status()
        return Operators.from_json(await response.text())
