"""
用户认证模块
"""

from aiohttp import ClientSession

from ...client import AuthenticationView
from .common import BASE_URL, ENDPOINT_AUTHENTICATION


async def authentication(session: ClientSession) -> AuthenticationView:
    """
    进行用户认证。

    参数:
    session (ClientSession): 用于发送HTTP请求的会话对象。

    返回:
    Authentication: 认证响应对象。
    """
    url = f"{BASE_URL}/{ENDPOINT_AUTHENTICATION}"
    response = await session.post(url)
    response.raise_for_status()
    return AuthenticationView.model_validate(await response.json())
