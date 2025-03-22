from .common import *
import json

from aiohttp import ClientSession


class Authentication_User:
    def __init__(self, id):
        self.id = id


class Authentication_Token:
    def __init__(self, expiry):
        self.expiry = expiry


class Authentication:
    def __init__(self, user, token, permissions):
        self.user = Authentication_User(**user)
        self.token = Authentication_Token(**token)
        self.permissions = permissions

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


async def authentication(session: ClientSession) -> Authentication:
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
    return Authentication.from_json(await response.text())
