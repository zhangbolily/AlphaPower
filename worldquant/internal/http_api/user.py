from .common import *
import json
import requests


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


def authentication(session: requests.Session, username: str, password: str):
    """
    进行用户认证。

    参数:
    session (requests.Session): 用于发送HTTP请求的会话对象。
    username (str): 用户名。
    password (str): 密码。

    返回:
    Authentication: 认证响应对象。

    异常:
    requests.exceptions.HTTPError: 如果HTTP请求返回错误状态码。
    """
    url = f"{BASE_URL}/{ENDPOINT_AUTHENTICATION}"
    response = session.post(url, auth=(username, password))
    response.raise_for_status()
    return Authentication.from_json(response.content)
