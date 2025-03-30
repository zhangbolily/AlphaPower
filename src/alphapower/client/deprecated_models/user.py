"""
身份验证视图模型
"""

from typing import List

from pydantic import BaseModel


class AuthenticationView(BaseModel):
    """
    表示身份验证视图的主类，包含用户信息、令牌和权限。
    """

    user: "AuthenticationView.User"
    token: "AuthenticationView.Token"
    permissions: List[str]

    class User(BaseModel):
        """
        表示用户信息的嵌套类。
        """

        id: str

    class Token(BaseModel):
        """
        表示令牌信息的嵌套类。
        """

        expiry: float
