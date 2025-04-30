from typing import List

from pydantic import BaseModel

from alphapower.constants import UserPermission


class AuthenticationView(BaseModel):

    class User(BaseModel):

        id: str

    class Token(BaseModel):

        expiry: float

    user: User
    token: Token
    permissions: List[UserPermission]
