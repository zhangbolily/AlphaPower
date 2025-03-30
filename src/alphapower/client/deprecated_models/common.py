"""
Base model for AlphaPower API client.
"""

from typing import List

from multidict import CIMultiDictProxy
from pydantic import BaseModel


class RateLimit:
    """
    表示 API 的速率限制信息。
    """

    def __init__(self, limit: int, remaining: int, reset: int) -> None:
        self.limit: int = limit
        self.remaining: int = remaining
        self.reset: int = reset

    @classmethod
    def from_headers(cls, headers: CIMultiDictProxy[str]) -> "RateLimit":
        """
        从响应头中创建 RateLimit 实例。
        :param headers: 响应头
        :return: RateLimit 实例
        """

        limit: int = int(headers.get("RateLimit-Limit", 0))
        remaining: int = int(headers.get("RateLimit-Remaining", 0))
        reset: int = int(headers.get("RateLimit-Reset", 0))
        return cls(limit, remaining, reset)

    def __str__(self) -> str:
        return f"RateLimit(limit={self.limit}, remaining={self.remaining}, reset={self.reset})"


class TableSchema(BaseModel):
    """
    表示记录的模式。
    """

    name: str
    title: str
    properties: List["TableSchema.Property"]

    class Property(BaseModel):
        """
        表示模式中的一个属性。
        """

        name: str
        title: str
        type: str
