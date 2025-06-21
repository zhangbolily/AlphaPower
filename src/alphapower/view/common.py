from typing import Any, Dict, List, Optional

from pydantic import BaseModel, TypeAdapter
from structlog.stdlib import BoundLogger

from alphapower.constants import Region
from alphapower.internal.logging import get_logger

# 全局复用日志对象，避免每次调用都新建
logger: BoundLogger = get_logger(__name__)

RegionListAdaptor: TypeAdapter[List[Region]] = TypeAdapter(
    List[Region],
)


class QueryBase(BaseModel):
    limit: Optional[int] = None
    offset: Optional[int] = None

    def to_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = self.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude_unset=True,
        )
        return params


class PayloadBase(BaseModel):
    # 将对象转换为可被官方 json 库序列化的字典对象
    def to_serializable_dict(self) -> Dict[str, Any]:
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
        )


class RateLimit(BaseModel):
    limit: int
    remaining: int
    reset: int
    # 新增字段，标记是否有 RateLimit 信息
    available: bool = False

    @classmethod
    def from_headers(cls, headers: Dict[str, str]) -> Optional["RateLimit"]:
        """
        从响应头中解析 RateLimit 信息。
        如果没有相关字段，返回 None，调用方可感知无速率限制信息。
        """
        lower_headers: Dict[str, str] = {k.lower(): v for k, v in headers.items()}

        limit_str: Optional[str] = lower_headers.get("ratelimit-limit", None)
        remaining_str: Optional[str] = lower_headers.get("ratelimit-remaining", None)
        reset_str: Optional[str] = lower_headers.get("ratelimit-reset", None)

        # 精简日志，仅在调试模式下输出参数
        logger.debug(
            "解析 RateLimit 响应头参数",
            limit_str=limit_str,
            remaining_str=remaining_str,
            reset_str=reset_str,
            emoji="🪪",
        )

        # 如果缺少任一字段，认为没有 RateLimit 信息
        if limit_str is None or remaining_str is None or reset_str is None:
            logger.debug(
                "未检测到 RateLimit 相关响应头，速率限制信息不可用", emoji="🚫"
            )
            return None

        try:
            limit: int = int(limit_str)
            remaining: int = int(remaining_str)
            reset: int = int(reset_str)
        except ValueError:
            logger.warning("RateLimit 响应头字段解析失败，存在非整数内容", emoji="⚠️")
            return None

        logger.debug(
            "成功解析 RateLimit 信息",
            limit=limit,
            remaining=remaining,
            reset=reset,
            emoji="✅",
        )
        return cls(limit=limit, remaining=remaining, reset=reset, available=True)

    def __str__(self) -> str:
        # 中文注释：格式化输出速率限制信息
        return (
            f"RateLimit(limit={self.limit}, remaining={self.remaining}, "
            f"reset={self.reset}, available={self.available})"
        )
