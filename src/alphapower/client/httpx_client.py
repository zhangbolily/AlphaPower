import asyncio
import time
import traceback
from typing import Any, Dict, Optional, Tuple, TypeVar, Union

import httpx
from pydantic import BaseModel
from structlog.stdlib import BoundLogger

from alphapower.constants import RETRYABLE_HTTP_CODES
from alphapower.internal.logging import get_logger
from alphapower.view.common import RateLimit

T = TypeVar("T", bound=BaseModel)  # 泛型约束为 BaseModel 子类


class HttpXClient:
    """HttpXClient 封装底层 HTTP 请求，支持限流重试、超时重试、JSON 反序列化、接口级本地速率限制等功能"""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 1.5,
        max_retry_after: float = 60.0,
        headers: Optional[Dict[str, str]] = None,
        module_name: str = __name__,
    ) -> None:
        self._log: BoundLogger = get_logger(module_name=module_name)
        self._base_url: str = base_url
        self._timeout: float = timeout
        self._max_retries: int = max_retries
        self._backoff_factor: float = backoff_factor
        self._max_retry_after: float = max_retry_after
        self._headers: Dict[str, str] = headers or {}
        self._client: Optional[httpx.AsyncClient] = None

        # 接口级本地速率限制参数，key 为 api_name，value 为 (RateLimit, 上次更新时间)
        self._rate_limit_map: Dict[str, tuple[RateLimit, float]] = {}
        # 协程安全锁，保护 _rate_limit_map
        self._rate_limit_lock: asyncio.Lock = asyncio.Lock()
        # 新增：保护 _client 的协程安全锁
        self._client_lock: asyncio.Lock = asyncio.Lock()

    async def __aenter__(self) -> "HttpXClient":
        """
        协程安全地初始化 AsyncClient，防止并发场景下重复初始化
        """
        async with self._client_lock:
            if self._client is None:
                self._client = httpx.AsyncClient(
                    base_url=self._base_url,
                    timeout=self._timeout,
                    headers=self._headers,
                )
                await self._log.ainfo(
                    "HttpXClient 初始化完成", emoji="🚀", base_url=self._base_url
                )
            else:
                await self._log.adebug(
                    "HttpXClient 已存在，跳过初始化", emoji="⚠️", base_url=self._base_url
                )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        协程安全地关闭 AsyncClient，防止并发场景下重复关闭
        """
        async with self._client_lock:
            if self._client:
                try:
                    await self._client.aclose()
                    await self._log.ainfo("HttpXClient 已销毁", emoji="🛑")
                except Exception as e:
                    await self._log.aerror(
                        "HttpXClient 关闭异常", emoji="💥", error=str(e)
                    )
                self._client = None
            else:
                await self._log.adebug(
                    "HttpXClient 已销毁或未初始化，跳过关闭", emoji="⚠️"
                )

    async def _wait_for_rate_limit(self, api_name: str) -> None:
        """
        针对接口名称的本地速率限制（rate limit）检查与等待。
        仅当 RateLimit.available=True 时生效。
        """
        async with self._rate_limit_lock:
            rate_limit_info: Optional[tuple[RateLimit, float]] = (
                self._rate_limit_map.get(api_name)
            )
            if rate_limit_info is None:
                return  # 没有本地限流信息，直接通过

            rate_limit: RateLimit
            last_update: float
            rate_limit, last_update = rate_limit_info

            if not getattr(rate_limit, "available", False):
                return  # 限流信息不可用，跳过本地限流

            now: float = time.time()
            reset_time: float = last_update + rate_limit.reset
            if rate_limit.remaining <= 0:
                wait_seconds: float = max(0.0, reset_time - now)
                if wait_seconds > 0:
                    await self._log.awarning(
                        "接口本地速率限制额度已用尽，等待下一个周期",
                        emoji="⏳",
                        wait_seconds=wait_seconds,
                        api_name=api_name,
                        rate_limit_limit=rate_limit.limit,
                        rate_limit_remaining=rate_limit.remaining,
                        rate_limit_reset=rate_limit.reset,
                    )
                    await asyncio.sleep(wait_seconds)
                    # 重置本地额度，避免负数
                    rate_limit.remaining = max(rate_limit.limit, 0)
                    self._rate_limit_map[api_name] = (rate_limit, time.time())
                    await self._log.ainfo(
                        "接口本地速率限制额度已重置",
                        emoji="🔄",
                        api_name=api_name,
                        rate_limit_limit=rate_limit.limit,
                        rate_limit_remaining=rate_limit.remaining,
                        rate_limit_reset=rate_limit.reset,
                    )

    async def _update_rate_limit_from_headers(
        self, api_name: str, headers: Dict[str, str]
    ) -> None:
        """
        从响应头更新接口级本地速率限制信息，仅当 RateLimit.available=True 时生效。
        响应头大小写不敏感，需全部转小写。
        """
        try:
            # 兼容大小写，全部转小写
            lower_headers: Dict[str, str] = {k.lower(): v for k, v in headers.items()}
            rate_limit: Optional[RateLimit] = RateLimit.from_headers(lower_headers)
            if rate_limit and rate_limit.available:
                async with self._rate_limit_lock:
                    self._rate_limit_map[api_name] = (rate_limit, time.time())
                await self._log.adebug(
                    "更新接口本地速率限制信息",
                    emoji="📊",
                    api_name=api_name,
                    rate_limit_limit=rate_limit.limit,
                    rate_limit_remaining=rate_limit.remaining,
                    rate_limit_reset=rate_limit.reset,
                )
        except Exception as e:
            await self._log.awarning(
                "解析速率限制响应头失败，跳过本地限流",
                emoji="⚠️",
                error=str(e),
                api_name=api_name,
                headers=dict(headers),
            )

    async def _handle_rate_limit(self, api_name: str) -> None:
        """处理本地速率限制逻辑"""
        await self._wait_for_rate_limit(api_name)

    async def _handle_http_error_response(
        self,
        resp: httpx.Response,
        url: str,
        retry_on_status: Tuple[int, ...],
        retry_after_override: Optional[float],
    ) -> Optional[float]:
        """
        处理 HTTP 异常状态码响应
        返回：若需重试则返回 retry_after 秒，否则返回 None
        """
        # 检查是否为可重试状态码
        if resp.status_code in retry_on_status:
            retry_after: float = (
                retry_after_override
                if retry_after_override is not None
                else self._get_retry_after(resp)
            )
            if retry_after > self._max_retry_after:
                await self._log.awarning(
                    "重试等待时间过长，放弃重试",
                    emoji="⏳",
                    retry_after=retry_after,
                )
                try:
                    resp.raise_for_status()
                except Exception as e:
                    await self._log.aerror(
                        "HTTP 响应状态码异常",
                        emoji="🚨",
                        status_code=resp.status_code,
                        url=url,
                        error=str(e),
                    )
                    raise
                return None
            await self._log.awarning(
                "检测到限流或服务不可用，准备重试",
                emoji="🔁",
                status_code=resp.status_code,
                retry_after=retry_after,
            )
            return retry_after
        else:
            await self._log.aerror(
                "HTTP 响应状态码异常",
                emoji="🚨",
                status_code=resp.status_code,
                url=url,
                text=(resp.text[:200] + "..." if len(resp.text) > 200 else resp.text),
            )
            try:
                resp.raise_for_status()
            except Exception as e:
                await self._log.aerror(
                    "HTTP 响应状态码异常",
                    emoji="🚨",
                    status_code=resp.status_code,
                    url=url,
                    error=str(e),
                )
                raise
            return None

    async def _handle_response_json(
        self,
        resp: httpx.Response,
        response_model: Optional[type[T]],
    ) -> Union[T, Any]:
        """
        处理响应 JSON 内容，支持模型反序列化
        """
        data: str = resp.text
        await self._log.adebug(
            "响应 JSON 内容",
            emoji="📝",
            data=((str(data)[:200] + "...") if len(str(data)) > 200 else data),
        )
        if response_model:
            try:
                # 注意：model_validate 接收 dict，需先解析 JSON 字符串
                json_obj: Any = resp.json()
                obj: T = response_model.model_validate(json_obj)
                await self._log.adebug(
                    "反序列化为模型成功",
                    emoji="✅",
                    model=response_model.__name__,
                )
                return obj
            except Exception as e:
                await self._log.aerror(
                    "模型反序列化失败",
                    emoji="❌",
                    error=str(e),
                    data=((str(data)[:200] + "...") if len(str(data)) > 200 else data),
                )
                raise
        else:
            await self._log.adebug(
                "未指定响应模型，直接返回原始 JSON",
                emoji="🔄",
            )
            try:
                json_data: Any = resp.json()
                return json_data
            except Exception as e:
                await self._log.aerror(
                    "JSON 解析失败",
                    emoji="❌",
                    error=str(e),
                    data=((str(data)[:200] + "...") if len(str(data)) > 200 else data),
                )
                raise

    async def _handle_request_exception(self, e: Exception, retries: int) -> None:
        """处理请求异常（超时、网络异常等）"""
        await self._log.awarning(
            "请求超时或网络异常，准备退避（backoff，退避算法）重试",
            emoji="⏱️",
            error=str(e),
            retries=retries,
            stack=traceback.format_exc(),
        )

    async def _handle_unknown_exception(self, e: Exception) -> None:
        """处理未知异常"""
        await self._log.aerror(
            "请求发生未知异常",
            emoji="💥",
            error=str(e),
            stack=traceback.format_exc(),
        )

    async def request(
        self,
        method: str,
        url: str,
        api_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_on_status: Tuple[int, ...] = tuple(RETRYABLE_HTTP_CODES),
        response_model: Optional[type[T]] = None,
        response_json: bool = True,
        retry_after_override: Optional[float] = None,
        basic_auth: Optional[httpx.BasicAuth] = None,  # 新增 BasicAuth 参数
        **kwargs: Any,
    ) -> Union[T, Any, httpx.Response]:
        """
        通用异步请求方法，支持限流重试、超时重试、JSON 反序列化、接口级本地速率限制。
        支持 BasicAuth（基础认证）参数传递。
        """
        if self._client is None:
            raise RuntimeError(
                "HttpXClient 未初始化，请使用 async with 语法"
            )  # 中文异常
        if not api_name:
            raise ValueError("必须传递 api_name 参数用于本地限流唯一标识")  # 中文异常
        retries: int = 0
        last_exc: Optional[Exception] = None

        merged_headers: Dict[str, str] = dict(self._headers)
        if headers:
            merged_headers.update(headers)

        while retries <= self._max_retries:
            await self._handle_rate_limit(api_name)
            try:
                await self._log.adebug(
                    "发起 HTTP 请求",
                    emoji="🌐",
                    method=method,
                    url=url,
                    api_name=api_name,
                    params=params,
                    json=json,
                    headers={k: v for k, v in merged_headers.items()},
                    retries=retries,
                    basic_auth=bool(basic_auth),  # 记录是否使用 BasicAuth
                )
                # 处理 BasicAuth（基础认证）参数
                request_kwargs: Dict[str, Any] = dict(
                    params=params,
                    json=json,
                    headers=merged_headers,
                    **kwargs,
                )
                if basic_auth is not None:
                    request_kwargs["auth"] = basic_auth

                resp: httpx.Response = await self._client.request(
                    method,
                    url,
                    **request_kwargs,
                )
                await self._log.adebug(
                    "收到 HTTP 响应",
                    emoji="📩",
                    status_code=resp.status_code,
                    headers={k: v for k, v in dict(resp.headers).items()},
                )
                await self._update_rate_limit_from_headers(api_name, dict(resp.headers))

                # 判断响应状态码
                if resp.status_code == 200:
                    if response_json:
                        return await self._handle_response_json(resp, response_model)
                    else:
                        return resp
                else:
                    retry_after: Optional[float] = (
                        await self._handle_http_error_response(
                            resp,
                            url,
                            retry_on_status,
                            retry_after_override,
                        )
                    )
                    if retry_after is not None:
                        await self._log.adebug(
                            "重试等待时间",
                            emoji="⏳",
                            retry_after=retry_after,
                            retries=retries,
                        )
                        await asyncio.sleep(retry_after)
                        retries += 1
                        await self._log.adebug(
                            "重试次数增加",
                            emoji="🔁",
                            retries=retries,
                        )
                        continue
                    # 如果未返回 retry_after，说明已抛出异常或无需重试
                    break
            except (httpx.TimeoutException, httpx.RequestError) as e:  # 修正异常类型
                await self._handle_request_exception(e, retries)
                last_exc = e
                backoff_time: float = self._backoff_factor * (2**retries)
                await self._log.adebug(
                    "退避等待时间（backoff）",
                    emoji="⏱️",
                    backoff_time=backoff_time,
                    retries=retries,
                )
                await asyncio.sleep(backoff_time)
                retries += 1
                await self._log.adebug(
                    "重试次数增加",
                    emoji="🔁",
                    retries=retries,
                )
                continue
            except Exception as e:
                await self._handle_unknown_exception(e)
                last_exc = e
                break

        await self._log.aerror(
            "请求重试次数已达上限，抛出异常",
            emoji="❗",
            url=url,
            last_exc=str(last_exc),
            stack=traceback.format_exc() if last_exc else "",
        )
        if last_exc:
            raise last_exc
        raise RuntimeError("请求失败，未知原因")  # 中文异常

    @staticmethod
    def _get_retry_after(resp: httpx.Response) -> float:
        """
        从响应头中提取 Retry-After 字段
        """
        retry_after: Optional[str] = None
        # 响应头大小写不敏感
        for k, v in resp.headers.items():
            if k.lower() == "retry-after":
                retry_after = v
                break
        if retry_after is not None:
            try:
                return float(retry_after)
            except Exception:
                # 解析失败，返回默认值
                return 0.0
        return 0.0
