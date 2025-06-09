import asyncio
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar, Union

import httpx
from pydantic import BaseModel

from alphapower.constants import (
    MAX_RETRY_RECURSION_DEPTH,
    RETRY_INITIAL_BACKOFF,
    RETRYABLE_HTTP_CODES,
    LoggingEmoji,
)
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import BaseLogger
from alphapower.view.common import RateLimit

T = TypeVar("T", bound=BaseModel)  # 泛型约束为 BaseModel 子类


class RetrayableError(Exception):
    """可重试异常类"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class HttpXClient(BaseLogger):
    """HttpXClient 封装底层 HTTP 请求，支持限流重试、超时重试、JSON 反序列化、接口级本地速率限制等功能"""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 1.5,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._base_url: str = base_url
        self._timeout: float = timeout
        self._max_retries: int = max_retries
        self._backoff_factor: float = backoff_factor
        self._headers: Dict[str, str] = headers or {}
        self._client: httpx.AsyncClient = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._headers,
        )

        # 接口级本地速率限制参数，key 为 api_name，value 为 (RateLimit, 上次更新时间)
        self._rate_limit_map: Dict[str, tuple[RateLimit, datetime]] = {}
        # 协程安全锁，保护 _rate_limit_map
        self._rate_limit_lock: asyncio.Lock = asyncio.Lock()
        # 新增：保护 _client 的协程安全锁
        self._client_lock: asyncio.Lock = asyncio.Lock()

    async def close(self) -> None:
        """显式关闭 HttpXClient，确保资源释放"""
        if self._client:
            async with self._client_lock:
                await self._client.aclose()
                self._client = None  # type: ignore
                await self.log.ainfo(
                    "HttpXClient 已关闭",
                    emoji=LoggingEmoji.DISCONNECT.value,
                    base_url=self._base_url,
                    timeout=self._timeout,
                    max_retries=self._max_retries,
                )

    @async_exception_handler
    async def _wait_for_rate_limit(self, api_name: str) -> None:
        """
        优化锁持有时间：只在访问 _rate_limit_map 时持有锁，等待期间释放锁，避免阻塞其他协程。
        """
        while True:
            # 只在访问 _rate_limit_map 时加锁
            async with self._rate_limit_lock:
                rate_limit_info: Optional[tuple[RateLimit, datetime]] = (
                    self._rate_limit_map.get(api_name)
                )

            if rate_limit_info is None:
                # 没有本地限流信息，直接通过
                await self.log.adebug(
                    "未找到本地速率限制信息，直接通过",
                    emoji=LoggingEmoji.SUCCESS.value,
                    api_name=api_name,
                )
                return

            rate_limit, last_update = rate_limit_info
            if not getattr(rate_limit, "available", False):
                # 限流信息不可用，跳过本地限流
                await self.log.awarning(
                    "本地速率限制信息不可用，跳过限流",
                    emoji=LoggingEmoji.WARNING.value,
                    api_name=api_name,
                )
                return

            await self.log.ainfo(
                "检查接口本地速率限制",
                emoji=LoggingEmoji.INFO.value,
                api_name=api_name,
                rate_limit=rate_limit.model_dump(mode="json"),
            )

            now: float = time.time()
            reset_time: float = last_update.timestamp() + rate_limit.reset
            if rate_limit.remaining > 0:
                # 还有额度，直接通过
                async with self._rate_limit_lock:
                    rate_limit.remaining -= 1
                    self._rate_limit_map[api_name] = (rate_limit, last_update)

                await self.log.adebug(
                    "本地速率限制额度充足，直接通过",
                    emoji=LoggingEmoji.SUCCESS.value,
                    api_name=api_name,
                    rate_limit=rate_limit.model_dump(mode="json"),
                )
                return

            wait_seconds: float = max(0.0, reset_time - now)
            if wait_seconds <= 0:
                # 已到重置时间，重置额度
                rate_limit.remaining = max(rate_limit.limit, 0)
                async with self._rate_limit_lock:
                    self._rate_limit_map[api_name] = (rate_limit, datetime.now())
                await self.log.ainfo(
                    "接口本地速率限制额度已重置",
                    emoji=LoggingEmoji.UPDATE.value,
                    api_name=api_name,
                    rate_limit=rate_limit.model_dump(mode="json"),
                )
                return

            # 需要等待，先输出日志再等待，等待期间不持有锁
            await self.log.awarning(
                "接口本地速率限制额度已用尽，等待下一个周期",
                emoji=LoggingEmoji.WARNING.value,
                wait_seconds=wait_seconds,
                api_name=api_name,
                rate_limit=rate_limit.model_dump(mode="json"),
            )
            await asyncio.sleep(wait_seconds)
            # 等待后重入循环，重新检查限流状态

    async def _create_do_request_func(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        basic_auth: Optional[httpx.BasicAuth] = None,  # 新增 BasicAuth 参数
    ) -> Callable[..., Awaitable[httpx.Response]]:
        """
        创建请求函数，便于重试逻辑复用
        """

        async def do_request() -> httpx.Response:
            await self.log.ainfo(
                event="进入 do_request 函数",
                emoji=LoggingEmoji.STEP_IN_FUNC.value,
                method=method,
                url=url,
            )

            if self._client is None:
                raise RuntimeError("HttpXClient 未初始化或已关闭，请检查代码逻辑")

            try:
                response: httpx.Response = await self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers=headers,
                    auth=basic_auth,  # 使用 BasicAuth 参数
                )
                return response
            except (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.PoolTimeout,
            ) as e:
                error_type = e.__class__.__name__
                error_message = str(e)
                log_message = {
                    "连接错误": "连接错误",
                    "ReadTimeout": "请求超时",
                    "WriteTimeout": "请求写入超时",
                }.get(error_type, "请求失败")

                await self.log.aerror(
                    log_message,
                    emoji=LoggingEmoji.ERROR.value,
                    exception=error_type,
                    error=error_message,
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers={k: v for k, v in headers.items()} if headers else {},
                )
                raise RetrayableError(log_message) from e
            except Exception as e:
                await self.log.aerror(
                    "请求失败",
                    emoji=LoggingEmoji.ERROR.value,
                    exception=e.__class__.__name__,
                    error=str(e),
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers={k: v for k, v in headers.items()} if headers else {},
                )
                raise
            finally:
                await self.log.ainfo(
                    event="退出 do_request 函数",
                    emoji=LoggingEmoji.STEP_OUT_FUNC.value,
                    resp_code=response.status_code if "response" in locals() else None,
                    resp_content_type=(
                        response.headers.get("content-type")
                        if "response" in locals()
                        else None
                    ),
                )

        return do_request

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
        basic_auth: Optional[httpx.BasicAuth] = None,  # 新增 BasicAuth 参数
        **kwargs: Any,
    ) -> Tuple[Union[T, Any, httpx.Response], Optional[float]]:
        await self.log.ainfo(
            event=f"进入 {self.request.__qualname__} 函数",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
            method=method,
            url=url,
        )

        if self._client is None:
            raise RuntimeError(
                "HttpXClient 未初始化或已关闭，请检查代码逻辑"
            )  # 中文异常
        if not api_name:
            raise ValueError("必须传递 api_name 参数用于本地限流唯一标识")  # 中文异常

        merged_headers: Dict[str, str] = dict(self._headers)
        if headers:
            merged_headers.update(headers)

        do_request: Callable[..., Awaitable[httpx.Response]] = (
            await self._create_do_request_func(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=merged_headers,
                basic_auth=basic_auth,  # 传递 BasicAuth 参数
            )
        )

        await self._wait_for_rate_limit(api_name)
        remain_retries: int = self._max_retries
        while remain_retries:
            try:
                resp: httpx.Response = await do_request()
                await self.log.adebug(
                    "收到 HTTP 响应",
                    emoji=LoggingEmoji.HTTP.value,
                    status_code=resp.status_code,
                    headers={k: v for k, v in dict(resp.headers).items()},
                )

                handled_resp, retry_after = await self._handle_response(
                    resp=resp,
                    api_name=api_name,
                    retry_on_status=retry_on_status,
                    response_model=response_model,
                    response_json=response_json,
                    do_request_func=do_request,
                )

                return handled_resp, retry_after
            except RetrayableError as e:
                remain_retries -= 1
                await self.log.aerror(
                    "可重试错误",
                    emoji=LoggingEmoji.ERROR.value,
                    error=str(e),
                    api_name=api_name,
                    remain_retries=remain_retries,
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers={k: v for k, v in merged_headers.items()},
                )
                continue
            except Exception as e:
                await self.log.aerror(
                    "请求失败",
                    emoji=LoggingEmoji.ERROR.value,
                    error=str(e),
                    api_name=api_name,
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers={k: v for k, v in merged_headers.items()},
                )
                raise e
            finally:
                await self.log.ainfo(
                    event=f"退出 {self.request.__qualname__} 函数",
                    emoji=LoggingEmoji.STEP_OUT_FUNC.value,
                    resp_code=resp.status_code if "resp" in locals() else None,
                    resp_content_type=(
                        resp.headers.get("content-type") if "resp" in locals() else None
                    ),
                )

        await self.log.aerror(
            "请求失败，所有重试次数已用尽",
            emoji=LoggingEmoji.ERROR.value,
            api_name=api_name,
            method=method,
            url=url,
            params=params,
            json=json,
            headers={k: v for k, v in merged_headers.items()} if merged_headers else {},
            retry_on_status=retry_on_status,
            response_model=response_model,
            response_json=response_json,
        )
        raise RuntimeError("请求失败，所有重试次数已用尽")  # 中文异常

    @staticmethod
    def _get_retry_after(resp: httpx.Response) -> Optional[float]:
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
                return None
        return None  # 返回 None 以表示没有可用的 retry-after 值

    @async_exception_handler
    async def _handle_response(
        self,
        resp: httpx.Response,
        api_name: str,
        retry_on_status: Tuple[int, ...],
        response_model: Optional[type[T]],
        response_json: bool,
        do_request_func: Callable[..., Awaitable[httpx.Response]],
    ) -> Tuple[Union[T, Any, httpx.Response], Optional[float]]:
        """
        处理响应 JSON 内容，支持模型反序列化
        """
        ensured_resp: Optional[httpx.Response] = await self._handle_response_status(
            resp=resp,
            retry_on_status=retry_on_status,
            response_model=response_model,
            response_json=response_json,
            do_request_func=do_request_func,
            remain_retries=self._max_retries,
            backoff_time=RETRY_INITIAL_BACKOFF,
        )

        if not ensured_resp:
            await self.log.aerror(
                "请求响应状态码异常，未返回响应对象",
                emoji=LoggingEmoji.ERROR.value,
                api_name=api_name,
                resp=resp,
                status_code=resp.status_code,
                ensured_resp=ensured_resp,
                retry_on_status=retry_on_status,
            )
            raise RuntimeError("请求响应状态码异常，未返回响应对象")

        retry_after: Optional[float] = await self._handle_response_headers(
            resp=ensured_resp,
            api_name=api_name,
        )

        if response_json:
            json_obj: Union[T, Any] = await self._handle_response_body(
                resp=ensured_resp,
                response_model=response_model,
            )

            return json_obj, retry_after

        return ensured_resp, retry_after

    async def _handle_response_body(
        self,
        resp: httpx.Response,
        response_model: Optional[type[T]],
    ) -> Union[T, Any]:
        """
        处理响应 JSON 内容，支持模型反序列化
        """
        data: str = resp.text
        await self.log.adebug(
            "响应 JSON 内容",
            emoji=LoggingEmoji.HTTP.value,
            data=((str(data)[:200] + "...") if len(str(data)) > 200 else data),
        )
        if response_model:
            try:
                # 注意：model_validate 接收 dict，需先解析 JSON 字符串
                obj: T = response_model.model_validate_json(resp.text)
                await self.log.adebug(
                    "反序列化为模型成功",
                    emoji=LoggingEmoji.SUCCESS.value,
                    model=response_model.__name__,
                )
                return obj
            except Exception as e:
                await self.log.aerror(
                    "模型反序列化失败",
                    emoji=LoggingEmoji.ERROR.value,
                    response_model=response_model.__name__,
                    content_type=resp.headers.get("content-type"),
                    content_length=resp.headers.get("content-length"),
                    error=str(e),
                    data=(
                        resp.text[:200] + "..." if len(resp.text) > 200 else resp.text
                    ),
                )
                raise
        else:
            await self.log.adebug(
                "未指定响应模型，直接返回原始 JSON",
                emoji="🔄",
            )
            try:
                json_data: Any = resp.json()
                return json_data
            except Exception as e:
                await self.log.aerror(
                    "JSON 解析失败",
                    emoji=LoggingEmoji.ERROR.value,
                    error=str(e),
                    data=((str(data)[:200] + "...") if len(str(data)) > 200 else data),
                )
                raise

    @async_exception_handler
    async def _handle_response_headers(
        self,
        resp: httpx.Response,
        api_name: str,
    ) -> Optional[float]:
        retry_after: Optional[float] = None
        retry_after = self._get_retry_after(resp)

        await self._handle_response_headers_ratelimit(
            api_name=api_name,
            resp=resp,
        )

        return retry_after

    async def _handle_response_headers_ratelimit(
        self,
        api_name: str,
        resp: httpx.Response,
    ) -> None:
        headers: Dict[str, str] = {k.lower(): v for k, v in resp.headers.items()}

        try:
            rate_limit: Optional[RateLimit] = RateLimit.from_headers(headers=headers)
            if rate_limit and rate_limit.available:
                async with self._rate_limit_lock:
                    self._rate_limit_map[api_name] = (rate_limit, datetime.now())
                await self.log.adebug(
                    "更新接口本地速率限制信息",
                    emoji=LoggingEmoji.DEBUG.value,
                    api_name=api_name,
                    rate_limit_limit=rate_limit.limit,
                    rate_limit_remaining=rate_limit.remaining,
                    rate_limit_reset=rate_limit.reset,
                )
        except Exception as e:
            await self.log.awarning(
                "解析速率限制响应头失败，跳过本地限流",
                emoji=LoggingEmoji.WARNING.value,
                error=str(e),
                api_name=api_name,
                headers={k: v for k, v in headers.items()},
            )

    async def _handle_response_status(
        self,
        resp: httpx.Response,
        retry_on_status: Tuple[int, ...],
        response_model: Optional[type[T]],
        response_json: bool,
        do_request_func: Callable[..., Awaitable[httpx.Response]],
        remain_retries: int,
        backoff_time: Optional[float],
    ) -> Optional[httpx.Response]:
        if remain_retries > MAX_RETRY_RECURSION_DEPTH:
            await self.log.aerror(
                "递归重试次数超过最大限制，抛出异常",
                emoji=LoggingEmoji.ERROR.value,
                remain_retries=remain_retries,
                max_retries=MAX_RETRY_RECURSION_DEPTH,
            )
            raise RuntimeError("递归重试次数超过最大限制，抛出异常")

        if (resp.status_code // 100) == 2:
            # 成功状态码，直接返回响应对象
            return resp

        # 非成功状态码，尝试解析 JSON 错误信息
        content_type: str = resp.headers.get("content-type", "").lower()
        await self.log.aerror(
            event="HTTP 异常响应",
            content_type=content_type,
            status_code=resp.status_code,
            emoji=LoggingEmoji.ERROR.value,
        )
        if "application/json" in content_type:
            try:
                json_data: Any = resp.json()
                await self.log.aerror(
                    "HTTP 响应状态码异常，返回 JSON 错误信息",
                    emoji=LoggingEmoji.ERROR.value,
                    status_code=resp.status_code,
                    url=str(resp.url),
                    json_data=json_data,
                )
            except Exception as e:
                await self.log.aerror(
                    "HTTP 响应状态码异常，JSON 解析失败",
                    emoji=LoggingEmoji.ERROR.value,
                    status_code=resp.status_code,
                    url=str(resp.url),
                    error=str(e),
                    text=(
                        resp.text[:200] + "..." if len(resp.text) > 200 else resp.text
                    ),
                )
                raise
        else:
            # 其他情况也要尝试返回 content 里面可能有错误关键信息
            await self.log.aerror(
                "HTTP 响应状态码异常，返回非 JSON 错误信息",
                emoji=LoggingEmoji.ERROR.value,
                status_code=resp.status_code,
                url=str(resp.url),
                text=resp.text,
            )

        # 可重试状态码，递归异步重试
        if resp.status_code in retry_on_status:
            while remain_retries > 0:
                await self.log.awarning(
                    "检测到可重试状态码，准备异步重试",
                    emoji=LoggingEmoji.WARNING.value,
                    status_code=resp.status_code,
                    remain_retries=remain_retries,
                )
                await asyncio.sleep(backoff_time or 1)

                try:
                    next_backoff_time: float = (
                        backoff_time * self._backoff_factor if backoff_time else 1
                    )
                    retried_resp: httpx.Response = await do_request_func()

                    ensured_resp: Optional[httpx.Response] = (
                        await self._handle_response_status(
                            retried_resp,
                            retry_on_status=retry_on_status,
                            response_model=response_model,
                            response_json=response_json,
                            do_request_func=do_request_func,
                            remain_retries=remain_retries - 1,
                            backoff_time=next_backoff_time,
                        )
                    )

                    return ensured_resp
                except RetrayableError as e:
                    await self.log.aerror(
                        "可重试错误",
                        emoji=LoggingEmoji.ERROR.value,
                        error=str(e),
                        incomeing_resp_body=resp.text,
                        incomeing_resp_status_code=resp.status_code,
                        incomeing_resp_url=str(resp.url),
                        incomeing_resp_headers={k: v for k, v in resp.headers.items()},
                    )
                    # 继续尝试重试请求
                    backoff_time = next_backoff_time
                    remain_retries -= 1
                    continue
                except Exception as e:
                    await self.log.aerror(
                        "异步重试请求失败",
                        emoji=LoggingEmoji.ERROR.value,
                        error=str(e),
                        incomeing_resp_body=resp.text,
                        incomeing_resp_status_code=resp.status_code,
                        incomeing_resp_url=str(resp.url),
                        incomeing_resp_headers={k: v for k, v in resp.headers.items()},
                    )
                    raise
            else:
                await self.log.aerror(
                    "异步重试次数已用尽",
                    emoji=LoggingEmoji.ERROR.value,
                    status_code=resp.status_code if resp else None,
                    remain_retries=remain_retries,
                )
                raise RuntimeError("异步重试次数已用尽")

        # 其他状态码，抛出异常
        await self.log.aerror(
            "请求响应状态码异常，未返回响应对象",
            emoji=LoggingEmoji.ERROR.value,
            resp=resp,
            status_code=resp.status_code,
            retry_on_status=retry_on_status,
        )
        resp.raise_for_status()
        return None
