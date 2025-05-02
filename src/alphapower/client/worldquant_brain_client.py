import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

from httpx import BasicAuth

from alphapower.constants import (
    BASE_URL,
    ENDPOINT_AUTHENTICATION,
    ENDPOINT_TAGS,
    UserPermission,
    UserRole,
)
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import LogBase
from alphapower.view.alpha import CreateTagsPayload, ListTagAlphaView
from alphapower.view.user import AuthenticationView

from .httpx_client import HttpXClient
from .worldquant_brain_client_abc import AbstractWorldQuantBrainClient


class WorldQuantBrainClient(AbstractWorldQuantBrainClient, LogBase):
    """
    Client for interacting with the WorldQuant Brain API.
    """

    def __init__(self, username: str, password: str, **kwargs: Any) -> None:
        """
        Initialize the client with the given arguments.
        """
        super().__init__(**kwargs)
        self._username: str = username
        self._password: str = password
        self._http_client: HttpXClient = HttpXClient(
            base_url=BASE_URL,
            module_name=f"{self.__class__.__module__}.{self.__class__.__name__}",
        )
        self._authentication_info: Optional[Tuple[datetime, AuthenticationView]] = None
        self._http_client_lock: asyncio.Lock = asyncio.Lock()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._refresh_task: Optional[asyncio.Task] = None

    async def http_client(self) -> HttpXClient:
        """
        获取 HTTP 客户端实例，确保协程安全（coroutine-safe）。
        """

        if self._http_client is None:
            await self.log.aerror(
                "HTTP 客户端未初始化",
                emoji="❌",
            )
            raise ValueError("HTTP 客户端未初始化")

        await self.log.adebug(
            "获取 HTTP 客户端实例",
            emoji="🔗",
            client_type=type(self._http_client).__name__,
        )
        if not isinstance(self._http_client, HttpXClient):
            await self.log.aerror(
                "HTTP 客户端类型错误",
                emoji="❌",
                expected=HttpXClient.__name__,
                got=type(self._http_client).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {HttpXClient.__name__}，实际为 {type(self._http_client).__name__}"
            )

        # 协程安全地检查并刷新会话
        if await self._session_expired():
            async with self._http_client_lock:
                if self._stop_event.is_set():
                    if self._refresh_task:
                        self._refresh_task.cancel()
                        await self.log.ainfo(
                            "会话刷新任务已取消",
                            emoji="🛑",
                            username=self._username,
                        )
                        self._refresh_task = None
                    raise asyncio.CancelledError("客户端已停止，无法刷新会话")

                # 再次检查，避免并发下重复刷新
                if await self._session_expired():
                    await self._login(
                        username=self._username,
                        password=self._password,
                    )
                    await self.log.ainfo(
                        "会话已刷新",
                        emoji="🔄",
                        username=self._username,
                    )

                if not self._refresh_task:
                    self._refresh_task = asyncio.create_task(
                        self._refresh_session_loop()
                    )
                    await self.log.ainfo(
                        "会话刷新任务已启动",
                        emoji="🔄",
                        username=self._username,
                    )

        return self._http_client

    async def _refresh_session_loop(self) -> None:
        """
        循环刷新会话，直到停止事件被设置。
        """
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(60)
                # 提前 5 分钟检查会话是否过期
                if await self._session_expired(after=timedelta(minutes=5)):
                    await self._login(
                        username=self._username,
                        password=self._password,
                    )
                    await self.log.ainfo(
                        "会话已刷新",
                        emoji="🔄",
                        username=self._username,
                    )
        except asyncio.CancelledError:
            await self.log.ainfo(
                "后台任务已取消",
                emoji="🛑",
            )
        except Exception as e:
            await self.log.aerror(
                "后台任务刷新会话异常",
                emoji="💥",
                error=str(e),
                stack=traceback.format_exc(),
            )

    async def _session_expired(self, after: timedelta = timedelta(0)) -> bool:
        """
        检查会话是否过期。
        """
        if self._authentication_info is None:
            return True
        timestamp, auth_info = self._authentication_info

        if not isinstance(auth_info, AuthenticationView):
            await self.log.aerror(
                "认证信息类型错误",
                emoji="❌",
                expected=AuthenticationView.__name__,
                got=type(auth_info).__name__,
            )
            return True

        if not isinstance(timestamp, datetime):
            await self.log.aerror(
                "认证时间戳类型错误",
                emoji="❌",
                expected=datetime.__name__,
                got=type(timestamp).__name__,
            )
            return True

        if (
            timestamp + timedelta(seconds=auth_info.token.expiry)
            < datetime.now() + after
        ):
            await self.log.ainfo(
                "会话已过期",
                emoji="⏳",
                timestamp=timestamp,
                expiry=auth_info.token.expiry,
                after=after,
            )
            return True
        await self.log.ainfo(
            "会话未过期",
            emoji="🕒",
            timestamp=timestamp,
            expiry=auth_info.token.expiry,
            after=after,
        )
        return False

    async def get_authentication(
        self,
        username: str,
        password: str,
        **kwargs: Any,
    ) -> AuthenticationView:
        """
        获取 WorldQuant Brain 平台认证信息（GET 方式）。
        """
        auth: BasicAuth = BasicAuth(username=username, password=password)
        await self.log.adebug(
            "准备发起认证请求",
            emoji="🔑",
            username=username,
            kwargs=kwargs,
        )
        try:
            result: Any = await self._http_client.request(
                method="GET",
                url=ENDPOINT_AUTHENTICATION,
                auth=auth,
                api_name=WorldQuantBrainClient.get_authentication.__name__,
                response_model=AuthenticationView,
                **kwargs,
            )
            await self.log.adebug(
                "认证请求返回结果",
                emoji="📩",
                result_type=type(result).__name__,
            )
            if not isinstance(result, AuthenticationView):
                await self.log.aerror(
                    "认证响应类型错误",
                    emoji="❌",
                    expected=AuthenticationView.__name__,
                    got=type(result).__name__,
                )
                raise TypeError(
                    f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(result).__name__}"
                )
            await self.log.ainfo(
                "认证成功",
                emoji="✅",
                username=username,
            )
            return result
        except TypeError as e:
            # 类型错误单独处理，便于定位模型反序列化问题
            await self.log.aerror(
                "认证响应类型异常",
                emoji="❌",
                username=username,
                error=str(e),
                stack=traceback.format_exc(),
            )
            raise
        except Exception as e:
            # 其他异常统一处理，堆栈信息已在 httpx_client 内部详细记录
            await self.log.aerror(
                "认证请求异常",
                emoji="💥",
                username=username,
                error=str(e),
                stack=traceback.format_exc(),
            )
            raise

    async def _login(
        self,
        username: str,
        password: str,
        **kwargs: Any,
    ) -> AuthenticationView:
        """
        登录 WorldQuant Brain 平台，返回认证信息。
        """
        auth: BasicAuth = BasicAuth(username=username, password=password)
        await self.log.adebug(
            "准备发起登录请求",
            emoji="🔑",
            username=username,
            kwargs=kwargs,
        )
        try:
            result: Any = await self._http_client.request(
                method="POST",
                url=ENDPOINT_AUTHENTICATION,
                basic_auth=auth,
                api_name=WorldQuantBrainClient.login.__name__,
                response_model=AuthenticationView,
                **kwargs,
            )
            await self.log.adebug(
                "登录请求返回结果",
                emoji="📩",
                result_type=type(result).__name__,
            )
            if not isinstance(result, AuthenticationView):
                await self.log.aerror(
                    "登录响应类型错误",
                    emoji="❌",
                    expected=AuthenticationView.__name__,
                    got=type(result).__name__,
                )
                raise TypeError(
                    f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(result).__name__}"
                )
            await self.log.ainfo(
                "登录成功",
                emoji="✅",
                username=username,
            )
            return result
        except Exception as e:
            await self.log.aerror(
                "登录请求异常",
                emoji="💥",
                username=username,
                error=str(e),
                stack=traceback.format_exc(),
            )
            raise

    @async_exception_handler
    async def login(
        self,
        username: str,
        password: str,
        **kwargs: Any,
    ) -> AuthenticationView:
        """
        登录 WorldQuant Brain 平台，返回认证信息。协程安全（coroutine-safe）地处理会话状态。
        """
        await self.log.adebug(
            "准备协程安全地登录",
            emoji="🔑",
            username=username,
            kwargs=kwargs,
        )

        # 协程安全地处理会话状态，防止并发下状态不一致
        async with self._http_client_lock:
            authentication_view: AuthenticationView = await self._login(
                username=username,
                password=password,
                **kwargs,
            )
            self._authentication_info = (
                datetime.now(),
                authentication_view,
            )

            await self.log.ainfo(
                "登录时间记录，认证信息已更新",
                emoji="🕒",
                timestamp=self._authentication_info[0],
                username=username,
            )
            return authentication_view

    @async_exception_handler
    async def logout(self) -> None:
        """
        注销登录，清除认证信息。协程安全（coroutine-safe）地更新会话状态。
        """
        await self.log.ainfo(
            "注销登录",
            emoji="🔒",
            username=self._username,
        )

        # 协程安全地处理注销和会话状态
        async with self._http_client_lock:
            if self._refresh_task:
                self._refresh_task.cancel()
                try:
                    await self._refresh_task
                except asyncio.CancelledError:
                    await self.log.ainfo(
                        "会话刷新任务已取消",
                        emoji="🛑",
                        username=self._username,
                    )
                self._refresh_task = None

            http_client: HttpXClient = await self.http_client()
            await http_client.request(
                method="DELETE",
                url=ENDPOINT_AUTHENTICATION,
                api_name=WorldQuantBrainClient.logout.__name__,
                response_model=None,
            )

            self._authentication_info = None
            self._stop_event.set()
            await self.log.ainfo(
                "认证信息已清除，注销流程完成",
                emoji="✅",
                username=self._username,
            )

    @async_exception_handler
    async def get_user_id(self) -> str:
        """
        获取用户 ID。
        """
        await self.log.ainfo(
            "获取用户 ID",
            emoji="🔍",
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_id.__name__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户 ID 响应类型错误",
                emoji="❌",
                expected=AuthenticationView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(response).__name__}"
            )

        user_id: str = response.user.id
        await self.log.ainfo(
            "获取用户 ID 成功",
            emoji="✅",
            user_id=user_id,
        )
        return user_id

    @async_exception_handler
    async def get_user_permissions(self) -> List[UserPermission]:
        """
        获取用户权限。
        """
        await self.log.ainfo(
            "获取用户权限",
            emoji="🔍",
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_permissions.__name__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户权限响应类型错误",
                emoji="❌",
                expected=AuthenticationView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(response).__name__}"
            )

        permissions: List[UserPermission] = response.permissions
        await self.log.ainfo(
            "获取用户权限成功",
            emoji="✅",
            permissions=permissions,
        )
        return permissions

    @async_exception_handler
    async def get_user_role(self) -> UserRole:
        """
        获取用户角色。
        """
        await self.log.ainfo(
            "获取用户角色",
            emoji="🔍",
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_role.__name__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户角色响应类型错误",
                emoji="❌",
                expected=AuthenticationView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(response).__name__}"
            )

        role: UserRole = (
            UserRole.CONSULTANT
            if UserRole.CONSULTANT.value in response.permissions
            else UserRole.USER
        )

        await self.log.ainfo(
            "获取用户角色成功",
            emoji="✅",
            role=role,
        )
        return role

    @async_exception_handler
    async def create_alpha_list(self, payload: CreateTagsPayload) -> ListTagAlphaView:
        """
        创建 Alpha 列表。
        """
        await self.log.ainfo(
            "创建 Alpha 列表",
            emoji="📝",
            payload=payload,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = await http_client.request(
            method="POST",
            url=ENDPOINT_TAGS,
            api_name=WorldQuantBrainClient.create_alpha_list.__name__,
            json=payload.model_dump(mode="json"),
            response_model=ListTagAlphaView,
        )

        if not isinstance(response, ListTagAlphaView):
            await self.log.aerror(
                "创建 Alpha 列表响应类型错误",
                emoji="❌",
                expected=ListTagAlphaView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {ListTagAlphaView.__name__}，实际为 {type(response).__name__}"
            )

        await self.log.ainfo(
            "创建 Alpha 列表成功",
            emoji="✅",
            response=response,
        )
        return response
