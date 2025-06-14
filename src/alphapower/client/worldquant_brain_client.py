import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from httpx import BasicAuth, Response
from pydantic import Field

from alphapower.client.common_view import TableView
from alphapower.constants import (
    BASE_URL,
    ENDPOINT_ALPHAS,
    ENDPOINT_ALPHAS_CORRELATIONS,
    ENDPOINT_ALPHAS_OPTIONS,
    ENDPOINT_AUTHENTICATION,
    ENDPOINT_DATA_CATEGORIES,
    ENDPOINT_DATA_SETS,
    ENDPOINT_RECORD_SETS,
    ENDPOINT_SIMULATIONS_OPTIONS,
    ENDPOINT_TAGS,
    ENDPOINT_USER_SELF_ALPHAS,
    ENDPOINT_USER_SELF_TAGS,
    CorrelationType,
    LoggingEmoji,
    RecordSetType,
    UserPermission,
    UserRole,
)
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import BaseLogger
from alphapower.internal.multiprocessing import BaseProcessSafeFactory
from alphapower.view.alpha import (
    AlphaDetailView,
    AlphaPropertiesPayload,
    CreateTagsPayload,
    SelfTagListQuery,
    SelfTagListView,
    TagView,
    UserAlphasQuery,
    UserAlphasSummaryView,
    UserAlphasView,
)
from alphapower.view.data import (
    DataCategoryListView,
    DataCategoryView,
    DatasetListView,
    DatasetsQuery,
    DatasetView,
)
from alphapower.view.options import AlphasOptions, SimulationsOptions
from alphapower.view.user import AuthenticationView

from .httpx_client import HttpXClient
from .worldquant_brain_client_abc import AbstractWorldQuantBrainClient


class WorldQuantBrainClient(AbstractWorldQuantBrainClient, BaseLogger):
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
                emoji=LoggingEmoji.ERROR.value,
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
                emoji=LoggingEmoji.ERROR.value,
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
                            emoji=LoggingEmoji.CANCELED.value,
                            username=self._username,
                        )
                        self._refresh_task = None
                    raise asyncio.CancelledError("客户端已停止，无法刷新会话")

                # 再次检查，避免并发下重复刷新
                if await self._session_expired():
                    auth_view: AuthenticationView = await self._login(
                        username=self._username,
                        password=self._password,
                    )
                    self._authentication_info = (
                        datetime.now(),
                        auth_view,
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
                    async with self._http_client_lock:
                        # 再次检查，避免并发下重复刷新
                        if await self._session_expired(after=timedelta(minutes=5)):
                            auth_view: AuthenticationView = await self._login(
                                username=self._username,
                                password=self._password,
                            )
                            self._authentication_info = (
                                datetime.now(),
                                auth_view,
                            )
                            await self.log.ainfo(
                                "会话已刷新",
                                emoji="🔄",
                                username=self._username,
                            )
        except asyncio.CancelledError:
            await self.log.ainfo(
                "后台任务已取消",
                emoji=LoggingEmoji.CANCELED.value,
            )
        except Exception as e:
            await self.log.aerror(
                "后台任务刷新会话异常",
                emoji=LoggingEmoji.ERROR.value,
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
                emoji=LoggingEmoji.ERROR.value,
                expected=AuthenticationView.__name__,
                got=type(auth_info).__name__,
            )
            return True

        if not isinstance(timestamp, datetime):
            await self.log.aerror(
                "认证时间戳类型错误",
                emoji=LoggingEmoji.ERROR.value,
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
                emoji=LoggingEmoji.EXPIRED.value,
                timestamp=timestamp.isoformat(),
                expiry=auth_info.token.expiry,
                after=str(after),
            )
            return True
        await self.log.ainfo(
            "会话未过期",
            emoji=LoggingEmoji.NOT_EXPIRED.value,
            timestamp=timestamp.isoformat(),
            expiry=auth_info.token.expiry,
            after=str(after),
            expire_at=(
                timestamp + timedelta(seconds=auth_info.token.expiry)
            ).isoformat(),
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
            emoji=LoggingEmoji.AUTHORIZE.value,
            username=username,
            kwargs=kwargs,
        )
        try:
            result, _ = await self._http_client.request(
                method="GET",
                url=ENDPOINT_AUTHENTICATION,
                auth=auth,
                api_name=WorldQuantBrainClient.get_authentication.__qualname__,
                response_model=AuthenticationView,
                **kwargs,
            )
            await self.log.adebug(
                "认证请求返回结果",
                emoji=LoggingEmoji.RESPONSE.value,
                result_type=type(result).__name__,
            )
            if not isinstance(result, AuthenticationView):
                await self.log.aerror(
                    "认证响应类型错误",
                    emoji=LoggingEmoji.ERROR.value,
                    expected=AuthenticationView.__name__,
                    got=type(result).__name__,
                )
                raise TypeError(
                    f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(result).__name__}"
                )
            await self.log.ainfo(
                "认证成功",
                emoji=LoggingEmoji.SUCCESS.value,
                username=username,
            )
            return result
        except TypeError as e:
            # 类型错误单独处理，便于定位模型反序列化问题
            await self.log.aerror(
                "认证响应类型异常",
                emoji=LoggingEmoji.ERROR.value,
                username=username,
                error=str(e),
                stack=traceback.format_exc(),
            )
            raise
        except Exception as e:
            # 其他异常统一处理，堆栈信息已在 httpx_client 内部详细记录
            await self.log.aerror(
                "认证请求异常",
                emoji=LoggingEmoji.ERROR.value,
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
            emoji=LoggingEmoji.AUTHORIZE.value,
            username=username,
            kwargs=kwargs,
        )
        try:
            result, _ = await self._http_client.request(
                method="POST",
                url=ENDPOINT_AUTHENTICATION,
                basic_auth=auth,
                api_name=WorldQuantBrainClient.login.__qualname__,
                response_model=AuthenticationView,
                **kwargs,
            )
            await self.log.adebug(
                "登录请求返回结果",
                emoji=LoggingEmoji.RESPONSE.value,
                result_type=type(result).__name__,
            )
            if not isinstance(result, AuthenticationView):
                await self.log.aerror(
                    "登录响应类型错误",
                    emoji=LoggingEmoji.ERROR.value,
                    expected=AuthenticationView.__name__,
                    got=type(result).__name__,
                )
                raise TypeError(
                    f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(result).__name__}"
                )
            await self.log.ainfo(
                "登录成功",
                emoji=LoggingEmoji.SUCCESS.value,
                username=username,
            )
            return result
        except Exception as e:
            await self.log.aerror(
                "登录请求异常",
                emoji=LoggingEmoji.ERROR.value,
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
            emoji=LoggingEmoji.AUTHORIZE.value,
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
                emoji=LoggingEmoji.SUCCESS.value,
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
                        emoji=LoggingEmoji.CANCELED.value,
                        username=self._username,
                    )
                self._refresh_task = None

            http_client: HttpXClient = await self.http_client()
            await http_client.request(
                method="DELETE",
                url=ENDPOINT_AUTHENTICATION,
                api_name=WorldQuantBrainClient.logout.__qualname__,
                response_model=None,
            )

            self._authentication_info = None
            self._stop_event.set()
            await self.log.ainfo(
                "认证信息已清除，注销流程完成",
                emoji=LoggingEmoji.FINISHED.value,
                username=self._username,
            )

    @async_exception_handler
    async def get_user_id(self) -> str:
        """
        获取用户 ID。
        """
        await self.log.ainfo(
            "获取用户 ID",
            emoji=LoggingEmoji.INFO.value,
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_id.__qualname__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户 ID 响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=AuthenticationView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(response).__name__}"
            )

        user_id: str = response.user.id
        await self.log.ainfo(
            "获取用户 ID 成功",
            emoji=LoggingEmoji.SUCCESS.value,
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
            emoji=LoggingEmoji.INFO.value,
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_permissions.__qualname__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户权限响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=AuthenticationView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AuthenticationView.__name__}，实际为 {type(response).__name__}"
            )

        permissions: List[UserPermission] = response.permissions
        await self.log.ainfo(
            "获取用户权限成功",
            emoji=LoggingEmoji.SUCCESS.value,
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
            emoji=LoggingEmoji.INFO.value,
            username=self._username,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_AUTHENTICATION,
            api_name=WorldQuantBrainClient.get_user_role.__qualname__,
            response_model=AuthenticationView,
        )

        if not isinstance(response, AuthenticationView):
            await self.log.aerror(
                "获取用户角色响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
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
            emoji=LoggingEmoji.SUCCESS.value,
            role=role,
        )
        return role

    @async_exception_handler
    async def create_alpha_list(self, payload: CreateTagsPayload) -> TagView:
        """
        创建 Alpha 列表。
        """
        await self.log.ainfo(
            "创建 Alpha 列表",
            emoji=LoggingEmoji.CREATE.value,
            payload=payload.to_serializable_dict(),
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="POST",
            url=ENDPOINT_TAGS,
            api_name=WorldQuantBrainClient.create_alpha_list.__qualname__,
            json=payload.to_serializable_dict(),
            response_model=TagView,
        )

        if not isinstance(response, TagView):
            await self.log.aerror(
                "创建 Alpha 列表响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=TagView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {TagView.__name__}，实际为 {type(response).__name__}"
            )

        await self.log.ainfo(
            "创建 Alpha 列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
            response=response,
        )
        return response

    @async_exception_handler
    async def delete_alpha_list(self, tag_id: str) -> None:
        """
        删除 Alpha 列表。
        """
        # INFO 日志：方法进入，参数输出
        await self.log.ainfo(
            "进入删除 Alpha 列表方法",
            emoji=LoggingEmoji.INFO.value,
            tag_id=tag_id,
        )

        http_client: HttpXClient = await self.http_client()
        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 DELETE 请求删除 Alpha 列表",
            emoji=LoggingEmoji.DEBUG.value,
            url=f"{ENDPOINT_TAGS}/{tag_id}",
            api_name=WorldQuantBrainClient.delete_alpha_list.__qualname__,
        )

        await http_client.request(
            method="DELETE",
            url=f"{ENDPOINT_TAGS}/{tag_id}",
            api_name=WorldQuantBrainClient.delete_alpha_list.__qualname__,
            response_model=None,
        )

        # INFO 日志：方法成功退出
        await self.log.ainfo(
            "删除 Alpha 列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
            tag_id=tag_id,
        )

    @async_exception_handler
    async def fetch_user_tags(
        self,
        query: SelfTagListQuery,
    ) -> SelfTagListView:
        """
        获取用户标签列表。
        """
        # INFO 日志：方法进入，参数输出
        await self.log.ainfo(
            "进入获取用户标签列表方法",
            emoji=LoggingEmoji.INFO.value,
            query=query.to_params(),
        )

        http_client: HttpXClient = await self.http_client()
        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 GET 请求获取用户标签列表",
            emoji="📤",
            url=ENDPOINT_USER_SELF_TAGS,
            api_name=WorldQuantBrainClient.fetch_user_tags.__qualname__,
            params=query.to_params(),
        )

        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_USER_SELF_TAGS,
            api_name=WorldQuantBrainClient.fetch_user_tags.__qualname__,
            params=query.to_params(),
            response_model=SelfTagListView,
        )

        # DEBUG 日志：响应类型输出
        await self.log.adebug(
            "收到 GET 响应",
            emoji="📥",
            response_type=type(response).__name__,
        )

        if not isinstance(response, SelfTagListView):
            # ERROR 日志：类型错误
            await self.log.aerror(
                "获取用户标签列表响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=SelfTagListView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {SelfTagListView.__name__}，实际为 {type(response).__name__}"
            )

        # INFO 日志：方法成功退出，不打印返回参数
        await self.log.ainfo(
            "获取用户标签列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )
        # DEBUG 日志：返回参数详细输出
        await self.log.adebug(
            "返回的用户标签列表视图",
            emoji="📜",
            tag_list_ids=lambda: [tag.id for tag in response.results],
        )
        return response

    @async_exception_handler
    async def fetch_user_alphas_summary(self) -> UserAlphasSummaryView:
        """
        获取用户 Alpha 概要信息。
        """
        # INFO 日志：方法进入
        await self.log.ainfo(
            "进入获取用户 Alpha 概要信息方法",
            emoji=LoggingEmoji.INFO.value,
        )

        http_client: HttpXClient = await self.http_client()
        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 GET 请求获取用户 Alpha 概要信息",
            emoji="📤",
            url=ENDPOINT_USER_SELF_ALPHAS,
            api_name=WorldQuantBrainClient.fetch_user_alphas_summary.__qualname__,
        )

        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_USER_SELF_ALPHAS,
            api_name=WorldQuantBrainClient.fetch_user_alphas_summary.__qualname__,
            response_model=UserAlphasSummaryView,
        )

        # DEBUG 日志：响应类型输出
        await self.log.adebug(
            "收到 GET 响应",
            emoji="📥",
            response_type=type(response).__name__,
        )

        if not isinstance(response, UserAlphasSummaryView):
            # ERROR 日志：类型错误
            await self.log.aerror(
                "获取用户 Alpha 概要信息响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=UserAlphasSummaryView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {UserAlphasSummaryView.__name__}，实际为 {type(response).__name__}"
            )

        # INFO 日志：方法成功退出，不打印返回参数
        await self.log.ainfo(
            "获取用户 Alpha 概要信息成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        # DEBUG 日志：返回参数详细输出
        await self.log.adebug(
            "返回的用户 Alpha 概要视图",
            emoji="📜",
            response=response.model_dump(mode="json"),
        )

        return response

    @async_exception_handler
    async def fetch_user_alphas(self, query: UserAlphasQuery) -> UserAlphasView:
        """
        获取用户 Alpha 列表。
        """
        # INFO 日志：方法进入，参数输出
        await self.log.ainfo(
            "进入获取用户 Alpha 列表方法",
            emoji=LoggingEmoji.INFO.value,
            query=query.to_params(),
        )

        http_client: HttpXClient = await self.http_client()
        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 GET 请求获取用户 Alpha 列表",
            emoji="📤",
            url=ENDPOINT_USER_SELF_ALPHAS,
            api_name=WorldQuantBrainClient.fetch_user_alphas.__qualname__,
            params=query.to_params(),
        )

        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_USER_SELF_ALPHAS,
            api_name=WorldQuantBrainClient.fetch_user_alphas.__qualname__,
            params=query.to_params(),
            response_model=UserAlphasView,
        )

        # DEBUG 日志：响应类型输出
        await self.log.adebug(
            "收到 GET 响应",
            emoji="📥",
            response_type=type(response).__name__,
        )

        if not isinstance(response, UserAlphasView):
            # ERROR 日志：类型错误
            await self.log.aerror(
                "获取用户 Alpha 列表响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=UserAlphasView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {UserAlphasView.__name__}，实际为 {type(response).__name__}"
            )

        # INFO 日志：方法成功退出，不打印返回参数
        await self.log.ainfo(
            "获取用户 Alpha 列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )
        # DEBUG 日志：返回参数详细输出
        # 只打印 Alpha ID，避免输出无效信息
        # 仅在 debug 级别日志时才生成 alpha_id 列表，避免无谓的计算开销
        await self.log.adebug(
            "返回的用户 Alpha 列表视图，仅输出 alpha_id",
            emoji="📜",
            alpha_ids=lambda: [alpha.id for alpha in response.results],
        )
        return response

    @async_exception_handler
    async def update_alpha_properties(
        self,
        alpha_id: str,
        payload: AlphaPropertiesPayload,
    ) -> AlphaDetailView:
        """
        更新 Alpha 属性。
        """
        # INFO 日志：方法进入，参数输出
        await self.log.ainfo(
            "进入更新 Alpha 属性方法",
            emoji="📝",
            alpha_id=alpha_id,
            payload=payload.to_serializable_dict(),
        )

        http_client: HttpXClient = await self.http_client()
        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 PATCH 请求更新 Alpha 属性",
            emoji="📤",
            url=f"{ENDPOINT_ALPHAS}/{alpha_id}",
            api_name=WorldQuantBrainClient.update_alpha_properties.__qualname__,
            payload_dict=payload.to_serializable_dict(),
        )

        response, _ = await http_client.request(
            method="PATCH",
            url=f"{ENDPOINT_ALPHAS}/{alpha_id}",
            api_name=WorldQuantBrainClient.update_alpha_properties.__qualname__,
            json=payload.to_serializable_dict(),
            response_model=AlphaDetailView,
        )

        # DEBUG 日志：响应类型输出
        await self.log.adebug(
            "收到 PATCH 响应",
            emoji="📥",
            response_type=type(response).__name__,
        )

        if not isinstance(response, AlphaDetailView):
            # ERROR 日志：类型错误
            await self.log.aerror(
                "更新 Alpha 属性响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=AlphaDetailView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AlphaDetailView.__name__}，实际为 {type(response).__name__}"
            )

        # INFO 日志：方法成功退出，不打印返回参数
        await self.log.ainfo(
            "更新 Alpha 属性成功",
            emoji=LoggingEmoji.SUCCESS.value,
            alpha_id=alpha_id,
        )
        # DEBUG 日志：返回参数详细输出
        await self.log.adebug(
            "返回的 Alpha 详细视图",
            emoji="📜",
            response=response.model_dump(mode="json"),
            alpha_id=alpha_id,
        )
        return response

    @async_exception_handler
    async def fetch_alpha_correlation(
        self,
        alpha_id: str,
        correlation_type: CorrelationType,
        override_retry_after: Optional[float] = None,
    ) -> TableView:
        """
        获取指定 Alpha 的相关性（correlation）数据。

        参数:
            alpha_id: Alpha 的唯一标识符
            correlation_type: 相关性类型（CorrelationType，相关性类型）

        返回:
            TableView: 相关性数据表视图
        """
        # INFO 日志：方法进入，参数输出
        await self.log.ainfo(
            "进入获取 Alpha 相关性方法",
            emoji=LoggingEmoji.INFO.value,
            alpha_id=alpha_id,
            correlation_type=correlation_type,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = None
        retry_after: Optional[float] = -1

        # DEBUG 日志：请求参数详细输出
        await self.log.adebug(
            "准备发送 GET 请求获取 Alpha 相关性",
            emoji="📤",
            url=ENDPOINT_ALPHAS_CORRELATIONS(alpha_id, correlation_type),
            api_name=WorldQuantBrainClient.fetch_alpha_correlation.__qualname__,
        )

        while retry_after and retry_after != 0:
            response, retry_after = await http_client.request(
                method="GET",
                url=ENDPOINT_ALPHAS_CORRELATIONS(alpha_id, correlation_type),
                api_name=WorldQuantBrainClient.fetch_alpha_correlation.__qualname__,
                response_json=False,
            )

            if retry_after and retry_after != 0:
                retry_after = (
                    retry_after
                    if override_retry_after is None
                    else max(override_retry_after, retry_after)
                )

                await self.log.ainfo(
                    "请求需轮询等待完成",
                    emoji=LoggingEmoji.EXPIRED.value,
                    retry_after=retry_after,
                    override_retry_after=override_retry_after,
                    alpha_id=alpha_id,
                    correlation_type=correlation_type,
                )

                await asyncio.sleep(retry_after)
            elif isinstance(response, Response):
                try:
                    response = TableView.model_validate_json(response.text)
                    await self.log.adebug(
                        "响应已成功解析为 TableView",
                        emoji="📥",
                        response_type=type(response).__name__,
                        alpha_id=alpha_id,
                        correlation_type=correlation_type,
                    )
                except Exception as e:
                    await self.log.aerror(
                        "响应解析失败",
                        emoji=LoggingEmoji.ERROR.value,
                        error=str(e),
                        stack=traceback.format_exc(),
                        alpha_id=alpha_id,
                        correlation_type=correlation_type,
                    )
                    raise
            else:
                await self.log.aerror(
                    "响应类型错误",
                    emoji=LoggingEmoji.ERROR.value,
                    expected=Response.__name__,
                    got=type(response).__name__,
                    alpha_id=alpha_id,
                    correlation_type=correlation_type,
                )
                raise TypeError(
                    f"期望返回类型为 {Response.__name__}，实际为 {type(response).__name__}"
                )

        if not isinstance(response, TableView):
            await self.log.aerror(
                "获取 Alpha 相关性响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=TableView.__name__,
                got=type(response).__name__,
                alpha_id=alpha_id,
                correlation_type=correlation_type,
            )
            raise TypeError(
                f"期望返回类型为 {TableView.__name__}，实际为 {type(response).__name__}"
            )

        # INFO 日志：方法成功退出
        await self.log.ainfo(
            "获取 Alpha 相关性成功",
            emoji=LoggingEmoji.SUCCESS.value,
            alpha_id=alpha_id,
            correlation_type=correlation_type,
        )
        # DEBUG 日志：返回参数详细输出，仅输出表格行数和列数
        await self.log.adebug(
            "返回的 TableView 相关性数据",
            emoji="📊",
            alpha_id=alpha_id,
            correlation_type=correlation_type,
        )
        return response

    @async_exception_handler
    async def fetch_alpha_record_sets(
        self,
        alpha_id: str,
        record_set_type: RecordSetType,
        override_retry_after: Optional[float] = None,
    ) -> TableView:
        await self.log.ainfo(
            event=f"进入 {self.fetch_alpha_record_sets.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        http_client: HttpXClient = await self.http_client()
        response: Any = None
        retry_after: Optional[float] = -1

        await self.log.adebug(
            event=f"准备发送 GET 请求获取 Alpha 记录集",
            emoji=LoggingEmoji.DEBUG.value,
            url=ENDPOINT_RECORD_SETS(alpha_id, record_set_type),
            api_name=self.fetch_alpha_record_sets.__qualname__,
        )

        while retry_after and retry_after != 0:
            response, retry_after = await http_client.request(
                method="GET",
                url=ENDPOINT_RECORD_SETS(alpha_id, record_set_type),
                api_name=self.fetch_alpha_record_sets.__qualname__,
                response_json=False,
            )

            if retry_after and retry_after != 0:
                retry_after = (
                    retry_after
                    if override_retry_after is None
                    else max(override_retry_after, retry_after)
                )

                await self.log.ainfo(
                    event=f"请求需轮询等待完成",
                    emoji=LoggingEmoji.EXPIRED.value,
                    retry_after=retry_after,
                    override_retry_after=override_retry_after,
                    alpha_id=alpha_id,
                    record_set_type=record_set_type,
                )

                await asyncio.sleep(retry_after)
            elif isinstance(response, Response):
                try:
                    response = TableView.model_validate_json(response.text)
                    await self.log.adebug(
                        event=f"响应已成功解析为 TableView",
                        emoji="📥",
                        response_type=type(response).__name__,
                        alpha_id=alpha_id,
                        record_set_type=record_set_type,
                    )
                except Exception as e:
                    await self.log.aerror(
                        event=f"响应解析失败",
                        emoji=LoggingEmoji.ERROR.value,
                        error=str(e),
                        stack=traceback.format_exc(),
                        alpha_id=alpha_id,
                        record_set_type=record_set_type,
                    )
                    raise
            else:
                await self.log.aerror(
                    event=f"响应类型错误",
                    emoji=LoggingEmoji.ERROR.value,
                    expected=Response.__name__,
                    got=type(response).__name__,
                    alpha_id=alpha_id,
                    record_set_type=record_set_type,
                )
                raise TypeError(
                    f"期望返回类型为 {Response.__name__}，实际为 {type(response).__name__}"
                )

        if not isinstance(response, TableView):
            await self.log.aerror(
                event=f"获取 Alpha 记录集响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=TableView.__name__,
                got=type(response).__name__,
                alpha_id=alpha_id,
                record_set_type=record_set_type,
            )
            raise TypeError(
                f"期望返回类型为 {TableView.__name__}，实际为 {type(response).__name__}"
            )

        await self.log.ainfo(
            event=f"获取 Alpha 记录集成功",
            emoji=LoggingEmoji.SUCCESS.value,
            alpha_id=alpha_id,
            record_set_type=record_set_type,
        )

        await self.log.ainfo(
            event=f"退出 {self.fetch_alpha_record_sets.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

        return response

    @async_exception_handler
    async def fetch_data_categories(self) -> List[DataCategoryView]:
        """
        获取数据类别列表。
        """
        await self.log.ainfo(
            event=f"进入 {self.fetch_data_categories.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.ainfo(
            "获取数据类别列表",
            emoji=LoggingEmoji.INFO.value,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_DATA_CATEGORIES,
            api_name=WorldQuantBrainClient.fetch_data_categories.__qualname__,
            response_model=DataCategoryListView,
        )

        if not isinstance(response, DataCategoryListView):
            await self.log.aerror(
                "获取数据类别列表响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=DataCategoryListView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {DataCategoryListView.__name__}，实际为 {type(response).__name__}"
            )

        if not response.root:
            await self.log.awarning(
                "获取数据类别列表为空",
                emoji=LoggingEmoji.WARNING.value,
            )
            return []

        await self.log.ainfo(
            "获取数据类别列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.fetch_data_categories.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

        return response.root

    @async_exception_handler
    async def fetch_datasets(self, query: DatasetsQuery) -> List[DatasetView]:
        await self.log.ainfo(
            event=f"进入 {self.fetch_datasets.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.ainfo(
            "获取数据集列表",
            emoji=LoggingEmoji.INFO.value,
            query=query.to_params(),
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="GET",
            url=ENDPOINT_DATA_SETS,
            api_name=WorldQuantBrainClient.fetch_datasets.__qualname__,
            params=query.to_params(),
            response_model=DatasetListView,
        )

        if not isinstance(response, DatasetListView):
            await self.log.aerror(
                "获取数据集列表响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=DatasetListView.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {DatasetListView.__name__}，实际为 {type(response).__name__}"
            )

        if not response.results:
            await self.log.awarning(
                "获取数据集列表为空",
                emoji=LoggingEmoji.WARNING.value,
            )
            return []

        await self.log.ainfo(
            "获取数据集列表成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.fetch_datasets.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

        return response.results

    @async_exception_handler
    async def fetch_alphas_options(self, user_id: str) -> AlphasOptions:
        """
        获取 Alphas 的动态配置选项。
        """
        await self.log.ainfo(
            event=f"进入 {self.fetch_alphas_options.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.ainfo(
            event="获取 Alphas 动态配置选项",
            emoji=LoggingEmoji.INFO.value,
            user_id=user_id,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="OPTIONS",
            url=ENDPOINT_ALPHAS_OPTIONS(user_id),
            api_name=WorldQuantBrainClient.fetch_alphas_options.__qualname__,
            response_model=AlphasOptions,
        )

        if not isinstance(response, AlphasOptions):
            await self.log.aerror(
                "获取 Alphas 动态配置选项响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=AlphasOptions.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {AlphasOptions.__name__}，实际为 {type(response).__name__}"
            )

        await self.log.ainfo(
            "获取 Alphas 动态配置选项成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.fetch_alphas_options.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return response

    @async_exception_handler
    async def fetch_simulations_options(self) -> SimulationsOptions:
        await self.log.ainfo(
            event=f"进入 {self.fetch_simulations_options.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.ainfo(
            event="获取 Simulations 动态配置选项",
            emoji=LoggingEmoji.INFO.value,
        )

        http_client: HttpXClient = await self.http_client()
        response, _ = await http_client.request(
            method="OPTIONS",
            url=ENDPOINT_SIMULATIONS_OPTIONS,
            api_name=WorldQuantBrainClient.fetch_simulations_options.__qualname__,
            response_model=SimulationsOptions,
        )

        if not isinstance(response, SimulationsOptions):
            await self.log.aerror(
                "获取 Simulations 动态配置选项响应类型错误",
                emoji=LoggingEmoji.ERROR.value,
                expected=SimulationsOptions.__name__,
                got=type(response).__name__,
            )
            raise TypeError(
                f"期望返回类型为 {SimulationsOptions.__name__}，实际为 {type(response).__name__}"
            )
        
        await self.log.ainfo(
            "获取 Simulations 动态配置选项成功",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.fetch_simulations_options.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

        return response


class WorldQuantBrainClientFactory(
    BaseProcessSafeFactory[AbstractWorldQuantBrainClient]
):
    """
    工厂类，用于创建 WorldQuantBrainClient 实例。
    """

    username: str = Field(default="")
    password: str = Field(default="")

    def __init__(self, username: str, password: str, **kwargs: Any) -> None:
        """
        初始化工厂类。
        """
        super().__init__(**kwargs)
        self.username: str = username
        self.password: str = password

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        """
        返回依赖的工厂列表。
        """
        return {}

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractWorldQuantBrainClient:
        client: AbstractWorldQuantBrainClient = WorldQuantBrainClient(
            username=self.username,
            password=self.password,
            **kwargs,
        )
        await self.log.ainfo(
            "WorldQuantBrainClient 实例已成功创建",
            emoji=LoggingEmoji.SUCCESS.value,
            username=self.username,
        )
        return client
