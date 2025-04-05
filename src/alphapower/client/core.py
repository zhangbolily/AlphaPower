"""
WorldQuant AlphaPower Client
"""

import asyncio
from contextlib import suppress
from typing import Coroutine, Optional, Union

from aiohttp import BasicAuth, ClientSession

from alphapower.internal.wraps import exception_handler
from alphapower.settings import settings

from .models import (
    AuthenticationView,
    DataCategoriesListView,
    DataFieldListView,
    DatasetDataFieldsView,
    DatasetDetailView,
    DatasetListView,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
    MultiSimulationPayload,
    MultiSimulationResultView,
    Operators,
    RateLimit,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
    SimulationProgressView,
    SingleSimulationPayload,
    SingleSimulationResultView,
)
from .raw_api import (
    authentication,
    create_multi_simulation,
    create_single_simulation,
    delete_simulation,
    fetch_data_categories,
    fetch_data_field_detail,
    fetch_dataset_data_fields,
    fetch_dataset_detail,
    fetch_datasets,
    get_all_operators,
    get_self_alphas,
    get_simulation_progress,
)
from .utils import rate_limit_handler


class WorldQuantClient:
    """
    WorldQuant 客户端类，用于与 AlphaPower API 交互。
    """

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:
        """
        初始化 WorldQuantClient 实例。

        参数:
        username (str): 用户名。
        password (str): 密码。
        """
        self._is_closed: bool = True
        self._auth: BasicAuth = BasicAuth(username, password)
        self.session: Optional[ClientSession] = None
        self._auth_task: Optional[Coroutine] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._usage_count: int = 0
        self._usage_lock: asyncio.Lock = asyncio.Lock()
        self.authentication_info: Optional[AuthenticationView] = None

    async def _start_refresh_task(self, expiry: float) -> None:
        """
        后台异步循环刷新会话。
        """
        while not self._is_closed:
            try:
                await asyncio.sleep(max(expiry - 60, 0))
                if self._auth_task is None:
                    raise RuntimeError("认证任务未初始化")
                session_info: AuthenticationView = await self._auth_task
                self.authentication_info = session_info
                expiry = session_info.token.expiry
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"刷新会话时发生错误: {e}")

    async def initialize(self) -> None:
        """
        初始化客户端会话。
        """
        self.session = ClientSession(auth=self._auth)
        self._auth_task = authentication(self.session)
        self.authentication_info = await self._auth_task
        self._is_closed = False
        self._refresh_task = asyncio.create_task(
            self._start_refresh_task(self.authentication_info.token.expiry)
        )

    async def close(self) -> None:
        """
        关闭客户端会话。
        """
        if self.session and not self.session.closed:
            await self.session.close()
        self._is_closed = True
        self._auth_task = None
        if self._refresh_task:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task
        self._refresh_task = None

    async def _is_initialized(self) -> bool:
        """
        检查客户端是否已初始化。

        返回:
        bool: 如果已初始化，则返回 True，否则返回 False。
        """
        if self.session and self.session.closed:
            if self._is_closed:
                return False
            else:
                await self.close()
                return False

        return not self._is_closed

    async def __aenter__(self) -> "WorldQuantClient":
        """
        异步上下文管理器的进入方法。

        返回:
        WorldQuantClient: 客户端实例。
        """
        async with self._usage_lock:
            if self._usage_count == 0:
                if self._is_closed:
                    await self.initialize()
                if self.session is None:
                    raise RuntimeError("会话未初始化")
            self._usage_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[BaseException],
    ) -> None:
        """
        异步上下文管理器的退出方法。

        参数:
        exc_type (Optional[type]): 异常类型。
        exc_val (Optional[BaseException]): 异常值。
        exc_tb (Optional[BaseException]): 异常回溯。
        """
        async with self._usage_lock:
            self._usage_count -= 1
            if self._usage_count == 0:
                await self.close()

    def __del__(self) -> None:
        """
        确保在对象销毁时清理资源。
        """
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    # -------------------------------
    # Simulation-related methods
    # -------------------------------
    @exception_handler
    async def create_single_simulation(
        self, payload: SingleSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建单次模拟。

        参数:
        payload (SingleSimulationPayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        success, progress_id, retry_after = await create_single_simulation(
            self.session, payload.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def create_multi_simulation(
        self, payload: MultiSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建多次模拟。

        参数:
        payload (MultiSimulationPayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        success, progress_id, retry_after = await create_multi_simulation(
            self.session, payload.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def delete_simulation(self, progress_id: str) -> bool:
        """
        删除模拟。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        bool: 删除是否成功。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        await delete_simulation(self.session, progress_id)
        return True

    @exception_handler
    async def get_single_simulation_progress(
        self, progress_id: str
    ) -> tuple[bool, Union[SingleSimulationResultView, SimulationProgressView], float]:
        """
        获取单次模拟的进度。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        tuple: 包含完成状态、进度或结果对象以及重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, retry_after = await get_simulation_progress(
            self.session, progress_id, is_multi=False
        )
        if not isinstance(
            progress_or_result,
            (SingleSimulationResultView, SimulationProgressView),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_progress(
        self, progress_id: str
    ) -> tuple[bool, Union[MultiSimulationResultView, SimulationProgressView], float]:
        """
        获取多次模拟的进度。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        tuple: 包含完成状态、进度或结果对象以及重试时间的元组。
        """

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, retry_after = await get_simulation_progress(
            self.session, progress_id, is_multi=True
        )
        if not isinstance(
            progress_or_result,
            (MultiSimulationResultView, SimulationProgressView),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_child_result(
        self, child_progress_id: str
    ) -> tuple[bool, SingleSimulationResultView]:
        """
        获取多次模拟的子结果。

        参数:
        child_progress_id (str): 子模拟进度 ID。

        返回:
        tuple: 包含完成状态和单次模拟结果的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, _ = await get_simulation_progress(
            self.session, child_progress_id, is_multi=False
        )
        if not isinstance(progress_or_result, SingleSimulationResultView):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result

    # -------------------------------
    # Alpha-related methods
    # -------------------------------
    @exception_handler
    @rate_limit_handler
    async def get_self_alphas(
        self, query: SelfAlphaListQueryParams
    ) -> tuple[SelfAlphaListView, RateLimit]:
        """
        获取用户的 Alpha 列表。

        参数:
        query (SelfAlphaListQueryParams): 查询参数。

        返回:
        tuple: 包含 Alpha 列表视图和速率限制信息的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await get_self_alphas(self.session, query.to_params())
        return resp

    # -------------------------------
    # Data-related methods
    # -------------------------------
    @exception_handler
    async def get_data_categories(self) -> DataCategoriesListView:
        """
        获取数据类别。

        返回:
        DataCategoriesListView: 数据类别列表。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_data_categories(self.session)
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_datasets(
        self, query: DataSetsQueryParams
    ) -> Optional[DatasetListView]:
        """
        获取数据集列表。

        参数:
        query (DataSetsQueryParams): 查询参数。

        返回:
        Optional[DatasetListView]: 数据集列表视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_datasets(self.session, query.to_params())
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_dataset_detail(self, dataset_id: str) -> Optional[DatasetDetailView]:
        """
        获取数据集详情。

        参数:
        dataset_id (str): 数据集 ID。

        返回:
        Optional[DatasetDetail]: 数据集详情。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_dataset_detail(self.session, dataset_id)
        return resp

    @exception_handler
    async def get_data_field_detail(self, data_field_id: str) -> DatasetDataFieldsView:
        """
        获取数据字段详情。

        参数:
        data_field_id (str): 数据字段 ID。

        返回:
        DatasetDataFieldsView: 数据字段详情视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_data_field_detail(self.session, data_field_id)
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_data_fields_in_dataset(
        self, query: GetDataFieldsQueryParams
    ) -> Optional[DataFieldListView]:
        """
        获取数据集中包含的数据字段。

        参数:
        query (GetDataFieldsQueryParams): 查询参数。

        返回:
        Optional[DataFieldListView]: 数据字段列表视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_dataset_data_fields(self.session, query.to_params())
        return resp

    # -------------------------------
    # Other utility methods
    # -------------------------------
    @exception_handler
    async def get_all_operators(self) -> Operators:
        """
        获取所有操作符。

        返回:
        Operators: 操作符对象。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await get_all_operators(self.session)
        return resp


wq_client: WorldQuantClient = WorldQuantClient(
    username=settings.credential.username,
    password=settings.credential.password,
)
