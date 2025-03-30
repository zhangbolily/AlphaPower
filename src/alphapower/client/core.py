"""
WorldQuant AlphaPower Client
"""

import asyncio
from contextlib import suppress
from typing import Coroutine, Optional, Union

from aiohttp import BasicAuth, ClientSession, TCPConnector

from alphapower.internal.wraps import exception_handler

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


def create_client(
    credentials: dict[str, str], pool_connections: int = 10, pool_maxsize: int = 10
) -> "WorldQuantClient":
    """
    创建 WorldQuant 客户端。

    参数:
    credentials (dict): 包含用户名和密码的凭据。
    pool_connections (int): 连接池的最大连接数。
    pool_maxsize (int): 每个主机的最大连接数。

    返回:
    WorldQuantClient: 客户端实例。
    """
    client = WorldQuantClient(
        username=credentials["username"],
        password=credentials["password"],
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    return client


class WorldQuantClient:
    """
    WorldQuant 客户端类，用于与 AlphaPower API 交互。
    """

    def __init__(
        self,
        username: str,
        password: str,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
    ) -> None:
        """
        初始化 WorldQuantClient 实例。

        参数:
        username (str): 用户名。
        password (str): 密码。
        """
        self._refresh_task: Optional[asyncio.Task] = None
        self.shutdown: bool = False

        """初始化 ClientSession"""
        connector: TCPConnector = TCPConnector(
            limit=pool_connections, limit_per_host=pool_maxsize
        )
        auth: BasicAuth = BasicAuth(username, password)
        self.session: ClientSession = ClientSession(connector=connector, auth=auth)
        self._auth_task: Coroutine = authentication(self.session)

    async def _refresh_session(self, expiry: float) -> None:
        """
        后台任务定期刷新会话。

        参数:
        expiry (int): 会话过期时间（秒）。
        """
        while not self.shutdown:
            await asyncio.sleep(expiry - 60)  # 提前 60 秒刷新
            self._auth_task = authentication(self.session)
            session_info: AuthenticationView = await self._auth_task
            expiry = session_info.token.expiry  # 更新下次刷新时间

    async def __aenter__(self) -> "WorldQuantClient":
        """
        异步上下文管理器的进入方法。

        返回:
        WorldQuantClient: 客户端实例。
        """
        if (
            not self.session
            or self.session.closed
            or not isinstance(self.session, ClientSession)
        ):
            raise RuntimeError("客户端会话未初始化或已关闭")

        if self._auth_task is None:
            raise RuntimeError("客户端会话认证方法未提供")

        session_info: AuthenticationView = await self._auth_task
        expiry: float = session_info.token.expiry
        self._refresh_task = asyncio.create_task(self._refresh_session(expiry))
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
        if self._refresh_task:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task
        if self.session:
            await self.session.close()

    # -------------------------------
    # Simulation-related methods
    # -------------------------------
    @exception_handler
    async def create_single_simulation(
        self, simulation_data: SingleSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建单次模拟。

        参数:
        simulation_data (SingleSimulationPayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        success, progress_id, retry_after = await create_single_simulation(
            self.session, simulation_data.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def create_multi_simulation(
        self, simulation_data: MultiSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建多次模拟。

        参数:
        simulation_data (MultiSimulationRayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        success, progress_id, retry_after = await create_multi_simulation(
            self.session, simulation_data.to_params()
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
    async def get_multi_simulation_result(
        self, child_progress_id: str
    ) -> tuple[bool, SingleSimulationResultView]:
        """
        获取多次模拟的子结果。

        参数:
        child_progress_id (str): 子模拟进度 ID。

        返回:
        tuple: 包含完成状态和单次模拟结果的元组。
        """
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
        resp = await get_all_operators(self.session)
        return resp
