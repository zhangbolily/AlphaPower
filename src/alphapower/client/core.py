#!/usr/bin/env python
import asyncio
from contextlib import suppress
from typing import Coroutine, List, Optional, Union

from aiohttp import BasicAuth, ClientSession, TCPConnector

from alphapower.client.models import (
    Authentication,
    DataCategoriesParent,
    DataFieldDetail,
    DatasetDataFields,
    DatasetDetail,
    DataSets,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
    MultiSimulationRequest,
    MultiSimulationResult,
    Operators,
    RateLimit,
    SelfAlphaList,
    SelfAlphaListQueryParams,
    SimulationProgress,
    SingleSimulationRequest,
    SingleSimulationResult,
)
from alphapower.client.raw_api import alphas, data, other, simulation, user
from alphapower.internal.wraps import exception_handler

from .utils import rate_limit_handler


def create_client(
    credentials: dict[str, str], pool_connections: int = 10, pool_maxsize: int = 10
) -> "WorldQuantClient":
    """
    创建 WorldQuant 客户端。

    参数:
    credentials (dict): 包含用户名和密码的凭据。

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
    def __init__(
        self,
        username: str,
        password: str,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
    ) -> None:
        self.pool_connections: int = pool_connections
        self.pool_maxsize: int = pool_maxsize
        self.username: str = username
        self.password: str = password
        self._refresh_task: Optional[asyncio.Task] = None
        self.shutdown: bool = False

        """初始化 ClientSession"""
        connector: TCPConnector = TCPConnector(
            limit=self.pool_connections, limit_per_host=self.pool_maxsize
        )
        auth: BasicAuth = BasicAuth(self.username, self.password)
        self.session: ClientSession = ClientSession(connector=connector, auth=auth)
        self._auth_task: Coroutine = user.authentication(self.session)

    async def _refresh_session(self, expiry: int) -> None:
        """后台任务定期刷新会话"""
        while not self.shutdown:
            await asyncio.sleep(expiry - 60)  # 提前 60 秒刷新
            self._auth_task = user.authentication(self.session)
            session_info: Authentication = await self._auth_task
            expiry = session_info.token.expiry  # 更新下次刷新时间

    async def __aenter__(self) -> "WorldQuantClient":
        if (
            not self.session
            or self.session.closed
            or not isinstance(self.session, ClientSession)
        ):
            raise RuntimeError("客户端会话未初始化或已关闭")

        if self._auth_task is None:
            raise RuntimeError("客户端会话认证方法未提供")

        session_info = await self._auth_task
        expiry: int = session_info.token.expiry
        self._refresh_task = asyncio.create_task(self._refresh_session(expiry))
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[BaseException],
    ) -> None:
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
        self, simulation_data: SingleSimulationRequest
    ) -> tuple[bool, str, float]:
        success, progress_id, retry_after = await simulation.create_single_simulation(
            self.session, simulation_data.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def create_multi_simulation(
        self, simulation_data: MultiSimulationRequest
    ) -> tuple[bool, str, float]:
        success, progress_id, retry_after = await simulation.create_multi_simulation(
            self.session, simulation_data.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def delete_simulation(self, progress_id: str) -> bool:
        success = await simulation.delete_simulation(self.session, progress_id)
        return success

    @exception_handler
    async def get_single_simulation_progress(
        self, progress_id: str
    ) -> tuple[bool, Union[SingleSimulationResult, SimulationProgress], float]:
        finished, progress_or_result, retry_after = (
            await simulation.get_simulation_progress(
                self.session, progress_id, is_multi=False
            )
        )
        if not isinstance(
            progress_or_result,
            (SingleSimulationResult, SimulationProgress),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_progress(
        self, progress_id: str
    ) -> tuple[bool, Union[MultiSimulationResult, SimulationProgress], float]:
        finished, progress_or_result, retry_after = (
            await simulation.get_simulation_progress(
                self.session, progress_id, is_multi=True
            )
        )
        if not isinstance(
            progress_or_result,
            (MultiSimulationResult, SimulationProgress),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_result(
        self, child_progress_id: str
    ) -> tuple[bool, SingleSimulationResult]:
        finished, progress_or_result, _ = await simulation.get_simulation_progress(
            self.session, child_progress_id, is_multi=False
        )
        if not isinstance(progress_or_result, SingleSimulationResult):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result

    # -------------------------------
    # Alpha-related methods
    # -------------------------------
    @exception_handler
    @rate_limit_handler
    async def get_self_alphas(
        self, query: SelfAlphaListQueryParams
    ) -> tuple[SelfAlphaList, RateLimit]:
        resp = await alphas.get_self_alphas(self.session, query.to_params())
        return resp

    # -------------------------------
    # Data-related methods
    # -------------------------------
    @exception_handler
    async def get_data_categories(self) -> List[DataCategoriesParent]:
        resp = await data.fetch_data_categories(self.session)
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_datasets(self, query: DataSetsQueryParams) -> Optional[DataSets]:
        resp = await data.fetch_datasets(self.session, query.to_params())
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_dataset_detail(self, dataset_id: str) -> Optional[DatasetDetail]:
        resp = await data.fetch_dataset_detail(self.session, dataset_id)
        return resp

    @exception_handler
    async def get_data_field_detail(self, data_field_id: str) -> DataFieldDetail:
        resp = await data.fetch_data_field_detail(self.session, data_field_id)
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_data_fields_in_dataset(
        self, query: GetDataFieldsQueryParams
    ) -> Optional[DatasetDataFields]:
        resp = await data.fetch_dataset_data_fields(self.session, query.to_params())
        return resp

    # -------------------------------
    # Other utility methods
    # -------------------------------
    @exception_handler
    async def get_all_operators(self) -> Operators:
        resp = await other.get_all_operators(self.session)
        return resp
