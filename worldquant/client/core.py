#!/usr/bin/env python
import asyncio
from contextlib import suppress
from typing import Optional, Union

from aiohttp import BasicAuth, ClientSession, TCPConnector

from worldquant.internal.http_api import (
    alphas,
    common,
    data,
    DatasetDataFields,
    DatasetDetail,
    DataSets,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
    other,
    simulations,
    user,
)
from worldquant.internal.wraps import exception_handler, rate_limit_handler


def create_client(credentials, pool_connections=10, pool_maxsize=10):
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
    def __init__(self, username, password, pool_connections=10, pool_maxsize=10):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.session = None
        self.username = username
        self.password = password
        self._auth_task = None
        self._refresh_task = None
        self.shutdown = False
        self._initialize_session()

    def _initialize_session(self):
        """初始化 ClientSession"""
        connector = TCPConnector(
            limit=self.pool_connections, limit_per_host=self.pool_maxsize
        )
        auth = BasicAuth(self.username, self.password)
        self.session = ClientSession(connector=connector, auth=auth)
        self._auth_task = user.authentication(self.session)

    async def _refresh_session(self, expiry: int):
        """后台任务定期刷新会话"""
        while not self.shutdown:
            await asyncio.sleep(expiry - 60)  # 提前 60 秒刷新
            self._auth_task = user.authentication(self.session)
            session_info = await self._auth_task
            expiry = session_info.token.expiry  # 更新下次刷新时间

    async def __aenter__(self):
        session_info = await self._auth_task
        expiry = session_info.token.expiry
        self._refresh_task = asyncio.create_task(self._refresh_session(expiry))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
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
        self, simulation_data: simulations.SingleSimulationRequest
    ) -> tuple[bool, str, int]:
        success, progress_id, retry_after = await simulations.create_single_simulation(
            self.session, simulation_data.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def create_multi_simulation(
        self, simulation_data: simulations.MultiSimulationRequest
    ) -> tuple[bool, str, int]:
        success, progress_id, retry_after = await simulations.create_multi_simulation(
            self.session, simulation_data.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def get_single_simulation_progress(self, progress_id: str) -> tuple[
        bool,
        Union[simulations.SingleSimulationResult, simulations.SimulationProgress],
        int,
    ]:
        finished, progress_or_result, retry_after = (
            await simulations.get_simulation_progress(
                self.session, progress_id, is_multi=False
            )
        )
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_progress(self, progress_id: str) -> tuple[
        bool,
        Union[simulations.MultiSimulationResult, simulations.SimulationProgress],
        int,
    ]:
        finished, progress_or_result, retry_after = (
            await simulations.get_simulation_progress(
                self.session, progress_id, is_multi=True
            )
        )
        return finished, progress_or_result, retry_after

    @exception_handler
    async def get_multi_simulation_result(self, child_progress_id: str) -> tuple[
        bool,
        simulations.SingleSimulationResult,
        int,
    ]:
        finished, progress_or_result, retry_after = (
            await simulations.get_simulation_progress(
                self.session, child_progress_id, is_multi=False
            )
        )
        return finished, progress_or_result

    # -------------------------------
    # Alpha-related methods
    # -------------------------------
    @exception_handler
    @rate_limit_handler
    async def get_self_alphas(
        self, query: alphas.SelfAlphaListQueryParams
    ) -> tuple[alphas.SelfAlphaList, common.RateLimit]:
        resp = await alphas.get_self_alphas(self.session, query.to_params())
        return resp

    # -------------------------------
    # Data-related methods
    # -------------------------------
    @exception_handler
    async def get_data_categories(self):
        resp = await data.fetch_data_categories(self.session)
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_datasets(self, query: DataSetsQueryParams) -> Optional[DataSets]:
        resp = await data.fetch_datasets(self.session, query.to_params())
        return resp

    @exception_handler
    @rate_limit_handler
    async def get_dataset_detail(self, dataset_id: int) -> Optional[DatasetDetail]:
        resp = await data.fetch_dataset_detail(self.session, dataset_id)
        return resp

    @exception_handler
    async def get_data_field_detail(self, data_field_id):
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
    async def get_all_operators(self):
        resp = await other.get_all_operators(self.session)
        return resp
