#!/usr/bin/env python
import asyncio
from contextlib import suppress
from typing import Optional

from aiohttp import BasicAuth, ClientSession, TCPConnector

from worldquant.internal.http_api import alphas, common, data, other, simulations, user
from worldquant.internal.http_api.data import (
    DatasetDataFields,
    DatasetDetail,
    DataSets,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
)
from worldquant.internal.wraps import exception_handler, rate_limit_handler


class WorldQuantClient:
    def __init__(self, username, password, pool_connections=10, pool_maxsize=10):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.session = None
        self.username = username
        self.password = password
        self._auth_task = None
        self._refresh_task = None
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
        while True:
            await asyncio.sleep(expiry - 60)  # 提前 60 秒刷新
            self._auth_task = user.authentication(
                self.session, self.username, self.password
            )
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

    @exception_handler
    @rate_limit_handler
    async def get_self_alphas(
        self, query: alphas.SelfAlphaListQueryParams
    ) -> tuple[alphas.SelfAlphaList, common.RateLimit]:
        resp = await alphas.get_self_alphas(self.session, query.to_params())
        return resp

    @exception_handler
    async def create_single_simulation(
        self, simulation_data: simulations.CreateSingleSimulationReq
    ) -> alphas.Alpha:
        success, progress_url, retry_after = await simulations.create_single_simulation(
            self.session, simulation_data
        )
        return success, progress_url, retry_after

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

    @exception_handler
    async def get_all_operators(self):
        resp = await other.get_all_operators(self.session)
        return resp
