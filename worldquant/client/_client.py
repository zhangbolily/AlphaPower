#!/usr/bin/env python
from typing import Optional

from aiohttp import BasicAuth, ClientSession, http_exceptions, TCPConnector

from worldquant.internal.http_api import alphas, common, data, other, simulations, user
from worldquant.internal.http_api.data import (
    DatasetDataFields,
    DatasetDetail,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
)
from worldquant.utils.client import exception_handler


class WorldQuantClient:
    def __init__(self, username, password, pool_connections=10, pool_maxsize=10):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.session = None
        self.username = username
        self.password = password
        self._auth_task = None
        connector = TCPConnector(
            limit=self.pool_connections, limit_per_host=self.pool_maxsize
        )
        auth = BasicAuth(self.username, self.password)
        self.session = ClientSession(connector=connector, auth=auth)
        self._auth_task = user.authentication(self.session)

    async def __aenter__(self):
        await self._auth_task
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @exception_handler
    async def get_self_alphas(
        self, query: alphas.SelfAlphaListQueryParams
    ) -> tuple[alphas.SelfAlphaList, common.RateLimit]:
        try:
            resp = await alphas.get_self_alphas(self.session, query.to_params())
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await alphas.get_self_alphas(self.session, query.to_params())
            else:
                raise
        return resp

    @exception_handler
    async def create_single_simulation(
        self, simulation_data: simulations.CreateSingleSimulationReq
    ) -> alphas.Alpha:
        try:
            success, progress_url, retry_after = (
                await simulations.create_single_simulation(
                    self.session, simulation_data
                )
            )
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                success, progress_url, retry_after = (
                    await simulations.create_single_simulation(
                        self.session, simulation_data
                    )
                )
            else:
                raise
        return success, progress_url, retry_after

    @exception_handler
    async def get_data_categories(self):
        try:
            resp = await data.fetch_data_categories(self.session)
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await data.fetch_data_categories(self.session)
            else:
                raise
        return resp

    @exception_handler
    async def get_datasets(self, query: DataSetsQueryParams) -> Optional[dict]:
        try:
            resp = await data.fetch_datasets(self.session, query.to_params())
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await data.fetch_datasets(self.session, query.to_params())
            else:
                raise
        return resp

    @exception_handler
    async def get_dataset_detail(self, dataset_id: int) -> Optional[DatasetDetail]:
        try:
            resp = await data.fetch_dataset_detail(self.session, dataset_id)
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await data.fetch_dataset_detail(self.session, dataset_id)
            else:
                raise
        return resp

    @exception_handler
    async def get_data_field_detail(self, data_field_id):
        try:
            resp = await data.fetch_data_field_detail(self.session, data_field_id)
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await data.fetch_data_field_detail(self.session, data_field_id)
            else:
                raise
        return resp

    @exception_handler
    async def get_data_fields_in_dataset(
        self, query: GetDataFieldsQueryParams
    ) -> Optional[DatasetDataFields]:
        try:
            resp = await data.fetch_dataset_data_fields(self.session, query.to_params())
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await data.fetch_dataset_data_fields(
                    self.session, query.to_params()
                )
            else:
                raise
        return resp

    @exception_handler
    async def get_all_operators(self):
        try:
            resp = await other.get_all_operators(self.session)
        except http_exceptions.HttpProcessingError as e:
            if e.code == 401:
                await self._auth_task
                resp = await other.get_all_operators(self.session)
            else:
                raise
        return resp
