#!/usr/bin/env python
from typing import Optional

import requests
from requests import adapters

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
        self.session = requests.Session()
        self.session.mount(
            "https://",
            adapters.HTTPAdapter(
                pool_connections=self.pool_connections, pool_maxsize=self.pool_maxsize
            ),
        )
        self.session.mount(
            "http://",
            adapters.HTTPAdapter(
                pool_connections=self.pool_connections, pool_maxsize=self.pool_maxsize
            ),
        )
        self.username = username
        self.password = password
        user.authentication(self.session, username, password)

    @exception_handler
    def get_self_alphas(
        self, query: alphas.SelfAlphaListQueryParams
    ) -> tuple[alphas.SelfAlphaList, common.RateLimit]:
        try:
            resp = alphas.get_self_alphas(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                alphas.authentication(self.session, self.username, self.password)
                resp = alphas.get_self_alphas(self.session, query.to_params())
            else:
                raise
        return resp

    @exception_handler
    def create_single_simulation(
        self, simulation_data: simulations.CreateSingleSimulationReq
    ) -> alphas.Alpha:
        try:
            success, progress_url, retry_after = simulations.create_single_simulation(
                self.session, simulation_data
            )
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                success, progress_url, retry_after = (
                    simulations.create_single_simulation(self.session, simulation_data)
                )
            else:
                raise
        return success, progress_url, retry_after

    @exception_handler
    def get_data_categories(self):
        try:
            resp = data.fetch_data_categories(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = data.fetch_data_categories(self.session)
            else:
                raise
        return resp

    @exception_handler
    def get_datasets(self, query: DataSetsQueryParams) -> Optional[dict]:
        try:
            resp = data.fetch_datasets(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = data.fetch_datasets(self.session, query.to_params())
            else:
                raise
        return resp

    @exception_handler
    def get_dataset_detail(self, dataset_id: int) -> Optional[DatasetDetail]:
        try:
            resp = data.fetch_dataset_detail(self.session, dataset_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = data.fetch_dataset_detail(self.session, dataset_id)
            else:
                raise
        return resp

    @exception_handler
    def get_data_field_detail(self, data_field_id):
        try:
            resp = data.fetch_data_field_detail(self.session, data_field_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = data.fetch_data_field_detail(self.session, data_field_id)
            else:
                raise
        return resp

    @exception_handler
    def get_data_fields_in_dataset(
        self, query: GetDataFieldsQueryParams
    ) -> Optional[DatasetDataFields]:
        try:
            resp = data.fetch_dataset_data_fields(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = data.fetch_dataset_data_fields(self.session, query.to_params())
            else:
                raise
        return resp

    @exception_handler
    def get_all_operators(self):
        try:
            resp = other.get_all_operators(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                user.authentication(self.session, self.username, self.password)
                resp = other.get_all_operators(self.session)
            else:
                raise
        return resp
