#!/usr/bin/env python
import requests

from ._http_api import _user, _alphas, _simulations, _data, _other, _common
from .__exception import exception_handler


class WorldQuantClient:
    def __init__(self, username=None, password=None):
        self.session = requests.Session()
        self.username = username
        self.password = password
        _user.authentication(self.session, username, password)

    @exception_handler
    def get_self_alphas(
        self, query: _alphas.SelfAlphaListQueryParams
    ) -> tuple[_alphas.SelfAlphaList, _common.RateLimit]:
        try:
            resp = _alphas.get_self_alphas(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _alphas.authentication(self.session, self.username, self.password)
                resp = _alphas.get_self_alphas(self.session, query.to_params())
            else:
                raise
        return resp

    def create_single_simulation(
        self, simulation_data: _simulations.CreateSingleSimulationReq
    ) -> _alphas.Alpha:
        try:
            success, progress_url, retry_after = _simulations.create_single_simulation(
                self.session, simulation_data
            )
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                success, progress_url, retry_after = (
                    _simulations.create_single_simulation(self.session, simulation_data)
                )
            else:
                raise
        return success, progress_url, retry_after

    def get_data_categories(self):
        try:
            resp = _data.get_data_categories(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _data.get_data_categories(self.session)
            else:
                raise
        return resp

    def get_datasets(self, query: _data.DataSetsQueryParams):
        try:
            resp = _data.get_datasets(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _data.get_datasets(self.session, query.to_params())
            else:
                raise
        return resp

    def get_dataset_detail(self, dataset_id):
        try:
            resp = _data.get_dataset_detail(self.session, dataset_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _data.get_dataset_detail(self.session, dataset_id)
            else:
                raise
        return resp

    def get_data_field_detail(self, data_field_id):
        try:
            resp = _data.get_data_field_detail(self.session, data_field_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _data.get_data_field_detail(self.session, data_field_id)
            else:
                raise
        return resp

    def get_data_fields_in_dataset(self, query: _data.GetDataFieldsQueryParams):
        try:
            resp = _data.get_dataset_data_fields(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _data.get_dataset_data_fields(self.session, query.to_params())
            else:
                raise
        return resp

    def get_all_operators(self):
        try:
            resp = _other.get_all_operators(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _user.authentication(self.session, self.username, self.password)
                resp = _other.get_all_operators(self.session)
            else:
                raise
        return resp
