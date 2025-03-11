#!/usr/bin/env python
import requests
import _http_api
import json

class WorldQuantClient:
    def __init__(self, username=None, password=None):
        self.session = requests.Session()
        self.username = username
        self.password = password
        _http_api.authentication(self.session, username, password)

    def get_self_alphas(
        self, query: _http_api.SelfAlphaListQueryParams
    ) -> _http_api.SelfAlphaListResp:
        try:
            resp = _http_api.get_alphas(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_alphas(self.session, query.to_params())
        return resp

    def create_single_simulation(
        self, simulation_data: _http_api.CreateSingleSimulationReq
    ) -> _http_api.AlphaResult:
        try:
            success, progress_url, retry_after = _http_api.create_single_simulation(self.session, simulation_data)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                success, progress_url, retry_after = _http_api.create_single_simulation(self.session, simulation_data)
        return success, progress_url, retry_after

    def get_data_categories(self):
        try:
            resp = _http_api.get_data_categories(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_data_categories(self.session)
        return resp

    def get_datasets(self, query: _http_api.DataSetsQueryParams):
        try:
            resp = _http_api.get_datasets(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_datasets(self.session, query.to_params())
        return resp

    def get_dataset_detail(self, dataset_id):
        try:
            resp = _http_api.get_dataset_detail(self.session, dataset_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_dataset_detail(self.session, dataset_id)
        return resp

    def get_data_field_detail(self, data_field_id):
        try:
            resp = _http_api.get_data_field_detail(self.session, data_field_id)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_data_field_detail(self.session, data_field_id)
        return resp

    def get_data_fields_in_dataset(self, query: _http_api.GetDataFieldsQueryParams):
        try:
            resp = _http_api.get_dataset_data_fields(self.session, query.to_params())
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_dataset_data_fields(
                    self.session, query.to_params()
                )
        return resp

    def get_all_operators(self):
        try:
            resp = _http_api.get_all_operators(self.session)
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                _http_api.authentication(self.session, self.username, self.password)
                resp = _http_api.get_all_operators(self.session)
        return resp


if __name__ == "__main__":
    with open('credentials.json') as f:
        credentials = json.load(f)
        username = credentials['username']
        password = credentials['password']

    client = WorldQuantClient(username=username, password=password)

    query = _http_api.SelfAlphaListQueryParams(
        hidden="false", limit=5, offset=0, order="-dateCreated", status="UNSUBMITTED"
    )
    response = client.get_self_alphas(query)
    print(response)

    response = client.get_data_categories()
    print(response)

    query = _http_api.DataSetsQueryParams(
        limit=5,
        offset=0,
        universe="TOP3000",
        region="USA",
    )
    response = client.get_datasets(query)
    print(response)

    dataset_id = response.results[0].id
    response = client.get_dataset_detail(dataset_id)
    print(response)

    query = _http_api.GetDataFieldsQueryParams(
        dataset_id=dataset_id,
        limit=5,
        offset=0,
        region="USA",
        instrumentType="EQUITY",
        delay=1,
        universe="TOP3000",
    )
    response = client.get_data_fields_in_dataset(query)
    print(response)
