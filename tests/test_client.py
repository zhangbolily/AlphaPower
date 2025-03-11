from worldquant._client import WorldQuantClient
from worldquant._http_api import _alphas, _data
import json

if __name__ == "__main__":
    with open("credentials.json") as f:
        credentials = json.load(f)
        username = credentials["username"]
        password = credentials["password"]

    client = WorldQuantClient(username=username, password=password)

    query = _alphas.SelfAlphaListQueryParams(
        hidden="false", limit=5, offset=0, order="-dateCreated", status="UNSUBMITTED"
    )
    response = client.get_self_alphas(query)
    print(response)

    response = client.get_data_categories()
    print(response)

    query = _data.DataSetsQueryParams(
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

    query = _data.GetDataFieldsQueryParams(
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