import json

import requests
from .common import *


class DataCategoriesChild:
    def __init__(
        self,
        id,
        name,
        datasetCount,
        fieldCount,
        alphaCount,
        userCount,
        valueScore,
        region,
        children=None,
    ):
        self.id = id
        self.name = name
        self.datasetCount = datasetCount
        self.fieldCount = fieldCount
        self.alphaCount = alphaCount
        self.userCount = userCount
        self.valueScore = valueScore
        self.region = region
        self.children = (
            [DataCategoriesChild(**child) for child in children] if children else []
        )


class DataCategoriesParent:
    def __init__(
        self,
        id,
        name,
        datasetCount,
        fieldCount,
        alphaCount,
        userCount,
        valueScore,
        region,
        children,
    ):
        self.id = id
        self.name = name
        self.datasetCount = datasetCount
        self.fieldCount = fieldCount
        self.alphaCount = alphaCount
        self.userCount = userCount
        self.valueScore = valueScore
        self.region = region
        self.children = (
            [DataCategoriesChild(**child) for child in children] if children else []
        )

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return [cls(**item) for item in data] if data else []


class DataSetsQueryParams:
    def __init__(
        self,
        category=None,
        delay=None,
        instrumentType=None,
        limit=None,
        offset=None,
        region=None,
        universe=None,
    ):
        self.category = category
        self.delay = delay
        self.instrumentType = instrumentType
        self.limit = limit
        self.offset = offset
        self.region = region
        self.universe = universe

    def to_params(self):
        return {
            "category": self.category,
            "delay": self.delay,
            "instrumentType": self.instrumentType,
            "limit": self.limit,
            "offset": self.offset,
            "region": self.region,
            "universe": self.universe,
        }


class DataSets_ItemCategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataSets_ItemSubcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataSets_Item:
    def __init__(
        self,
        id,
        name,
        description,
        category,
        subcategory,
        region,
        delay,
        universe,
        coverage,
        valueScore,
        userCount,
        alphaCount,
        fieldCount,
        themes,
        researchPapers,
        pyramidMultiplier=None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.category = DataSets_ItemCategory(**category)
        self.subcategory = DataSets_ItemSubcategory(**subcategory)
        self.region = region
        self.delay = delay
        self.universe = universe
        self.coverage = coverage
        self.valueScore = valueScore
        self.userCount = userCount
        self.alphaCount = alphaCount
        self.fieldCount = fieldCount
        self.themes = themes
        self.researchPapers = researchPapers
        self.pyramidMultiplier = pyramidMultiplier


class DataSets:
    def __init__(self, count, results):
        self.count = count
        self.results = (
            [DataSets_Item(**result) for result in results] if results else []
        )

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class DatasetDetail_Category:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetDetail_Subcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetDetail_DataItem:
    def __init__(
        self,
        region,
        delay,
        universe,
        coverage,
        valueScore,
        userCount,
        alphaCount,
        fieldCount,
        themes,
        pyramidMultiplier=None,
    ):
        self.region = region
        self.delay = delay
        self.universe = universe
        self.coverage = coverage
        self.valueScore = valueScore
        self.userCount = userCount
        self.alphaCount = alphaCount
        self.fieldCount = fieldCount
        self.themes = themes
        self.pyramidMultiplier = pyramidMultiplier


class DatasetDetail_ResearchPaper:
    def __init__(self, type, title, url):
        self.type = type
        self.title = title
        self.url = url


class DatasetDetail:
    def __init__(self, name, description, category, subcategory, data, researchPapers):
        self.name = name
        self.description = description
        self.category = DatasetDetail_Category(**category)
        self.subcategory = DatasetDetail_Subcategory(**subcategory)
        self.data = [DatasetDetail_DataItem(**item) for item in data] if data else []
        self.researchPapers = self.researchPapers = (
            [DatasetDetail_ResearchPaper(**paper) for paper in researchPapers]
            if researchPapers
            else []
        )

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class DataFieldDetail_Category:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetail_Subcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetail_DataItem:
    def __init__(
        self, region, delay, universe, coverage, userCount, alphaCount, themes
    ):
        self.region = region
        self.delay = delay
        self.universe = universe
        self.coverage = coverage
        self.userCount = userCount
        self.alphaCount = alphaCount
        self.themes = themes


class DataFieldDetail_Dataset:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetail:
    def __init__(self, dataset, category, subcategory, description, type, data):
        self.dataset = DataFieldDetail_Dataset(**dataset)
        self.category = DataFieldDetail_Category(**category)
        self.subcategory = DataFieldDetail_Subcategory(**subcategory)
        self.description = description
        self.type = type
        self.data = [DataFieldDetail_DataItem(**item) for item in data] if data else []

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class DataField_Dataset:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataField_Category:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataField_Subcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataField:
    def __init__(
        self,
        id,
        description,
        dataset,
        category,
        subcategory,
        region,
        delay,
        universe,
        type,
        coverage,
        userCount,
        alphaCount,
        themes,
    ):
        self.id = id
        self.description = description
        self.dataset = DataField_Dataset(**dataset)
        self.category = DataField_Category(**category)
        self.subcategory = DataField_Subcategory(**subcategory)
        self.region = region
        self.delay = delay
        self.universe = universe
        self.type = type
        self.coverage = coverage
        self.userCount = userCount
        self.alphaCount = alphaCount
        self.themes = themes


class DatasetDataFields:
    def __init__(self, count, results):
        self.count = count
        self.results = [DataField(**result) for result in results] if results else []

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class GetDataFieldsQueryParams:
    def __init__(
        self, dataset_id, delay, instrumentType, limit, offset, region, universe
    ):
        self.dataset_id = dataset_id
        self.delay = delay
        self.instrumentType = instrumentType
        self.limit = limit
        self.offset = offset
        self.region = region
        self.universe = universe

    def to_params(self):
        return {
            "dataset.id": self.dataset_id,
            "delay": self.delay,
            "instrumentType": self.instrumentType,
            "limit": self.limit,
            "offset": self.offset,
            "region": self.region,
            "universe": self.universe,
        }


def get_dataset_data_fields(session: requests.Session, params):
    url = f"{BASE_URL}/{ENDPOINT_DATA_FIELDS}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return DatasetDataFields.from_json(response.content)


def get_data_field_detail(session: requests.Session, field_id: str):
    url = f"{BASE_URL}/{ENDPOINT_DATA_FIELDS}/{field_id}"
    response = session.get(url)
    response.raise_for_status()
    return DataFieldDetail.from_json(response.content)


def get_dataset_detail(session: requests.Session, dataset_id: str):
    url = f"{BASE_URL}/{ENDPOINT_DATA_SETS}/{dataset_id}"
    response = session.get(url)
    response.raise_for_status()
    return DatasetDetail.from_json(response.content)


def get_datasets(session: requests.Session, params=None):
    url = f"{BASE_URL}/{ENDPOINT_DATA_SETS}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return DataSets.from_json(response.content)


def get_data_categories(session: requests.Session):
    url = f"{BASE_URL}/{ENDPOINT_DATA_CATEGORIES}"
    response = session.get(url)
    response.raise_for_status()
    return DataCategoriesParent.from_json(response.content)
