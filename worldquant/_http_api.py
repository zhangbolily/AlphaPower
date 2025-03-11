#!/usr/bin/env python

import json
import requests
import worldquant._common as _common


class AuthenticationResp_User:
    def __init__(self, id):
        self.id = id


class AuthenticationResp_Token:
    def __init__(self, expiry):
        self.expiry = expiry


class AuthenticationResp:
    def __init__(self, user, token, permissions):
        self.user = AuthenticationResp_User(**user)
        self.token = AuthenticationResp_Token(**token)
        self.permissions = permissions

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def authentication(session: requests.Session, username: str, password: str):
    """
    进行用户认证。

    参数:
    session (requests.Session): 用于发送HTTP请求的会话对象。
    username (str): 用户名。
    password (str): 密码。

    返回:
    AuthenticationResp: 认证响应对象。

    异常:
    requests.exceptions.HTTPError: 如果HTTP请求返回错误状态码。
    """
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_AUTHENTICATION}"
    response = session.post(url, auth=(username, password))
    response.raise_for_status()
    return AuthenticationResp.from_json(response.content)


class SelfAlphaListQueryParams:
    def __init__(self, hidden=None, limit=None, offset=None, order=None, status=None):
        self.hidden = hidden
        self.limit = limit
        self.offset = offset
        self.order = order
        self.status = status

    def to_params(self):
        params = {}
        if self.hidden is not None:
            params["hidden"] = self.hidden
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
        if self.order is not None:
            params["order"] = self.order
        if self.status is not None:
            params["status"] = self.status
        return params


class AlphaSettings:
    def __init__(
        self,
        instrumentType,
        region,
        universe,
        delay,
        decay,
        neutralization,
        truncation,
        pasteurization,
        unitHandling,
        nanHandling,
        language,
        visualization,
        testPeriod=None,
    ):
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.delay = delay
        self.decay = decay
        self.neutralization = neutralization
        self.truncation = truncation
        self.pasteurization = pasteurization
        self.unitHandling = unitHandling
        self.nanHandling = nanHandling
        self.language = language
        self.visualization = visualization
        self.testPeriod = testPeriod


class AlphaRegular:
    def __init__(self, code, description, operatorCount):
        self.code = code
        self.description = description
        self.operatorCount = operatorCount


class AlphaCheck:
    def __init__(
        self,
        name,
        result,
        limit=None,
        value=None,
        date=None,
        competitions=None,
        message=None,
    ):
        self.name = name
        self.result = result
        self.limit = limit
        self.value = value
        self.date = date
        self.competitions = competitions
        self.message = message


class AlphaIS:
    def __init__(
        self,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        returns,
        drawdown,
        margin,
        sharpe,
        fitness,
        startDate,
        checks,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.sharpe = sharpe
        self.fitness = fitness
        self.startDate = startDate
        self.checks = [AlphaCheck(**check) for check in checks]


class AlphaResult:
    def __init__(
        self,
        id,
        type,
        author,
        settings,
        regular,
        dateCreated,
        dateSubmitted,
        dateModified,
        name,
        favorite,
        hidden,
        color,
        category,
        tags,
        classifications,
        grade,
        stage,
        status,
        inSample,
        outSample,
        train,
        test,
        prod,
        competitions,
        themes,
        pyramids,
        team,
    ):
        self.id = id
        self.type = type
        self.author = author
        self.settings = AlphaSettings(**settings)
        self.regular = AlphaRegular(**regular)
        self.dateCreated = dateCreated
        self.dateSubmitted = dateSubmitted
        self.dateModified = dateModified
        self.name = name
        self.favorite = favorite
        self.hidden = hidden
        self.color = color
        self.category = category
        self.tags = tags
        self.classifications = classifications
        self.grade = grade
        self.stage = stage
        self.status = status
        self.inSample = AlphaIS(**inSample) if inSample else None
        self.outSample = outSample
        self.train = train
        self.test = test
        self.prod = prod
        self.competitions = competitions
        self.themes = themes
        self.pyramids = pyramids
        self.team = team


class SelfAlphaListResp:
    def __init__(self, count, next, previous, results):
        self.count = count
        self.next = next
        self.previous = previous
        self.results = [AlphaResult(**result) for result in results]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)

        # 字段名映射
        field_mapping = {
            "is": "inSample",
            "os": "outSample",
        }

        def map_fields(obj):
            if isinstance(obj, dict):
                return {field_mapping.get(k, k): map_fields(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [map_fields(i) for i in obj]
            else:
                return obj

        mapped_data = map_fields(data)
        return cls(**mapped_data)


def get_alphas(session: requests.Session, params=None):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_SELF_ALPHA_LIST}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return SelfAlphaListResp.from_json(response.content)


class SimulationSettings:
    def __init__(
        self,
        nanHandling,
        instrumentType,
        delay,
        universe,
        truncation,
        unitHandling,
        testPeriod,
        pasteurization,
        region,
        language,
        decay,
        neutralization,
        visualization,
    ):
        self.nanHandling = nanHandling
        self.instrumentType = instrumentType
        self.delay = delay
        self.universe = universe
        self.truncation = truncation
        self.unitHandling = unitHandling
        self.testPeriod = testPeriod
        self.pasteurization = pasteurization
        self.region = region
        self.language = language
        self.decay = decay
        self.neutralization = neutralization
        self.visualization = visualization


class CreateSingleSimulationReq:
    def __init__(self, type, settings, regular):
        self.type = type
        self.settings = SimulationSettings(**settings)
        self.regular = regular

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def create_single_simulation(session: requests.Session, simulation_data):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_SIMULATION}"
    response = session.post(url, json=simulation_data)
    response.raise_for_status()
    # TODO: 这里的返回结果，需要根据状态码进行判断，后面需要抓包看一下
    if response.status_code == 201:
        # 创建成功，返回模拟进度轮询地址、轮询间隔时间
        return (True, response.headers["Location"], response.headers["Retry-After"])
    return (False, "", 0)


class SingleSimulationProgress:
    def __init__(self, progress):
        self.progress = progress

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class ErrorLocation:
    def __init__(self, line, start, end, property):
        self.line = line
        self.start = start
        self.end = end
        self.property = property


class SimulationSettings:
    def __init__(
        self,
        instrumentType,
        region,
        universe,
        delay,
        decay,
        neutralization,
        truncation,
        pasteurization,
        unitHandling,
        nanHandling,
        language,
        visualization,
    ):
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.delay = delay
        self.decay = decay
        self.neutralization = neutralization
        self.truncation = truncation
        self.pasteurization = pasteurization
        self.unitHandling = unitHandling
        self.nanHandling = nanHandling
        self.language = language
        self.visualization = visualization


class SingleSimulationResult:
    def __init__(
        self,
        id,
        type,
        status,
        message=None,
        location=None,
        settings=None,
        regular=None,
        alpha=None,
    ):
        self.id = id
        self.type = type
        self.status = status
        self.message = message
        self.location = ErrorLocation(**location) if location else None
        self.settings = SimulationSettings(**settings) if settings else None
        self.regular = regular
        self.alpha = alpha

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def get_single_simulation_progress(session: requests.Session, progress_url):
    """
    从给定的URL获取单次模拟的进度或结果。

    参数:
        session (requests.Session): 用于发送HTTP请求的会话对象。
        progress_url (str): 获取模拟进度或结果的URL。

    返回:
        tuple: 包含以下内容的元组:
            - finished (bool): 模拟是否完成。
            - progress_or_result (SingleSimulationProgress 或 SingleSimulationResult):
              如果模拟仍在运行，则为模拟进度；如果模拟已完成，则为模拟结果。
            - retry_after (int): 如果模拟仍在运行，则为重试前等待的秒数。
    """
    response = session.get(progress_url)
    response.raise_for_status()

    if response.headers.get("Retry-After") is not None:
        # 模拟中，返回模拟进度
        retry_after = response.headers["Retry-After"]
        finished = False
        return (
            finished,
            SingleSimulationProgress.from_json(response.content),
            retry_after,
        )
    else:
        # 模拟完成，返回模拟结果
        finished = True
        return finished, SingleSimulationResult.from_json(response.content), 0


class SelfSimulationActivities_Period:
    def __init__(self, start, end, value):
        self.start = start
        self.end = end
        self.value = value


class SelfSimulationActivities_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class SelfSimulationActivities_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [
            SelfSimulationActivities_Property(**prop) for prop in properties
        ]


class SelfSimulationActivities_Records:
    def __init__(self, schema, records):
        self.schema = SelfSimulationActivities_Schema(**schema)
        self.records = records


class SelfSimulationActivities:
    def __init__(self, yesterday, current, previous, ytd, total, records, type):
        self.yesterday = SelfSimulationActivities_Period(**yesterday)
        self.current = SelfSimulationActivities_Period(**current)
        self.previous = SelfSimulationActivities_Period(**previous)
        self.ytd = SelfSimulationActivities_Period(**ytd)
        self.total = SelfSimulationActivities_Period(**total)
        self.records = SelfSimulationActivities_Records(**records)
        self.type = type

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def get_self_simulation_activities(session: requests.Session, date: str):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ACTIVITIES_SIMULATION}"
    response = session.get(url, params={"date": date})
    response.raise_for_status()
    return SelfSimulationActivities.from_json(response.content)


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
        self.children = [DataCategoriesChild(**child) for child in children]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return [cls(**item) for item in data]


def get_data_categories(session: requests.Session):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_DATA_CATEGORIES}"
    response = session.get(url)
    response.raise_for_status()
    return DataCategoriesParent.from_json(response.content)


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


class DatasetListItemCategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetListItemSubcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetListItem:
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
    ):
        self.id = id
        self.name = name
        self.description = description
        self.category = DatasetListItemCategory(**category)
        self.subcategory = DatasetListItemSubcategory(**subcategory)
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


class DatasetListResponse:
    def __init__(self, count, results):
        self.count = count
        self.results = [DatasetListItem(**result) for result in results]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def get_datasets(session: requests.Session, params=None):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_DATA_SETS}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return DatasetListResponse.from_json(response.content)


class DatasetDetailCategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetDetailSubcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DatasetDetailDataItem:
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


class DatasetDetail:
    def __init__(self, name, description, category, subcategory, data, researchPapers):
        self.name = name
        self.description = description
        self.category = DatasetDetailCategory(**category)
        self.subcategory = DatasetDetailSubcategory(**subcategory)
        self.data = [DatasetDetailDataItem(**item) for item in data]
        self.researchPapers = researchPapers

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def get_dataset_detail(session: requests.Session, dataset_id: str):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_DATA_SETS}/{dataset_id}"
    response = session.get(url)
    response.raise_for_status()
    return DatasetDetail.from_json(response.content)


class DataFieldDetailCategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetailSubcategory:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetailDataItem:
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


class DataFieldDetailDataset:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class DataFieldDetail:
    def __init__(self, dataset, category, subcategory, description, type, data):
        self.dataset = DataFieldDetailDataset(**dataset)
        self.category = DataFieldDetailCategory(**category)
        self.subcategory = DataFieldDetailSubcategory(**subcategory)
        self.description = description
        self.type = type
        self.data = [DataFieldDetailDataItem(**item) for item in data]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


def get_data_field_detail(session: requests.Session, field_id: str):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_DATA_FIELDS}/{field_id}"
    response = session.get(url)
    response.raise_for_status()
    return DataFieldDetail.from_json(response.content)


class Dataset:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Category:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Subcategory:
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
        self.dataset = Dataset(**dataset)
        self.category = Category(**category)
        self.subcategory = Subcategory(**subcategory)
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
        self.results = [DataField(**result) for result in results]

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
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_DATA_FIELDS}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return DatasetDataFields.from_json(response.content)


class Operator:
    def __init__(
        self, name, category, scope, definition, description, documentation, level
    ):
        self.name = name
        self.category = category
        self.scope = scope
        self.definition = definition
        self.description = description
        self.documentation = documentation
        self.level = level


class Operators:
    def __init__(self, operators):
        self.operators = [Operator(**operator) for operator in operators]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(data)


def get_all_operators(session: requests.Session):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_OPERATORS}"
    response = session.get(url)
    response.raise_for_status()
    return Operators.from_json(response.content)
