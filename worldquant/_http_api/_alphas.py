import json
import requests
import worldquant._http_api._common as _common
import pandas as pd


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


class AlphaResult_Settings:
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


class AlphaResult_Regular:
    def __init__(self, code, description, operatorCount):
        self.code = code
        self.description = description
        self.operatorCount = operatorCount


class AlphaResult_IS_Check:
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


class AlphaResult_IS:
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
        self.checks = [AlphaResult_IS_Check(**check) for check in checks]


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
        self.settings = AlphaResult_Settings(**settings)
        self.regular = AlphaResult_Regular(**regular)
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
        self.inSample = AlphaResult_IS(**inSample) if inSample else None
        self.outSample = outSample  # TODO: 未定义，没有参考数据
        self.train = train
        self.test = test
        self.prod = prod
        self.competitions = competitions
        self.themes = themes
        self.pyramids = pyramids
        self.team = team


class SelfAlphaList:
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


class AlphaDetail_Settings:
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
        testPeriod,
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


class AlphaDetail_Regular:
    def __init__(self, code, description, operatorCount):
        self.code = code
        self.description = description
        self.operatorCount = operatorCount


class AlphaDetail_IS_Check:
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


class AlphaDetail_IS:
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
        self.checks = [AlphaDetail_IS_Check(**check) for check in checks]


class AlphaDetail_Train:
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
        fitness,
        sharpe,
        startDate,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.sharpe = sharpe
        self.startDate = startDate


class AlphaDetail_Test:
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
        fitness,
        sharpe,
        startDate,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.sharpe = sharpe
        self.startDate = startDate


class AlphaDetail_Classification:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class AlphaDetail:
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
        is_,
        os_,
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
        self.settings = AlphaDetail_Settings(**settings)
        self.regular = AlphaDetail_Regular(**regular)
        self.dateCreated = dateCreated
        self.dateSubmitted = dateSubmitted
        self.dateModified = dateModified
        self.name = name
        self.favorite = favorite
        self.hidden = hidden
        self.color = color
        self.category = category
        self.tags = tags
        self.classifications = [
            AlphaDetail_Classification(**classification)
            for classification in classifications
        ]
        self.grade = grade
        self.stage = stage
        self.status = status
        self.is_ = AlphaDetail_IS(**is_) if is_ else None
        self.os_ = os_
        self.train = AlphaDetail_Train(**train) if train else None
        self.test = AlphaDetail_Test(**test) if test else None
        self.prod = prod
        self.competitions = competitions
        self.themes = themes
        self.pyramids = pyramids
        self.team = team

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class AlphaYearlyStats_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaYearlyStats_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [AlphaYearlyStats_Property(**prop) for prop in properties]


class AlphaYearlyStats_Record:
    def __init__(
        self,
        year,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        sharpe,
        returns,
        drawdown,
        margin,
        fitness,
        stage,
    ):
        self.year = year
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.sharpe = sharpe
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.stage = stage


class AlphaYearlyStats:
    def __init__(self, schema, records):
        self.schema = AlphaYearlyStats_Schema(**schema)
        self.records = [AlphaYearlyStats_Record(*record) for record in records]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)

    def to_dataframe(self):
        data = []
        for record in self.records:
            data.append([getattr(record, prop.name) for prop in self.schema.properties])
        return pd.DataFrame(
            data, columns=[prop.title for prop in self.schema.properties]
        )


class AlphaPnL_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaPnL_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [AlphaPnL_Property(**prop) for prop in properties]


class AlphaPnL_Record:
    def __init__(self, date, pnl):
        self.date = date
        self.pnl = pnl


class AlphaPnL:
    def __init__(self, schema, records):
        self.schema = AlphaPnL_Schema(**schema)
        self.records = [AlphaPnL_Record(*record) for record in records]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class AlphaSelfCorrelations_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaSelfCorrelations_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [
            AlphaSelfCorrelations_Property(**prop) for prop in properties
        ]


class AlphaSelfCorrelations_Record:
    def __init__(
        self,
        id,
        name,
        instrumentType,
        region,
        universe,
        correlation,
        sharpe,
        returns,
        turnover,
        fitness,
        margin,
    ):
        self.id = id
        self.name = name
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.correlation = correlation
        self.sharpe = sharpe
        self.returns = returns
        self.turnover = turnover
        self.fitness = fitness
        self.margin = margin


class AlphaSelfCorrelations:
    def __init__(self, schema, records, min, max):
        self.schema = AlphaSelfCorrelations_Schema(**schema)
        self.records = [AlphaSelfCorrelations_Record(*record) for record in records]
        self.min = min
        self.max = max

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class AlphaPropertiesBody_Regular:
    def __init__(self, description):
        self.description = description


class AlphaPropertiesBody:
    def __init__(self, color, name, tags, category, regular):
        self.color = color
        self.name = name
        self.tags = tags
        self.category = category
        self.regular = AlphaPropertiesBody_Regular(**regular)

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class AplhaCheckResult:
    def __init__(self, inSample, outSample):
        self.inSample = inSample
        self.outSample = outSample

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


def get_self_alphas(session: requests.Session, params=None):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_SELF_ALPHA_LIST}"
    response = session.get(url, params=params)
    response.raise_for_status()
    return SelfAlphaList.from_json(response.content), _common.RateLimit.from_headers(
        response.headers
    )


def get_alpha_detail(session: requests.Session, alpha_id):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHAS}/{alpha_id}"
    response = session.get(url)
    response.raise_for_status()
    return AlphaDetail.from_json(response.content), _common.RateLimit.from_headers(
        response.headers
    )


def get_alpha_yearly_stats(session: requests.Session, alpha_id):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHA_YEARLY_STATS(alpha_id)}"
    response = session.get(url)
    response.raise_for_status()
    return AlphaYearlyStats.from_json(response.content), _common.RateLimit.from_headers(
        response.headers
    )


def get_alpha_pnl(session: requests.Session, alpha_id):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHA_PNL(alpha_id)}"
    response = session.get(url)
    response.raise_for_status()
    return AlphaPnL.from_json(response.content), _common.RateLimit.from_headers(
        response.headers
    )


def get_alpha_self_correlations(session: requests.Session, alpha_id):
    """
    获取指定 alpha 的自相关性数据。

    参数:
    session (requests.Session): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    tuple: 包含以下元素的元组:
        - finished (bool): 请求是否已完成。
        - retry_after (str 或 None): 如果请求被速率限制，返回重试时间，否则为 None。
        - AlphaSelfCorrelations 或 None: 如果请求成功，返回 AlphaSelfCorrelations 对象，否则为 None。
        - RateLimit: 包含速率限制信息的对象。
    """
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHA_SELF_CORRELATIONS(alpha_id)}"
    response = session.get(url)
    response.raise_for_status()
    finished = False
    retry_after = response.headers.get("Retry-After")

    if retry_after is not None:
        return (
            finished,
            retry_after,
            None,
            _common.RateLimit.from_headers(response.headers),
        )
    else:
        finished = True
        return (
            finished,
            None,
            AlphaSelfCorrelations.from_json(response.content),
            _common.RateLimit.from_headers(response.headers),
        )


def set_alpha_properties(
    session: requests.Session, alpha_id, properties: AlphaPropertiesBody
):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHAS}/{alpha_id}"
    response = session.patch(url, json=properties)
    response.raise_for_status()
    return response.json(), _common.RateLimit.from_headers(response.headers)


def alpha_check_submission(session: requests.Session, alpha_id):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ALPHAS}/{alpha_id}/check"
    response = session.get(url)
    response.raise_for_status()

    finished = False
    retry_after = response.headers.get("Retry-After")

    if retry_after is not None:
        return (
            finished,
            retry_after,
            None,
            _common.RateLimit.from_headers(response.headers),
        )
    else:
        finished = True
        return (
            finished,
            None,
            AplhaCheckResult.from_json(response.content),
            _common.RateLimit.from_headers(response.headers),
        )
