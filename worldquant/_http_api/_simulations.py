import json
import requests
import worldquant._http_api._common as _common


class SingleSimulationResult_ErrorLocation:
    def __init__(self, line, start, end, property):
        self.line = line
        self.start = start
        self.end = end
        self.property = property


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


class SingleSimulationProgress:
    def __init__(self, progress):
        self.progress = progress

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


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
        self.location = (
            SingleSimulationResult_ErrorLocation(**location) if location else None
        )
        self.settings = SimulationSettings(**settings) if settings else None
        self.regular = regular
        self.alpha = alpha

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


class CreateSingleSimulationReq:
    def __init__(self, type, settings, regular):
        self.type = type
        self.settings = SimulationSettings(**settings)
        self.regular = regular

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


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


def create_single_simulation(session: requests.Session, simulation_data):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_SIMULATION}"
    response = session.post(url, json=simulation_data)
    response.raise_for_status()
    if response.status_code == 201:
        # 创建成功，返回模拟进度轮询地址、轮询间隔时间
        return (True, response.headers["Location"], response.headers["Retry-After"])
    return (False, "", 0)


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
            _common.RateLimit.from_headers(response.headers),
        )
    else:
        # 模拟完成，返回模拟结果
        finished = True
        return (
            finished,
            SingleSimulationResult.from_json(response.content),
            0,
            _common.RateLimit.from_headers(response.headers),
        )


def get_self_simulation_activities(session: requests.Session, date: str):
    url = f"{_common.BASE_URL}/{_common.ENDPOINT_ACTIVITIES_SIMULATION}"
    response = session.get(url, params={"date": date})
    response.raise_for_status()
    return SelfSimulationActivities.from_json(response.content)
