"""
alphapower.client.raw_api.alphas
========================
AlphaPower Alphas API
========================
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession
from multidict import CIMultiDictProxy
from structlog.stdlib import BoundLogger

from alphapower.constants import (
    BASE_URL,
    ENDPOINT_ACTIVITIES_SIMULATION,
    ENDPOINT_ALPHA_PNL,
    ENDPOINT_ALPHA_SELF_CORRELATIONS,
    ENDPOINT_ALPHA_YEARLY_STATS,
    ENDPOINT_ALPHAS,
    ENDPOINT_AUTHENTICATION,
    ENDPOINT_BEFORE_AND_AFTER_PERFORMANCE,
    ENDPOINT_COMPETITIONS,
    ENDPOINT_DATA_CATEGORIES,
    ENDPOINT_DATA_FIELDS,
    ENDPOINT_DATA_SETS,
    ENDPOINT_OPERATORS,
    ENDPOINT_SELF_ALPHA_LIST,
    ENDPOINT_SIMULATION,
    CorrelationType,
)
from alphapower.internal.logging import get_logger

from .checks_view import BeforeAndAfterPerformanceView, SubmissionCheckResultView
from .common_view import TableView
from .models import (
    AlphaDetailView,
    AlphaPropertiesPayload,
    AuthenticationView,
    CompetitionListView,
    DataCategoriesListView,
    DataFieldListView,
    DatasetDataFieldsView,
    DatasetDetailView,
    DatasetListView,
    MultiSimulationResultView,
    Operators,
    RateLimit,
    SelfAlphaListView,
    SelfSimulationActivitiesView,
    SimulationProgressView,
    SingleSimulationResultView,
)

log: BoundLogger = get_logger(module_name=__name__)

DEFAULT_SIMULATION_RESPONSE: Tuple[bool, str, float] = (False, "", 0.0)


def retry_after_from_headers(headers: CIMultiDictProxy[str]) -> float:
    """ "
    从响应头中提取重试时间

    :param headers: 响应头
    :return: 重试时间（秒）
    """
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return 0.0


def quote_alpha_list_query_params(
    params: Optional[Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    """
    对 alpha 列表查询参数进行编码。

    参数:
    params (Optional[Dict[str, Any]]): 查询参数的字典。

    返回:
    Tuple[str, Dict[str, Any]]: 编码后的查询参数字符串和剩余参数字典。
    """
    if not params:
        return "", {}

    quoted_params: str = ""
    rem_params: Dict[str, Any] = {}

    switch: Dict[str, str] = {
        "status!": "status!={}",
        "dateCreated>": "dateCreated%3E={}",
        "dateCreated<": "dateCreated%3C{}",
    }

    for key, value in params.items():
        if key in switch:
            quoted_params += switch[key].format(value) + "&"
        else:
            rem_params[key] = value

    if quoted_params:
        quoted_params = quoted_params[:-1]

    return quoted_params, rem_params


async def get_self_alphas(
    session: aiohttp.ClientSession, params: Optional[Dict[str, Any]] = None
) -> Tuple[SelfAlphaListView, RateLimit]:
    """
    获取当前用户的 alpha 列表。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    params (Optional[Dict[str, Any]]): 查询参数。

    返回:
    Tuple[SelfAlphaList, RateLimit]: 包含 alpha 列表和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_SELF_ALPHA_LIST}"
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return SelfAlphaListView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_detail(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[AlphaDetailView, RateLimit]:
    """
    获取指定 alpha 的详细信息。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    Tuple[AlphaDetail, RateLimit]: 包含 alpha 详细信息和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.get(url) as response:
        response.raise_for_status()
        return AlphaDetailView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_yearly_stats(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[TableView, RateLimit]:
    """
    获取指定 alpha 的年度统计数据。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    Tuple[AlphaYearlyStats, RateLimit]: 包含年度统计数据和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHA_YEARLY_STATS(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()
        return TableView.model_validate_json(
            await response.text()
        ), RateLimit.from_headers(response.headers)


async def alpha_fetch_record_set_pnl(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, Optional[TableView], float, RateLimit]:
    """
    获取指定 alpha 的收益数据。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    Tuple[AlphaPnL, RateLimit]: 包含收益数据和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHA_PNL(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()

        retry_after: float = retry_after_from_headers(response.headers)
        if retry_after != 0.0:
            return (
                False,
                None,
                retry_after,
                RateLimit.from_headers(response.headers),
            )

        return (
            True,
            TableView.model_validate_json(await response.text()),
            0.0,
            RateLimit.from_headers(response.headers),
        )


async def alpha_fetch_correlations(
    session: aiohttp.ClientSession, alpha_id: str, corr_type: CorrelationType
) -> Tuple[bool, Optional[float], Optional[TableView], RateLimit]:
    """
    获取指定 alpha 的自相关性数据。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。
    corr_type (CorrelationType): 自相关性类型。

    返回:
    Tuple[bool, Optional[float], Optional[AlphaCorrelations], RateLimit]:
        包含请求完成状态、重试时间、自相关性数据和速率限制信息的元组。
    """
    url: str = (
        f"{BASE_URL}/{ENDPOINT_ALPHA_SELF_CORRELATIONS(alpha_id, corr_type.value)}"
    )
    async with session.get(url) as response:
        response.raise_for_status()
        retry_after: float = retry_after_from_headers(response.headers)

        if retry_after != 0.0:
            return (
                False,
                retry_after,
                None,
                RateLimit.from_headers(response.headers),
            )
        else:
            return (
                True,
                None,
                TableView.model_validate_json(await response.text()),
                RateLimit.from_headers(response.headers),
            )


async def alpha_fetch_before_and_after_performance(
    session: aiohttp.ClientSession, competition_id: Optional[str], alpha_id: str
) -> Tuple[bool, Optional[float], Optional[BeforeAndAfterPerformanceView]]:
    """
    获取指定 alpha 的前后性能数据。
    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。
    返回:
    Tuple[bool, Optional[float], Optional[AlphaCorrelations], RateLimit]:
        包含请求完成状态、重试时间、自相关性数据和速率限制信息的元组。
    """
    url: str = (
        f"{BASE_URL}/{ENDPOINT_BEFORE_AND_AFTER_PERFORMANCE(competition_id, alpha_id)}"
    )
    async with session.get(url) as response:
        response.raise_for_status()
        retry_after: float = retry_after_from_headers(response.headers)

        if retry_after != 0.0:
            return (
                False,
                retry_after,
                None,
            )

        return (
            True,
            None,
            BeforeAndAfterPerformanceView.model_validate_json(await response.text()),
        )


async def set_alpha_properties(
    session: aiohttp.ClientSession, alpha_id: str, properties: AlphaPropertiesPayload
) -> Tuple[AlphaDetailView, RateLimit]:
    """
    设置指定 alpha 的属性。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。
    properties (AlphaPropertiesBody): 要设置的属性数据。

    返回:
    Tuple[AlphaDetailView, RateLimit]: 包含响应数据和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.patch(
        url, json=properties.model_dump(mode="python")
    ) as response:
        response.raise_for_status()
        return AlphaDetailView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def alpha_fetch_submission_check_result(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, float, Optional[SubmissionCheckResultView], RateLimit]:
    """
    检查指定 alpha 的提交状态。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    Tuple[bool, float, Optional[AlphaCheckResult], RateLimit]:
        包含请求完成状态、重试时间、检查结果和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}/check"

    async with session.get(url) as response:
        response.raise_for_status()
        retry_after: float = retry_after_from_headers(response.headers)

        if retry_after != 0.0:
            return (
                False,
                float(retry_after),
                None,
                RateLimit.from_headers(response.headers),
            )
        else:
            return (
                True,
                0.0,
                SubmissionCheckResultView.model_validate_json(await response.text()),
                RateLimit.from_headers(response.headers),
            )


async def alpha_fetch_competitions(
    session: aiohttp.ClientSession, params: Optional[Dict[str, Any]] = None
) -> CompetitionListView:
    """
    获取当前用户的 alpha 竞赛列表。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    params (Optional[Dict[str, Any]]): 查询参数。

    返回:
    CompetitionListView: 包含 alpha 竞赛列表的对象。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_COMPETITIONS}"
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return CompetitionListView.model_validate_json(await response.text())


async def fetch_dataset_data_fields(
    session: ClientSession, params: Dict[str, Any]
) -> DataFieldListView:
    """
    从 API 获取数据集字段。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        params (Dict[str, Any]): 请求的查询参数。

    返回:
        DatasetDataFields: 解析后的数据集字段。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_FIELDS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DataFieldListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_data_field_detail(
    session: ClientSession, field_id: str
) -> DatasetDataFieldsView:
    """
    获取特定数据字段的详细信息。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        field_id (str): 数据字段的 ID。

    返回:
        DataFieldDetail: 解析后的数据字段详细信息。
    """
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_FIELDS}/{field_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDataFieldsView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_dataset_detail(
    session: ClientSession, dataset_id: str
) -> DatasetDetailView:
    """
    获取特定数据集的详细信息。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        dataset_id (str): 数据集的 ID。

    返回:
        DatasetDetail: 解析后的数据集详细信息。
    """
    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_SETS}/{dataset_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDetailView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_datasets(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> DatasetListView:
    """
    从 API 获取数据集列表。

    参数:
        session (ClientSession): aiohttp 客户端会话。
        params (Optional[Dict[str, Any]]): 请求的查询参数。

    返回:
        DataSets: 解析后的数据集列表。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_SETS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DatasetListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_data_categories(session: ClientSession) -> DataCategoriesListView:
    """
    从 API 获取数据类别。

    参数:
        session (ClientSession): aiohttp 客户端会话。

    返回:
        List[DataCategoriesParent]: 解析后的数据类别列表。
    """
    url = urljoin(BASE_URL, ENDPOINT_DATA_CATEGORIES)
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataCategoriesListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def get_all_operators(session: ClientSession) -> Operators:
    """
    从 AlphaPower API 获取所有运营商。

    参数:
        session (ClientSession): aiohttp.ClientSession 的实例，用于发起 HTTP 请求。

    返回:
        Operators: 一个 Operators 模型实例，包含从 API 响应中获取的数据。

    异常:
        aiohttp.ClientResponseError: 如果 HTTP 请求失败或返回错误状态。
    """
    url = f"{BASE_URL}/{ENDPOINT_OPERATORS}"
    async with session.get(url) as response:
        response.raise_for_status()
        return Operators.model_validate_json(await response.text())


async def _create_simulation(
    session: aiohttp.ClientSession, simulation_data: Union[dict[str, Any], List[Any]]
) -> tuple[bool, str, float]:
    """
    创建模拟的通用函数。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        simulation_data (Union[dict[str, Any], List[Any]]): 模拟所需的数据。

    返回:
        tuple: 包含以下内容的元组:
            - success (bool): 是否成功创建模拟。
            - progress_id (str): 模拟进度的唯一标识符。
            - retry_after (float): 重试前等待的秒数。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_SIMULATION}"

    async with session.post(url, json=simulation_data) as response:
        response.raise_for_status()
        if response.status == 201:
            progress_id: str = response.headers["Location"].split("/")[-1]
            retry_after: float = float(response.headers["Retry-After"])
            return True, progress_id, retry_after
        return DEFAULT_SIMULATION_RESPONSE


async def create_single_simulation(
    session: aiohttp.ClientSession, simulation_data: dict[str, Any]
) -> tuple[bool, str, float]:
    """
    创建单次模拟。
    """
    return await _create_simulation(session, simulation_data)


async def create_multi_simulation(
    session: aiohttp.ClientSession, simulation_data: List[Any]
) -> tuple[bool, str, float]:
    """
    创建多次模拟。
    """
    return await _create_simulation(session, simulation_data)


async def delete_simulation(session: aiohttp.ClientSession, progress_id: str) -> None:
    """
    删除指定的模拟。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        progress_id (str): 要删除的模拟的唯一标识符。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_SIMULATION}/{progress_id}"
    async with session.delete(url) as response:
        response.raise_for_status()


async def get_simulation_progress(
    session: aiohttp.ClientSession, progress_id: str, is_multi: bool
) -> tuple[
    bool,
    Union[
        SingleSimulationResultView, MultiSimulationResultView, SimulationProgressView
    ],
    float,
]:
    """
    获取模拟的进度或结果。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        progress_id (str): 模拟进度的唯一标识符。
        is_multi (bool): 是否为多次模拟。

    返回:
        tuple: 包含以下内容的元组:
            - finished (bool): 模拟是否完成。
            - progress_or_result (Union[SingleSimulationResult,
                                    MultiSimulationResult, SimulationProgress]):
              模拟的进度或结果。
            - retry_after (float): 如果模拟仍在运行，则为重试前等待的秒数。
    """
    progress_url: str = f"{BASE_URL}/{ENDPOINT_SIMULATION}/{progress_id}"
    async with session.get(progress_url) as response:
        response.raise_for_status()

        finished: bool = False
        if response.headers.get("Retry-After") is not None:
            # 模拟中，返回模拟进度
            retry_after: float = float(response.headers["Retry-After"])
            finished = False
            return (
                finished,
                SimulationProgressView.model_validate_json(await response.text()),
                retry_after,
            )
        else:
            # 模拟完成，返回模拟结果
            finished = True
            result: Union[SingleSimulationResultView, MultiSimulationResultView]
            if is_multi:
                result = MultiSimulationResultView.model_validate_json(
                    await response.text()
                )
            else:
                result = SingleSimulationResultView.model_validate(
                    await response.json()
                )
            return (
                finished,
                result,
                0.0,
            )


async def get_self_simulation_activities(
    session: aiohttp.ClientSession, date: str
) -> SelfSimulationActivitiesView:
    """
    获取用户的模拟活动。

    参数:
        session (aiohttp.ClientSession): 用于发送HTTP请求的会话对象。
        date (str): 查询活动的日期。

    返回:
        SelfSimulationActivities: 用户的模拟活动数据。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ACTIVITIES_SIMULATION}"
    async with session.get(url, params={"date": date}) as response:
        response.raise_for_status()
        return SelfSimulationActivitiesView.model_validate_json(await response.text())


async def authentication(session: ClientSession) -> AuthenticationView:
    """
    进行用户认证。

    参数:
    session (ClientSession): 用于发送HTTP请求的会话对象。

    返回:
    Authentication: 认证响应对象。
    """
    url = f"{BASE_URL}/{ENDPOINT_AUTHENTICATION}"
    response = await session.post(url)
    response.raise_for_status()
    return AuthenticationView.model_validate_json(await response.text())
