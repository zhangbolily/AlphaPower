from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession
from multidict import CIMultiDictProxy
from structlog.stdlib import BoundLogger

from alphapower.constants import (
    BASE_URL,
    ENDPOINT_ACTIVITIES_DIVERSITY,
    ENDPOINT_ACTIVITIES_PYRAMID_ALPHAS,
    ENDPOINT_ACTIVITIES_SIMULATIONS,
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
    ENDPOINT_RECORD_SETS,
    ENDPOINT_SIMULATION,
    ENDPOINT_USER_SELF_ALPHAS,
    CorrelationType,
    RecordSetType,
)
from alphapower.internal.logging import get_logger
from alphapower.view.activities import DiversityView, PyramidAlphasView
from alphapower.view.alpha import (
    AlphaDetailView,
    SelfAlphaListView,
)
from alphapower.view.simulation import SingleSimulationResultView

from .checks_view import BeforeAndAfterPerformanceView, SubmissionCheckResultView
from .common_view import TableView
from .models import (
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
    SelfSimulationActivitiesView,
    SimulationProgressView,
)

log: BoundLogger = get_logger(module_name=__name__)

DEFAULT_SIMULATION_RESPONSE: Tuple[bool, str, float] = (False, "", 0.0)


def retry_after_from_headers(headers: CIMultiDictProxy[str]) -> float:

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

    url: str = f"{BASE_URL}/{ENDPOINT_USER_SELF_ALPHAS}"
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return SelfAlphaListView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_detail(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[AlphaDetailView, RateLimit]:

    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.get(url) as response:
        response.raise_for_status()
        return AlphaDetailView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_yearly_stats(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[TableView, RateLimit]:

    url: str = f"{BASE_URL}/{ENDPOINT_ALPHA_YEARLY_STATS(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()
        return TableView.model_validate_json(
            await response.text()
        ), RateLimit.from_headers(response.headers)


async def alpha_fetch_record_set_pnl(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, Optional[TableView], float, RateLimit]:

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


async def alpha_fetch_record_sets(
    session: aiohttp.ClientSession,
    alpha_id: str,
    record_type: RecordSetType,
) -> Tuple[bool, Optional[TableView], float, RateLimit]:

    url: str = f"{BASE_URL}/{ENDPOINT_RECORD_SETS(alpha_id, record_type)}"
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

    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.patch(
        url, json=properties.model_dump(mode="json", by_alias=True)
    ) as response:
        response.raise_for_status()
        return AlphaDetailView.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def alpha_fetch_submission_check_result(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, float, Optional[SubmissionCheckResultView], RateLimit]:

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

    url: str = f"{BASE_URL}/{ENDPOINT_COMPETITIONS}"
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return CompetitionListView.model_validate_json(await response.text())


async def fetch_dataset_data_fields(
    session: ClientSession, params: Dict[str, Any]
) -> DataFieldListView:

    url = urljoin(BASE_URL, ENDPOINT_DATA_FIELDS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DataFieldListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_data_field_detail(
    session: ClientSession, field_id: str
) -> DatasetDataFieldsView:

    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_FIELDS}/{field_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDataFieldsView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_dataset_detail(
    session: ClientSession, dataset_id: str
) -> DatasetDetailView:

    url = urljoin(BASE_URL, f"{ENDPOINT_DATA_SETS}/{dataset_id}")
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DatasetDetailView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_datasets(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> DatasetListView:

    url = urljoin(BASE_URL, ENDPOINT_DATA_SETS)
    response = await session.get(url, params=params)  # 修改为 await
    response.raise_for_status()
    return DatasetListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def fetch_data_categories(session: ClientSession) -> DataCategoriesListView:

    url = urljoin(BASE_URL, ENDPOINT_DATA_CATEGORIES)
    response = await session.get(url)  # 修改为 await
    response.raise_for_status()
    return DataCategoriesListView.model_validate(
        await response.json()
    )  # 修改为 await response.json()


async def get_all_operators(session: ClientSession) -> Operators:

    url = f"{BASE_URL}/{ENDPOINT_OPERATORS}"
    async with session.get(url) as response:
        response.raise_for_status()
        return Operators.model_validate_json(await response.text())


async def _create_simulation(
    session: aiohttp.ClientSession, simulation_data: Union[dict[str, Any], List[Any]]
) -> tuple[bool, str, float]:

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

    return await _create_simulation(session, simulation_data)


async def create_multi_simulation(
    session: aiohttp.ClientSession, simulation_data: List[Any]
) -> tuple[bool, str, float]:

    return await _create_simulation(session, simulation_data)


async def delete_simulation(session: aiohttp.ClientSession, progress_id: str) -> None:

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

    url: str = f"{BASE_URL}/{ENDPOINT_ACTIVITIES_SIMULATIONS}"
    async with session.get(url, params={"date": date}) as response:
        response.raise_for_status()
        return SelfSimulationActivitiesView.model_validate_json(await response.text())


async def authentication(session: ClientSession) -> AuthenticationView:

    url = f"{BASE_URL}/{ENDPOINT_AUTHENTICATION}"
    response = await session.post(url)
    response.raise_for_status()
    return AuthenticationView.model_validate_json(await response.text())


async def user_fetch_pyramid_alphas(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> PyramidAlphasView:

    url = urljoin(BASE_URL, ENDPOINT_ACTIVITIES_PYRAMID_ALPHAS)
    response = await session.get(url, params=params)
    response.raise_for_status()
    return PyramidAlphasView.model_validate(await response.json())


async def user_fetch_diversity(
    session: ClientSession, params: Optional[Dict[str, Any]] = None
) -> DiversityView:

    url = urljoin(BASE_URL, ENDPOINT_ACTIVITIES_DIVERSITY)
    response = await session.get(url, params=params)
    response.raise_for_status()
    return DiversityView.model_validate(await response.json())
