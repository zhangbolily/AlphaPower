"""
alphapower.client.raw_api.alphas
========================
AlphaPower Alphas API
========================
"""

from typing import Any, Dict, Optional, Tuple

import aiohttp

from alphapower.client.models import (
    AlphaCheckResult,
    AlphaCorrelations,
    AlphaDetail,
    AlphaPnL,
    AlphaPropertiesBody,
    AlphaYearlyStats,
    RateLimit,
    SelfAlphaList,
)

from .common import (
    BASE_URL,
    ENDPOINT_ALPHA_PNL,
    ENDPOINT_ALPHA_SELF_CORRELATIONS,
    ENDPOINT_ALPHA_YEARLY_STATS,
    ENDPOINT_ALPHAS,
    ENDPOINT_SELF_ALPHA_LIST,
    retry_after_from_headers,
)


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
) -> Tuple[SelfAlphaList, RateLimit]:
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
        return SelfAlphaList.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_detail(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[AlphaDetail, RateLimit]:
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
        return AlphaDetail.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_yearly_stats(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[AlphaYearlyStats, RateLimit]:
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
        return AlphaYearlyStats.model_validate(
            await response.json()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_pnl(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[AlphaPnL, RateLimit]:
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
        return AlphaPnL.model_validate(await response.json()), RateLimit.from_headers(
            response.headers
        )


async def get_alpha_self_correlations(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, Optional[float], Optional[AlphaCorrelations], RateLimit]:
    """
    获取指定 alpha 的自相关性数据。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    Tuple[bool, Optional[float], Optional[AlphaCorrelations], RateLimit]:
        包含请求完成状态、重试时间、自相关性数据和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHA_SELF_CORRELATIONS(alpha_id)}"
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
                AlphaCorrelations.model_validate(await response.json()),
                RateLimit.from_headers(response.headers),
            )


async def set_alpha_properties(
    session: aiohttp.ClientSession, alpha_id: str, properties: AlphaPropertiesBody
) -> Tuple[Dict[str, Any], RateLimit]:
    """
    设置指定 alpha 的属性。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。
    properties (AlphaPropertiesBody): 要设置的属性数据。

    返回:
    Tuple[Dict[str, Any], RateLimit]: 包含响应数据和速率限制信息的元组。
    """
    url: str = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.patch(url, json=properties) as response:
        response.raise_for_status()
        return await response.json(), RateLimit.from_headers(response.headers)


async def alpha_check_submission(
    session: aiohttp.ClientSession, alpha_id: str
) -> Tuple[bool, float, Optional[AlphaCheckResult], RateLimit]:
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
                AlphaCheckResult.model_validate(await response.json()),
                RateLimit.from_headers(response.headers),
            )
