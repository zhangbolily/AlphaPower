import aiohttp
from .common import *
from alphapower.client.models import *


def quote_alpha_list_query_params(params):
    if not params:
        return "", {}

    quoted_params = ""
    rem_params = {}

    switch = {
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


async def get_self_alphas(session: aiohttp.ClientSession, params=None):
    url = f"{BASE_URL}/{ENDPOINT_SELF_ALPHA_LIST}"
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        return SelfAlphaList.from_json(await response.text()), RateLimit.from_headers(
            response.headers
        )


async def get_alpha_detail(session: aiohttp.ClientSession, alpha_id):
    url = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.get(url) as response:
        response.raise_for_status()
        return AlphaDetail.from_json(await response.text()), RateLimit.from_headers(
            response.headers
        )


async def get_alpha_yearly_stats(session: aiohttp.ClientSession, alpha_id):
    url = f"{BASE_URL}/{ENDPOINT_ALPHA_YEARLY_STATS(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()
        return AlphaYearlyStats.from_json(
            await response.text()
        ), RateLimit.from_headers(response.headers)


async def get_alpha_pnl(session: aiohttp.ClientSession, alpha_id):
    url = f"{BASE_URL}/{ENDPOINT_ALPHA_PNL(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()
        return AlphaPnL.from_json(await response.text()), RateLimit.from_headers(
            response.headers
        )


async def get_alpha_self_correlations(session: aiohttp.ClientSession, alpha_id):
    """
    获取指定 alpha 的自相关性数据。

    参数:
    session (aiohttp.ClientSession): 用于发送 HTTP 请求的会话对象。
    alpha_id (str): alpha 的唯一标识符。

    返回:
    tuple: 包含以下元素的元组:
        - finished (bool): 请求是否已完成。
        - retry_after (str 或 None): 如果请求被速率限制，返回重试时间，否则为 None。
        - AlphaSelfCorrelations 或 None: 如果请求成功，返回 AlphaSelfCorrelations 对象，否则为 None。
        - RateLimit: 包含速率限制信息的对象。
    """
    url = f"{BASE_URL}/{ENDPOINT_ALPHA_SELF_CORRELATIONS(alpha_id)}"
    async with session.get(url) as response:
        response.raise_for_status()
        retry_after = response.headers.get("Retry-After")

        if retry_after is not None:
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
                AlphaSelfCorrelations.from_json(await response.text()),
                RateLimit.from_headers(response.headers),
            )


async def set_alpha_properties(
    session: aiohttp.ClientSession, alpha_id, properties: AlphaPropertiesBody
):
    url = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}"
    async with session.patch(url, json=properties) as response:
        response.raise_for_status()
        return await response.json(), RateLimit.from_headers(response.headers)


async def alpha_check_submission(session: aiohttp.ClientSession, alpha_id):
    url = f"{BASE_URL}/{ENDPOINT_ALPHAS}/{alpha_id}/check"
    async with session.get(url) as response:
        response.raise_for_status()
        retry_after = response.headers.get("Retry-After")

        if retry_after is not None:
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
                AplhaCheckResult.from_json(await response.text()),
                RateLimit.from_headers(response.headers),
            )
