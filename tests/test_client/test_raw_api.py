"""
测试 raw_api 模块
"""

from datetime import datetime
from typing import Optional
from unittest.mock import patch

import pytest
from aiohttp import ClientSession

from alphapower.client.models import (
    Alpha,
    AlphaCheckItem,
    AlphaCheckResult,
    AlphaCorrelations,
    AlphaDetail,
    Classification,
    Competition,
    Pyramid,
    RateLimit,
    Regular,
    SelfAlphaList,
    SimulationSettings,
)
from alphapower.client.raw_api.alphas import (
    alpha_check_submission,
    get_alpha_detail,
    get_self_alphas,
)


def assert_alpha_check_result(result: Optional[AlphaCheckResult]) -> None:
    """
    验证 AlphaCheckResult 对象的结构和类型。
    """
    assert isinstance(result, AlphaCheckResult)
    if result.in_sample:
        assert isinstance(result.in_sample, AlphaCheckResult.Sample)
        if result.in_sample.checks:
            assert all(
                isinstance(check, AlphaCheckItem) for check in result.in_sample.checks
            )
        if result.in_sample.self_correlated:
            assert isinstance(result.in_sample.self_correlated, AlphaCorrelations)
        if result.in_sample.prod_correlated:
            assert isinstance(result.in_sample.prod_correlated, AlphaCorrelations)
    if result.out_sample:
        assert isinstance(result.out_sample, AlphaCheckResult.Sample)
        if result.out_sample.checks:
            assert all(
                isinstance(check, AlphaCheckItem) for check in result.out_sample.checks
            )


def assert_alpha_detail(result: Optional[AlphaDetail]) -> None:
    """
    验证 AlphaDetail 对象的结构和类型。
    """
    assert isinstance(result, AlphaDetail)
    assert isinstance(result.settings, SimulationSettings)
    assert isinstance(result.date_created, datetime)
    if result.date_submitted:
        assert isinstance(result.date_submitted, datetime)
    if result.date_modified:
        assert isinstance(result.date_modified, datetime)
    assert isinstance(result.in_sample, AlphaDetail.Sample)
    if result.out_sample:
        assert isinstance(result.out_sample, AlphaDetail.Sample)
    if result.train:
        assert isinstance(result.train, AlphaDetail.Sample)
    if result.test:
        assert isinstance(result.test, AlphaDetail.Sample)
    if result.prod:
        assert isinstance(result.prod, AlphaDetail.Sample)
    if result.competitions:
        assert all(isinstance(comp, Competition) for comp in result.competitions)
    if result.tags:
        assert all(isinstance(tag, str) for tag in result.tags)
    if result.pyramids:
        assert all(isinstance(pyramid, Pyramid) for pyramid in result.pyramids)
    if result.classifications:
        assert all(
            isinstance(classification, Classification)
            for classification in result.classifications
        )


def assert_alpha_list(result: Optional[SelfAlphaList]) -> None:
    """
    验证 SelfAlphaList 对象的结构和类型。
    """

    def assert_alpha_item(alpha: Alpha) -> None:
        """
        验证 Alpha 对象的结构和类型。
        """
        assert isinstance(alpha, Alpha)
        assert isinstance(alpha.id, str)
        assert isinstance(alpha.type, str)
        assert isinstance(alpha.author, str)
        assert isinstance(alpha.settings, SimulationSettings)
        assert isinstance(alpha.regular, Regular)
        if alpha.date_created:
            assert isinstance(alpha.date_created, datetime)
        if alpha.date_submitted:
            assert isinstance(alpha.date_submitted, datetime)
        if alpha.date_modified:
            assert isinstance(alpha.date_modified, datetime)
        if alpha.competitions:
            assert all(isinstance(comp, Competition) for comp in alpha.competitions)
        if alpha.pyramids:
            assert all(isinstance(pyramid, Pyramid) for pyramid in alpha.pyramids)

    assert isinstance(result, SelfAlphaList)
    assert isinstance(result.count, int)
    if result.results:
        assert all(isinstance(alpha, Alpha) for alpha in result.results)
    for alpha in result.results:
        assert_alpha_item(alpha)


@pytest.mark.asyncio
async def test_alpha_check_submission(setup_mock_responses: str) -> None:
    """
    测试 Alpha Check 提交的响应
    """
    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        alpha_id = "alpha_id"
        session: ClientSession = ClientSession()

        finished, retry_after, result, rate_limit = await alpha_check_submission(
            session, alpha_id
        )

        assert finished is True
        assert retry_after == 0.0
        assert_alpha_check_result(result)
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)


@pytest.mark.asyncio
async def test_alphas_detail(setup_mock_responses: str) -> None:
    """
    测试 Alphas Detail 的响应
    """
    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        alpha_id = "alpha_id"
        session: ClientSession = ClientSession()

        result, rate_limit = await get_alpha_detail(session, alpha_id)

        assert_alpha_detail(result)
        assert rate_limit is not None


@pytest.mark.asyncio
async def test_self_alpha_list(setup_mock_responses: str) -> None:
    """
    测试 Self Alpha List 的响应
    """
    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        result, rate_limit = await get_self_alphas(session)
        assert_alpha_list(result)
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)
