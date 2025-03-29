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
    MultiSimulationResult,
    Pyramid,
    RateLimit,
    Regular,
    SelfAlphaList,
    SimulationProgress,
    SimulationSettings,
    SingleSimulationResult,
)
from alphapower.client.raw_api.alphas import (
    alpha_check_submission,
    get_alpha_detail,
    get_self_alphas,
)
from alphapower.client.raw_api.simulation import get_simulation_progress


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


def assert_single_simulation_result(
    result: Optional[
        SingleSimulationResult | MultiSimulationResult | SimulationProgress
    ],
) -> None:
    """
    验证 SingleSimulationResult 对象的结构和类型。
    """
    assert isinstance(result, SingleSimulationResult)
    assert isinstance(result.id, str)
    assert isinstance(result.type, str)
    assert isinstance(result.status, str)
    if result.message:
        assert isinstance(result.message, str)
    if result.location:
        assert isinstance(result.location, SingleSimulationResult.ErrorLocation)
        if result.location.line:
            assert isinstance(result.location.line, int)
        if result.location.start:
            assert isinstance(result.location.start, int)
        if result.location.end:
            assert isinstance(result.location.end, int)
        if result.location.property:
            assert isinstance(result.location.property, str)
    if result.settings:
        assert isinstance(result.settings, SimulationSettings)
    if result.regular:
        assert isinstance(result.regular, str)
    if result.alpha:
        assert isinstance(result.alpha, str)
    if result.parent:
        assert isinstance(result.parent, str)


def assert_multi_simulation_result(
    result: Optional[
        SingleSimulationResult | MultiSimulationResult | SimulationProgress
    ],
) -> None:
    """
    验证 MultiSimulationResult 对象的结构和类型。
    """
    assert isinstance(result, MultiSimulationResult)
    assert isinstance(result.children, list)
    assert all(isinstance(child, str) for child in result.children)
    assert isinstance(result.status, str)
    assert isinstance(result.type, str)
    if result.status == "COMPELETE" and result.settings:
        assert isinstance(result.settings, SimulationSettings)


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
    session: ClientSession = ClientSession()

    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        alpha_ids = ["regular_alpha_0", "regular_alpha_1", "super_alpha_0"]
        for alpha_id in alpha_ids:
            # 使用不同的 alpha_id 测试
            result, rate_limit = await get_alpha_detail(session, alpha_id)

            assert_alpha_detail(result)
            assert rate_limit is not None
            assert isinstance(rate_limit, RateLimit)


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


@pytest.mark.asyncio
async def test_simulation_result(setup_mock_responses: str) -> None:
    """
    测试模拟结果的响应
    """
    with patch(
        "alphapower.client.raw_api.simulation.BASE_URL", new=setup_mock_responses
    ):
        session: ClientSession = ClientSession()

        single_progress_ids = [
            "single_0",
            "single_1",
            "single_failed_0",
            "single_failed_1",
            "super_failed_0",
        ]
        multi_progress_ids = [
            "multi_failed_0",
        ]
        super_progress_ids = [
            "super_0",
            "super_failed_0",
        ]

        for progress_id in single_progress_ids:
            # 使用不同的 progress_id 测试
            finished, result, retry_after = await get_simulation_progress(
                session, progress_id, is_multi=False
            )
            assert finished is True
            assert retry_after == 0.0
            assert_single_simulation_result(result)

        for progress_id in multi_progress_ids:
            # 使用不同的 progress_id 测试
            finished, result, retry_after = await get_simulation_progress(
                session, progress_id, is_multi=True
            )
            assert finished is True
            assert retry_after == 0.0
            assert_multi_simulation_result(result)

        for progress_id in super_progress_ids:
            # 使用不同的 progress_id 测试
            finished, result, retry_after = await get_simulation_progress(
                session, progress_id, is_multi=False
            )
            assert finished is True
            assert retry_after == 0.0
            assert_single_simulation_result(result)
