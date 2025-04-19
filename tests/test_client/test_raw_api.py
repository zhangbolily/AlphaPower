"""
测试 raw_api 模块
"""

from datetime import datetime
from typing import Optional
from unittest.mock import patch

import pytest
from aiohttp import ClientSession

from alphapower.client import (
    AlphaCheckItemView,
    AlphaDetailView,
    AlphaPropertiesPayload,
    AlphaView,
    BeforeAndAfterPerformanceView,
    ClassificationView,
    CompetitionListView,
    CompetitionRefView,
    CompetitionView,
    MultiSimulationResultView,
    PyramidView,
    RateLimit,
    RegularView,
    SelfAlphaListView,
    SimulationProgressView,
    SimulationSettingsView,
    SingleSimulationResultView,
    SubmissionCheckResultView,
    TableView,
)
from alphapower.client.raw_api import (
    alpha_fetch_before_and_after_performance,
    alpha_fetch_competitions,
    alpha_fetch_correlations,
    alpha_fetch_record_set_pnl,
    alpha_fetch_submission_check_result,
    get_alpha_detail,
    get_self_alphas,
    get_simulation_progress,
    set_alpha_properties,
)
from alphapower.constants import AlphaType, CompetitionStatus, CorrelationType


def assert_alpha_check_result(result: Optional[SubmissionCheckResultView]) -> None:
    """
    验证 AlphaCheckResult 对象的结构和类型。
    """
    assert isinstance(result, SubmissionCheckResultView)
    if result.in_sample:
        assert isinstance(result.in_sample, SubmissionCheckResultView.Sample)
        if result.in_sample.checks:
            assert all(
                isinstance(check, AlphaCheckItemView)
                for check in result.in_sample.checks
            )
        if result.in_sample.self_correlated:
            assert isinstance(result.in_sample.self_correlated, TableView)
        if result.in_sample.prod_correlated:
            assert isinstance(result.in_sample.prod_correlated, TableView)
    if result.out_sample:
        assert isinstance(result.out_sample, SubmissionCheckResultView.Sample)
        if result.out_sample.checks:
            assert all(
                isinstance(check, AlphaCheckItemView)
                for check in result.out_sample.checks
            )


def assert_alpha_detail(result: Optional[AlphaDetailView]) -> None:
    """
    验证 AlphaDetail 对象的结构和类型。
    """
    assert isinstance(result, AlphaDetailView)
    assert isinstance(result.settings, SimulationSettingsView)
    assert isinstance(result.date_created, datetime)
    if result.date_submitted:
        assert isinstance(result.date_submitted, datetime)
    if result.date_modified:
        assert isinstance(result.date_modified, datetime)
    assert isinstance(result.in_sample, AlphaDetailView.Sample)
    if result.out_sample:
        assert isinstance(result.out_sample, AlphaDetailView.Sample)
    if result.train:
        assert isinstance(result.train, AlphaDetailView.Sample)
    if result.test:
        assert isinstance(result.test, AlphaDetailView.Sample)
    if result.prod:
        assert isinstance(result.prod, AlphaDetailView.Sample)
    if result.competitions:
        assert all(isinstance(comp, CompetitionView) for comp in result.competitions)
    if result.tags:
        assert all(isinstance(tag, str) for tag in result.tags)
    if result.pyramids:
        assert all(isinstance(pyramid, PyramidView) for pyramid in result.pyramids)
    if result.classifications:
        assert all(
            isinstance(classification, ClassificationView)
            for classification in result.classifications
        )


def assert_alpha_list(result: Optional[SelfAlphaListView]) -> None:
    """
    验证 SelfAlphaList 对象的结构和类型。
    """

    def assert_alpha_item(alpha: AlphaView) -> None:
        """
        验证 Alpha 对象的结构和类型。
        """
        assert isinstance(alpha, AlphaView)
        assert isinstance(alpha.id, str)
        assert isinstance(alpha.type, str)
        assert isinstance(alpha.author, str)
        assert isinstance(alpha.settings, SimulationSettingsView)
        assert isinstance(alpha.regular, RegularView)
        if alpha.date_created:
            assert isinstance(alpha.date_created, datetime)
        if alpha.date_submitted:
            assert isinstance(alpha.date_submitted, datetime)
        if alpha.date_modified:
            assert isinstance(alpha.date_modified, datetime)
        if alpha.competitions:
            assert all(
                isinstance(comp, CompetitionRefView) for comp in alpha.competitions
            )
        if alpha.pyramids:
            assert all(isinstance(pyramid, PyramidView) for pyramid in alpha.pyramids)

    assert isinstance(result, SelfAlphaListView)
    assert isinstance(result.count, int)
    if result.results:
        assert all(isinstance(alpha, AlphaView) for alpha in result.results)
    for alpha in result.results:
        assert_alpha_item(alpha)


def assert_single_simulation_result(
    result: Optional[
        SingleSimulationResultView | MultiSimulationResultView | SimulationProgressView
    ],
) -> None:
    """
    验证 SingleSimulationResult 对象的结构和类型。
    """
    assert isinstance(result, SingleSimulationResultView)
    assert isinstance(result.id, str)
    assert isinstance(result.type, AlphaType)
    assert isinstance(result.status, str)
    if result.message:
        assert isinstance(result.message, str)
    if result.location:
        assert isinstance(result.location, SingleSimulationResultView.ErrorLocation)
        if result.location.line:
            assert isinstance(result.location.line, int)
        if result.location.start:
            assert isinstance(result.location.start, int)
        if result.location.end:
            assert isinstance(result.location.end, int)
        if result.location.property:
            assert isinstance(result.location.property, str)
    if result.settings:
        assert isinstance(result.settings, SimulationSettingsView)
    if result.regular:
        assert isinstance(result.regular, str)
    if result.alpha:
        assert isinstance(result.alpha, str)
    if result.parent:
        assert isinstance(result.parent, str)


def assert_multi_simulation_result(
    result: Optional[
        SingleSimulationResultView | MultiSimulationResultView | SimulationProgressView
    ],
) -> None:
    """
    验证 MultiSimulationResult 对象的结构和类型。
    """
    assert isinstance(result, MultiSimulationResultView)
    assert isinstance(result.children, list)
    assert all(isinstance(child, str) for child in result.children)
    assert isinstance(result.status, str)
    assert isinstance(result.type, AlphaType)
    if result.status == "COMPELETE" and result.settings:
        assert isinstance(result.settings, SimulationSettingsView)


@pytest.mark.asyncio
async def test_alpha_check_submission(setup_mock_responses: str) -> None:
    """
    测试 Alpha Check 提交的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        alpha_ids = [
            "regular_alpha_0",
            "regular_alpha_1",
        ]
        session: ClientSession = ClientSession()

        for alpha_id in alpha_ids:
            finished, retry_after, result, rate_limit = (
                await alpha_fetch_submission_check_result(session, alpha_id)
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

    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
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
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        result, rate_limit = await get_self_alphas(session)
        assert_alpha_list(result)
        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)


@pytest.mark.asyncio
async def test_set_alpha_properties(setup_mock_responses: str) -> None:
    """
    测试设置 Alpha 属性的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        # 创建测试用的属性数据
        alpha_props = AlphaPropertiesPayload(name="Test Alpha", tags=["test", "demo"])

        alpha_ids = ["regular_alpha_0", "super_alpha_0"]

        for alpha_id in alpha_ids:
            result, rate_limit = await set_alpha_properties(
                session, alpha_id, alpha_props
            )

            assert_alpha_detail(result)
            assert rate_limit is not None
            assert isinstance(rate_limit, RateLimit)


@pytest.mark.asyncio
async def test_simulation_result(setup_mock_responses: str) -> None:
    """
    测试模拟结果的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
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


@pytest.mark.asyncio
async def test_alpha_competitions(setup_mock_responses: str) -> None:
    """
    测试 Alpha Competitions 的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        result: CompetitionListView = await alpha_fetch_competitions(session)

        assert isinstance(result, CompetitionListView)
        assert isinstance(result.count, int)
        if result.results:
            assert all(isinstance(comp, CompetitionView) for comp in result.results)
        for comp in result.results:
            assert isinstance(comp.id, str)
            assert isinstance(comp.name, str)
            if comp.start_date:
                assert isinstance(comp.start_date, datetime)
            if comp.end_date:
                assert isinstance(comp.end_date, datetime)
            if comp.status:
                assert isinstance(comp.status, CompetitionStatus)
            if comp.description:
                assert isinstance(comp.description, str)


@pytest.mark.asyncio
async def test_alpha_correlations(setup_mock_responses: str) -> None:
    """
    测试 Alpha Correlations 的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        alpha_ids = ["regular_alpha_0"]
        for alpha_id in alpha_ids:
            _, _, result, _ = await alpha_fetch_correlations(
                session, alpha_id, CorrelationType.SELF
            )

            assert isinstance(result, TableView)

            _, _, result, _ = await alpha_fetch_correlations(
                session, alpha_id, CorrelationType.PROD
            )

            assert isinstance(result, TableView)


@pytest.mark.asyncio
async def test_alpha_fetch_before_and_after_performance(
    setup_mock_responses: str,
) -> None:
    """
    测试 Alpha Fetch Before and After Performance 的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        competition_ids = ["competition_0"]
        alpha_ids = ["regular_alpha_0"]
        for alpha_id in alpha_ids:
            for competition_id in competition_ids:
                _, _, result, _ = await alpha_fetch_before_and_after_performance(
                    session, competition_id, alpha_id
                )

                assert isinstance(result, BeforeAndAfterPerformanceView)


@pytest.mark.asyncio
async def test_fetch_alpha_record_set_pnl(
    setup_mock_responses: str,
) -> None:
    """
    测试 Alpha Fetch Record Set PnL 的响应
    """
    with patch("alphapower.client.raw_api.BASE_URL", new=setup_mock_responses):
        session: ClientSession = ClientSession()

        alpha_ids = ["regular_alpha_0"]
        for alpha_id in alpha_ids:
            result, rate_limit = await alpha_fetch_record_set_pnl(session, alpha_id)

            assert isinstance(result, TableView)
            assert rate_limit is not None
            assert isinstance(rate_limit, RateLimit)
