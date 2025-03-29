"""
测试 raw_api 模块
"""

from datetime import datetime
from unittest.mock import patch

import pytest
from aiohttp import ClientSession

from alphapower.client.models import (
    AlphaCheckItem,
    AlphaCheckResult,
    AlphaCorrelations,
    AlphaDetail,
    RateLimit,
    SimulationSettings,
)
from alphapower.client.raw_api.alphas import alpha_check_submission, get_alpha_detail


@pytest.mark.asyncio
async def test_alpha_check_submission(
    setup_mock_responses: str,
) -> None:
    """
    测试 Alpha Check 提交的响应
    """
    # Mock BASE_URL
    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        # 设置 mock 响应
        alpha_id = "alpha_id"

        session: ClientSession = ClientSession()

        # 调用实际的函数
        finished, retry_after, result, rate_limit = await alpha_check_submission(
            session, alpha_id
        )

        # 断言响应是否符合预期
        assert finished is True
        assert retry_after == 0.0
        assert isinstance(result, AlphaCheckResult)
        if result.inSample:
            assert isinstance(
                result.inSample,
                AlphaCheckResult.Sample,
            )

            if result.inSample.checks:
                assert isinstance(result.inSample.checks[0], AlphaCheckItem)

            if result.inSample.selfCorrelated:
                assert isinstance(
                    result.inSample.selfCorrelated,
                    AlphaCorrelations,
                )

            if result.inSample.prodCorrelated:
                assert isinstance(
                    result.inSample.prodCorrelated,
                    AlphaCorrelations,
                )

        if result.outSample is not None:
            assert isinstance(
                result.outSample,
                AlphaCheckResult.Sample,
            )

        assert rate_limit is not None


@pytest.mark.asyncio
async def test_alphas_detail(
    setup_mock_responses: str,
) -> None:
    """
    测试 Alphas Detail 的响应
    """
    # Mock BASE_URL
    with patch("alphapower.client.raw_api.alphas.BASE_URL", new=setup_mock_responses):
        # 设置 mock 响应
        alpha_id = "alpha_id"

        session: ClientSession = ClientSession()

        # 调用实际的函数
        result, rate_limit = await get_alpha_detail(session, alpha_id)

        # 断言响应是否符合预期
        assert result is not None
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

        assert rate_limit is not None
        assert isinstance(rate_limit, RateLimit)
