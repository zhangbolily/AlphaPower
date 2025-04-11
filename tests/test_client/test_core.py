"""
测试 WorldQuantClient 的核心功能。
"""

import asyncio
from typing import AsyncGenerator

import pytest

from alphapower.client import (
    MultiSimulationPayload,
    MultiSimulationResultView,
    SimulationProgressView,
    SimulationSettingsView,
    SingleSimulationPayload,
    SingleSimulationResultView,
    WorldQuantClient,
    wq_client,
)


@pytest.fixture(name="client")
async def fixture_client() -> AsyncGenerator[WorldQuantClient, None]:
    """
    创建一个异步客户端会话，用于测试。

    返回:
        异步生成器，生成一个 WorldQuantClient 实例。
    """
    async with wq_client as client_session:
        yield client_session


@pytest.mark.asyncio
async def test_create_single_simulation(client: WorldQuantClient) -> None:
    """
    测试创建单次模拟的功能。

    参数:
        client: WorldQuantClient 实例，用于与服务交互。
    """
    simulation_data: SingleSimulationPayload = SingleSimulationPayload(
        type="REGULAR",
        settings=SimulationSettingsView(
            max_trade="OFF",
            nan_handling="OFF",
            instrument_type="EQUITY",
            delay=1,
            universe="TOP3000",
            truncation=0.08,
            unit_handling="VERIFY",
            test_period="P1Y",
            pasteurization="ON",
            region="USA",
            language="FASTEXPR",
            decay=6,
            neutralization="SUBINDUSTRY",
            visualization=False,
        ),
        regular="rank(-returns)",
    )
    success: bool
    progress_id: str
    retry_after: float
    success, progress_id, retry_after = await client.simulation_create_single(
        simulation_data
    )
    assert success
    assert isinstance(progress_id, str)
    assert isinstance(retry_after, float)

    await asyncio.sleep(retry_after)
    while True:
        finished: bool
        progress_or_result: SingleSimulationResultView | SimulationProgressView
        finished, progress_or_result, retry_after = (
            await client.simulation_get_progress_single(progress_id)
        )
        if finished:
            assert isinstance(progress_or_result, SingleSimulationResultView)
            break
        else:
            assert isinstance(progress_or_result, SimulationProgressView)
        await asyncio.sleep(retry_after)

    assert progress_or_result.id is not None
    assert progress_or_result.type is not None
    assert progress_or_result.status is not None

    if progress_or_result.status == "FAILED":
        assert progress_or_result.message is not None
        assert progress_or_result.location is not None
    elif progress_or_result.status == "COMPLETE":
        assert progress_or_result.settings is not None
        assert progress_or_result.regular is not None
        assert progress_or_result.alpha is not None
    else:
        pytest.fail(f"Invalid status {progress_or_result.status}")


@pytest.mark.asyncio
async def test_create_multi_simulation(client: WorldQuantClient) -> None:
    """
    测试创建多次模拟的功能。

    参数:
        client: WorldQuantClient 实例，用于与服务交互。
    """
    simulation_data: MultiSimulationPayload = MultiSimulationPayload(
        [
            SingleSimulationPayload(
                type="REGULAR",
                settings=SimulationSettingsView(
                    max_trade="OFF",
                    nan_handling="OFF",
                    instrument_type="EQUITY",
                    delay=1,
                    universe="TOP3000",
                    truncation=0.08,
                    unit_handling="VERIFY",
                    test_period="P1Y",
                    pasteurization="ON",
                    region="USA",
                    language="FASTEXPR",
                    decay=6,
                    neutralization="SUBINDUSTRY",
                    visualization=False,
                ),
                regular="rank(returns)",
            ),
            SingleSimulationPayload(
                type="REGULAR",
                settings=SimulationSettingsView(
                    max_trade="OFF",
                    nan_handling="OFF",
                    instrument_type="EQUITY",
                    delay=1,
                    universe="TOP1000",
                    truncation=0.08,
                    unit_handling="VERIFY",
                    test_period="P1Y",
                    pasteurization="ON",
                    region="USA",
                    language="FASTEXPR",
                    decay=4,
                    neutralization="SUBINDUSTRY",
                    visualization=False,
                ),
                regular="rank(-returns)",
            ),
        ],
    )
    success: bool
    progress_id: str
    retry_after: float
    success, progress_id, retry_after = await client.simulation_create_multi(
        simulation_data
    )
    assert success
    assert isinstance(progress_id, str)
    assert isinstance(retry_after, float)

    await asyncio.sleep(retry_after)
    finished: bool
    progress_or_result: (
        MultiSimulationResultView | SimulationProgressView | SingleSimulationResultView
    )
    while True:
        finished, progress_or_result, retry_after = (
            await client.simulation_get_progress_multi(progress_id)
        )
        if finished:
            assert isinstance(progress_or_result, MultiSimulationResultView)
            break
        else:
            assert isinstance(progress_or_result, SimulationProgressView)
        await asyncio.sleep(retry_after)

    assert progress_or_result.status is not None
    assert progress_or_result.type is not None
    assert len(progress_or_result.children) > 0

    if progress_or_result.status == "COMPLETE":
        for child_progress_id in progress_or_result.children:
            finished, progress_or_result = (
                await client.simulation_get_child_result(child_progress_id)
            )
            assert isinstance(progress_or_result, SingleSimulationResultView)
            assert progress_or_result.id is not None
            assert progress_or_result.type is not None
            assert progress_or_result.status is not None
            assert progress_or_result.status == "COMPLETE"
            assert progress_or_result.settings is not None
            assert progress_or_result.regular is not None
            assert progress_or_result.alpha is not None
    elif progress_or_result.status == "FAILED":
        pass


# TODO(Ball Chang): 修复无法通过的测试用例，提升测试覆盖率
