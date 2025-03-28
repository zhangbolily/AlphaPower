import asyncio
import unittest

from alphapower.client.core import create_client
from alphapower.config.settings import get_credentials
from alphapower.internal.http_api.simulation import (
    MultiSimulationRequest,
    MultiSimulationResult,
    SimulationProgress,
    SimulationSettings,
    SingleSimulationRequest,
    SingleSimulationResult,
)


class TestWorldQuantClientSimulations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # 使用真实的登录信息
        credentials = get_credentials(1)
        self.client = create_client(credentials)
        await self.client.__aenter__()

    async def asyncTearDown(self):
        await self.client.__aexit__(None, None, None)

    async def test_create_single_simulation(self):
        # 测试创建单次模拟
        simulation_data = SingleSimulationRequest(
            type="REGULAR",
            settings=SimulationSettings(
                maxTrade="OFF",
                nanHandling="OFF",
                instrumentType="EQUITY",
                delay=1,
                universe="TOP3000",
                truncation=0.08,
                unitHandling="VERIFY",
                testPeriod="P1Y",
                pasteurization="ON",
                region="USA",
                language="FASTEXPR",
                decay=6,
                neutralization="SUBINDUSTRY",
                visualization=False,
            ),
            regular="rank(-returns)",
        )
        success, progress_id, retry_after = await self.client.create_single_simulation(
            simulation_data
        )
        self.assertTrue(success)
        self.assertIsInstance(progress_id, str)
        self.assertIsInstance(retry_after, float)

        await asyncio.sleep(retry_after)
        while True:
            finished, progress_or_result, retry_after = (
                await self.client.get_single_simulation_progress(progress_id)
            )
            if finished:
                self.assertIsInstance(progress_or_result, SingleSimulationResult)
                break
            else:
                self.assertIsInstance(progress_or_result, SimulationProgress)
            await asyncio.sleep(retry_after)

        self.assertIsNotNone(progress_or_result.id)
        self.assertIsNotNone(progress_or_result.type)
        self.assertIsNotNone(progress_or_result.status)

        if progress_or_result.status == "FAILED":
            self.assertIsNotNone(progress_or_result.message)
            self.assertIsNotNone(progress_or_result.location)
        elif progress_or_result.status == "COMPLETE":
            self.assertIsNotNone(progress_or_result.settings)
            self.assertIsNotNone(progress_or_result.regular)
            self.assertIsNotNone(progress_or_result.alpha)
        else:
            self.fail(f"Invalid status {progress_or_result.status}")

    async def test_create_multi_simulation(self):
        # 测试创建多次模拟
        simulation_data = MultiSimulationRequest(
            [
                SingleSimulationRequest(
                    type="REGULAR",
                    settings=SimulationSettings(
                        maxTrade="OFF",
                        nanHandling="OFF",
                        instrumentType="EQUITY",
                        delay=1,
                        universe="TOP3000",
                        truncation=0.08,
                        unitHandling="VERIFY",
                        testPeriod="P1Y",
                        pasteurization="ON",
                        region="USA",
                        language="FASTEXPR",
                        decay=6,
                        neutralization="SUBINDUSTRY",
                        visualization=False,
                    ),
                    regular="rank(returns)",
                ),
                SingleSimulationRequest(
                    type="REGULAR",
                    settings=SimulationSettings(
                        maxTrade="OFF",
                        nanHandling="OFF",
                        instrumentType="EQUITY",
                        delay=1,
                        universe="TOP1000",
                        truncation=0.08,
                        unitHandling="VERIFY",
                        testPeriod="P1Y",
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
        success, progress_id, retry_after = await self.client.create_multi_simulation(
            simulation_data
        )
        self.assertTrue(success)
        self.assertIsInstance(progress_id, str)
        self.assertIsInstance(retry_after, float)

        await asyncio.sleep(retry_after)
        while True:
            finished, progress_or_result, retry_after = (
                await self.client.get_multi_simulation_progress(progress_id)
            )
            if finished:
                self.assertIsInstance(progress_or_result, MultiSimulationResult)
                break
            else:
                self.assertIsInstance(progress_or_result, SimulationProgress)
            await asyncio.sleep(retry_after)

        self.assertIsNotNone(progress_or_result.status)
        self.assertIsNotNone(progress_or_result.type)
        self.assertIsNot(len(progress_or_result.children), 0)

        if progress_or_result.status == "COMPLETE":
            for child_progress_id in progress_or_result.children:
                finished, progress_or_result = (
                    await self.client.get_multi_simulation_result(child_progress_id)
                )
                self.assertIsInstance(progress_or_result, SingleSimulationResult)
                self.assertIsNotNone(progress_or_result.id)
                self.assertIsNotNone(progress_or_result.type)
                self.assertIsNotNone(progress_or_result.status)
                self.assertEqual(progress_or_result.status, "COMPLETE")
                self.assertIsNotNone(progress_or_result.settings)
                self.assertIsNotNone(progress_or_result.regular)
                self.assertIsNotNone(progress_or_result.alpha)
        elif progress_or_result.status == "FAILED":
            pass


if __name__ == "__main__":
    unittest.main()
