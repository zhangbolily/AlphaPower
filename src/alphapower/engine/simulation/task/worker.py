"""
@file   worker.py
@brief  工作者类
"""

import asyncio
from typing import Callable, List, Optional

from alphapower.client import (
    MultiSimulationPayload,
    MultiSimulationResultView,
    SimulationProgressView,
    SimulationSettingsView,
    SingleSimulationPayload,
    SingleSimulationResultView,
    WorldQuantClient,
)
from alphapower.constants import (
    MAX_CONSULTANT_SIMULATION_SLOTS,
    ROLE_CONSULTANT,
    ROLE_USER,
)
from alphapower.entity import SimulationTask
from alphapower.internal.logging import setup_logging

from .scheduler_abc import AbstractScheduler
from .worker_abc import AbstractWorker

logger = setup_logging(__name__)


def get_single_simulation_payload(task: SimulationTask) -> SingleSimulationPayload:
    """
    获取单个模拟任务的负载数据。

    :param task: 模拟任务
    :return: 单个模拟任务的负载数据
    """
    logger.debug(
        f"生成单个模拟任务负载数据，任务 ID: {task.id}, 类型: {task.type}, 设置: {task.settings}"
    )
    payload: SingleSimulationPayload = SingleSimulationPayload(
        type=task.type.value,
        settings=SimulationSettingsView.model_validate(task.settings),
        regular=task.regular,
    )
    logger.debug(f"生成的负载数据: {payload}")
    return payload


class Worker(AbstractWorker):
    """
    工作者类，用于执行任务。
    """

    def __init__(self, client: WorldQuantClient) -> None:
        """
        初始化工作者实例。

        :param client: WorldQuant 客户端实例
        """
        self._client: WorldQuantClient = client
        self._post_handler_lock: asyncio.Lock = asyncio.Lock()
        self._post_handler_tasks: List[asyncio.Task] = []
        self._completed_task_callbacks: List[
            Callable[[SimulationTask, SingleSimulationResultView], None]
        ] = []
        self._scheduler: Optional[AbstractScheduler] = None
        self._shutdown: bool = True
        self._cancel_tasks: bool = False

        if not isinstance(self._client, WorldQuantClient):
            raise ValueError("Client must be an instance of WorldQuantClient.")

        if not self._client.authentication_info:
            raise ValueError("Client must be authenticated with valid credentials.")

        if ROLE_CONSULTANT in self._client.authentication_info.permissions:
            self._user_role = ROLE_CONSULTANT
        elif ROLE_USER in self._client.authentication_info.permissions:
            self._user_role = ROLE_USER
        else:
            raise ValueError("Client must have a valid user role (CONSULTANT or USER).")

    async def _try_cancel_tasks(self, progress_id: str) -> bool:
        """
        尝试取消任务。

        :param progress_id: 进度 ID
        :return: 是否成功取消任务
        """
        logger.debug(
            f"尝试取消任务，进度 ID: {progress_id}, shutdown: {self._shutdown}, cancel_tasks: {self._cancel_tasks}"
        )
        if self._shutdown and self._cancel_tasks:
            await logger.aerror(f"工作者已关闭，尝试取消任务，进度 ID: {progress_id}")
            success = await self._client.delete_simulation(progress_id=progress_id)
            if success:
                await logger.ainfo(f"任务取消成功，进度 ID: {progress_id}")
                return True
            await logger.aerror(f"任务取消失败，进度 ID: {progress_id}")
        return False

    async def _completed_task_post_handler(
        self, task: SimulationTask, result: SingleSimulationResultView
    ) -> None:
        """
        处理完成任务后的逻辑。

        :param task: 模拟任务
        :param result: 单个模拟任务的结果
        """
        # TODO: 这里可以添加完成任务后的处理逻辑
        # 1. 更新任务状态和必要的字段
        # 2. 如果是失败的任务，可能需要记录错误信息并通知其他服务
        logger.info(f"任务 {task.id} 完成，结果: {result}")

    async def _handle_single_simulation_task(self, task: SimulationTask) -> None:
        """
        处理单个模拟任务的方法。

        :param task: 模拟任务
        """
        logger.debug(f"开始处理单个模拟任务，任务 ID: {task.id}")
        if self._shutdown:
            await logger.aerror(f"工作者已关闭，无法处理任务 ID: {task.id}")
            return

        payload = get_single_simulation_payload(task)

        async with self._client:
            try:
                logger.debug(
                    f"发送创建单个模拟任务请求，任务 ID: {task.id}, 请求体: {payload}"
                )
                success, progress_id, retry_after = (
                    await self._client.create_single_simulation(payload=payload)
                )

                if not success:
                    await logger.awarning(
                        f"创建单个模拟任务失败，任务 ID: {task.id}，请求体: {payload}"
                    )
                    return

                await logger.ainfo(
                    f"创建单个模拟任务成功，任务 ID: {task.id}，进度 ID: {progress_id}"
                )

                await asyncio.sleep(retry_after)

                prev_progress: float = 0.0
                while True:
                    logger.debug(
                        f"检查任务进度，任务 ID: {task.id}, 进度 ID: {progress_id}"
                    )
                    if await self._try_cancel_tasks(progress_id):
                        await logger.ainfo(
                            f"任务已取消，任务 ID: {task.id}，进度 ID: {progress_id}"
                        )
                        break

                    finished, progress_or_result, retry_after = (
                        await self._client.get_single_simulation_progress(
                            progress_id=progress_id
                        )
                    )

                    if finished and isinstance(
                        progress_or_result, SingleSimulationResultView
                    ):
                        await logger.ainfo(
                            f"单个模拟任务完成，任务 ID: {task.id}，进度 ID: {progress_id}"
                        )
                        await self._completed_task_post_handler(
                            task, progress_or_result
                        )
                        break

                    if isinstance(progress_or_result, SimulationProgressView):
                        progress: float = progress_or_result.progress
                        if progress != prev_progress:
                            await logger.ainfo(
                                f"单个模拟任务进行中，任务 ID: {task.id}，进度 ID: {progress_id}，进度: {progress * 100:.2f}%"
                            )
                            prev_progress = progress
                    else:
                        await logger.aerror(
                            f"未知返回值组合，任务 ID: {task.id}，进度 ID: {progress_id}，"
                            f"返回值: finished={finished}, progress_or_result={progress_or_result}, retry_after={retry_after}"
                        )
                    await asyncio.sleep(retry_after)
            except Exception as e:
                await logger.aerror(
                    f"处理单个模拟任务失败，任务 ID: {task.id}，错误: {e}"
                )

    async def _multi_completed_task_post_handler(
        self, tasks: List[SimulationTask], result: MultiSimulationResultView
    ) -> None:
        """
        处理完成多个模拟任务后的逻辑。

        :param tasks: 模拟任务列表
        :param result: 多个模拟任务的结果
        """

        async def handle_completed_task(task: SimulationTask, child_id: str) -> None:
            """
            处理多个模拟任务的子任务。

            :param task: 模拟任务
            :param child_id: 子任务 ID
            """
            async with self._client:
                try:
                    success, result = (
                        await self._client.get_multi_simulation_child_result(
                            child_progress_id=child_id,
                        )
                    )
                    if not success:
                        await logger.aerror(
                            f"获取多个模拟任务的子任务结果失败，任务实体 ID {task.id} 子任务 ID {child_id}"
                        )
                        return
                    await logger.ainfo(
                        f"获取多个模拟任务的子任务结果成功，任务实体 ID {task.id} 子任务 ID {child_id}"
                    )
                    await self._completed_task_post_handler(task, result)
                except Exception as e:
                    # 捕获所有异常并记录错误，不向上继续抛出
                    await logger.aerror(
                        f"处理多个模拟任务的子任务失败，任务实体 ID {task.id} 子任务 ID {child_id} 错误: {e}"
                    )

        async with self._post_handler_lock:
            self._post_handler_tasks.extend(
                [
                    asyncio.create_task(handle_completed_task(task, child_id))
                    for task, child_id in zip(tasks, result.children)
                ]
            )

    async def _handle_multi_simulation_task(self, tasks: List[SimulationTask]) -> None:
        """
        处理多个模拟任务的方法。

        :param tasks: 模拟任务列表
        """
        logger.debug(
            f"开始处理多个模拟任务，任务 ID 列表: {[task.id for task in tasks]}"
        )
        if self._user_role != ROLE_CONSULTANT:
            await logger.aerror("当前用户角色不是顾问，无法处理多个模拟任务")
            return

        if self._shutdown:
            await logger.aerror("工作者已关闭，无法处理多个模拟任务")
            return

        if len(tasks) > MAX_CONSULTANT_SIMULATION_SLOTS:
            await logger.aerror(
                f"任务数量超出限制，当前任务数量: {len(tasks)}，最大数量: {MAX_CONSULTANT_SIMULATION_SLOTS}"
            )
            return

        single_simu_payloads: List[SingleSimulationPayload] = [
            get_single_simulation_payload(task) for task in tasks
        ]
        payload: MultiSimulationPayload = MultiSimulationPayload(
            root=single_simu_payloads
        )
        task_ids: List[int] = [task.id for task in tasks]
        task_ids_str: str = ", ".join(str(task_id) for task_id in task_ids)

        async with self._client:
            try:
                logger.debug(
                    f"发送创建多个模拟任务请求，任务 ID 列表: {task_ids_str}, 请求体: {payload}"
                )
                success, progress_id, retry_after = (
                    await self._client.create_multi_simulation(payload=payload)
                )

                if not success:
                    await logger.aerror(
                        f"创建多个模拟任务失败，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}"
                    )
                    return

                await logger.ainfo(
                    f"创建多个模拟任务成功，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}"
                )

                await asyncio.sleep(retry_after)

                prev_progress: float = 0.0
                while True:
                    logger.debug(
                        f"检查多个任务进度，任务 ID 列表: {task_ids_str}, 进度 ID: {progress_id}"
                    )
                    if await self._try_cancel_tasks(progress_id):
                        await logger.ainfo(
                            f"任务已取消，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}"
                        )
                        break

                    finished, progress_or_result, retry_after = (
                        await self._client.get_multi_simulation_progress(
                            progress_id=progress_id
                        )
                    )

                    if finished and isinstance(
                        progress_or_result, MultiSimulationResultView
                    ):
                        await logger.ainfo(
                            f"多个模拟任务完成，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}"
                        )
                        break

                    if isinstance(progress_or_result, SimulationProgressView):
                        progress: float = progress_or_result.progress
                        if progress != prev_progress:
                            await logger.ainfo(
                                f"多个模拟任务进行中，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}，进度: {progress * 100:.2f}%"
                            )
                            prev_progress = progress
                    else:
                        await logger.aerror(
                            f"未知返回值组合，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}，"
                            f"返回值: finished={finished}, progress_or_result={progress_or_result}, retry_after={retry_after}"
                        )
                    await asyncio.sleep(retry_after)
            except Exception as e:
                await logger.aerror(
                    f"处理多个模拟任务失败，任务 ID 列表: {task_ids_str}，错误: {e}"
                )

    async def _do_work(self) -> None:
        """
        执行工作的方法。
        """
        logger.debug("开始执行工作循环")
        while not self._shutdown:
            if self._scheduler is None:
                await logger.aerror("调度器未设置，无法执行工作")
                raise Exception("调度器未设置，无法执行工作")

            schedule_task_count: int = (
                MAX_CONSULTANT_SIMULATION_SLOTS
                if self._user_role == ROLE_CONSULTANT
                else 1
            )
            tasks: List[SimulationTask] = await self._scheduler.schedule(
                batch_size=schedule_task_count
            )
            if not tasks:
                logger.debug("调度器未返回任务，等待 5 秒后重试")
                await logger.ainfo("没有可执行的任务")
                await asyncio.sleep(5)
                continue

            if self._user_role == ROLE_USER:
                await self._handle_single_simulation_task(tasks[0])
            elif self._user_role == ROLE_CONSULTANT:
                await self._handle_multi_simulation_task(tasks)
            else:
                await logger.aerror(f"未知用户角色 {self._user_role}，无法处理任务")
                return

    async def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """
        设置调度器。

        :param scheduler: 调度器实例
        """
        logger.debug(
            f"设置调度器，当前调度器: {self._scheduler}, 新调度器: {scheduler}"
        )
        await logger.adebug(f"设置调度器 {scheduler}，当前调度器 {self._scheduler}")
        self._scheduler = scheduler

    async def run(self) -> None:
        """
        运行工作者，执行任务。
        """
        logger.debug("启动工作者")
        self._shutdown = False
        await self._do_work()

    async def stop(self, cancel_tasks: bool = False) -> None:
        """
        停止工作者，清理资源。

        :param cancel_tasks: 是否取消任务
        """
        logger.debug(f"停止工作者，cancel_tasks: {cancel_tasks}")
        self._shutdown = True
        self._cancel_tasks = cancel_tasks
        await asyncio.gather(*self._post_handler_tasks)
        await logger.ainfo("工作者已停止")

    async def add_task_complete_callback(
        self, callback: Callable[[SimulationTask, SingleSimulationResultView], None]
    ) -> None:
        """
        添加任务完成回调函数。

        :param callback: 任务完成时的回调函数
        """
        logger.debug(f"添加任务完成回调函数: {callback}")
        if not self._shutdown:
            await logger.aerror("工作者未关闭，无法添加任务完成回调函数")
        self._completed_task_callbacks.append(callback)
        await logger.adebug(
            f"添加任务完成回调函数 {callback}， 当前回调函数数量 {len(self._completed_task_callbacks)}"
        )
