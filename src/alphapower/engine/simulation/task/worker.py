"""工作者模块，处理模拟任务的执行和监控。

该模块包含执行模拟任务的工作者类，负责与WorldQuantClient通信，
发送模拟请求、监控任务进度并处理结果。支持单个和批量模拟任务处理。

Typical usage example:
  worker = Worker(client)
  await worker.set_scheduler(scheduler)
  await worker.run()
"""

import asyncio
from datetime import datetime
from typing import Awaitable, Callable, List, Optional, Union

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
    DB_SIMULATION,
    MAX_CONSULTANT_SIMULATION_SLOTS,
    ROLE_CONSULTANT,
    ROLE_USER,
    AlphaType,
)
from alphapower.entity import SimulationTask, SimulationTaskStatus
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import setup_logging

from .scheduler_abc import AbstractScheduler
from .worker_abc import AbstractWorker

logger = setup_logging(__name__)


def build_single_simulation_payload(task: SimulationTask) -> SingleSimulationPayload:
    """获取单个模拟任务的负载数据。

    将任务转换为适用于单个模拟的负载数据格式。

    Args:
        task: 模拟任务对象，包含任务的类型、设置和其他属性

    Returns:
        SingleSimulationPayload: 格式化后的单个模拟任务负载数据
    """
    # 详细记录构建负载数据过程，便于调试
    setting = SimulationSettingsView.model_construct(
        region=task.region.value,
        delay=task.delay.value,
        language=task.language.value,
        instrument_type=task.instrument_type.value,
        universe=task.universe.value,
        neutralization=task.neutralization.value,
        pasteurization=task.pasteurization.value,
        unit_handling=task.unit_handling.value,
        max_trade=task.max_trade.value,
        decay=task.decay,
        truncation=task.truncation,
        visualization=task.visualization,
        test_period=task.test_period,
    )

    logger.debug(
        f"生成单个模拟任务负载数据，任务 ID: {task.id}, 类型: {task.type}, 设置: {setting}"
    )
    payload: SingleSimulationPayload = SingleSimulationPayload(
        type=task.type.value,
        settings=setting,
        regular=task.regular,
    )
    logger.debug(f"生成的负载数据: {payload}")
    return payload


class Worker(AbstractWorker):
    """工作者类，用于执行模拟任务。

    负责处理模拟任务的执行、进度跟踪和结果处理。工作者使用WorldQuantClient
    与后端服务通信，并通过回调函数通知任务完成情况。

    Attributes:
        _client: WorldQuant 客户端实例
        _post_handler_lock: 用于同步处理回调的锁
        _post_handler_futures: 处理程序的异步任务列表
        _task_complete_callbacks: 任务完成时的回调函数列表
        _scheduler: 任务调度器实例
        _shutdown_flag: 工作者是否已关闭的标志
        _is_task_cancel_requested: 是否请求取消任务的标志
        _user_role: 用户角色，决定了工作者可以执行的任务类型
    """

    def __init__(self, client: WorldQuantClient, dry_run: bool = False) -> None:
        """初始化工作者实例。

        Args:
            client: WorldQuant 客户端实例，用于与服务端通信

        Raises:
            ValueError: 当客户端不是WorldQuantClient实例、未授权或没有有效角色时
        """
        self._client: WorldQuantClient = client
        self._post_handler_lock: asyncio.Lock = asyncio.Lock()
        self._post_handler_futures: List[asyncio.Task] = []
        self._task_complete_callbacks: List[
            Union[
                Callable[[SimulationTask, SingleSimulationResultView], None],
                Callable[[SimulationTask, SingleSimulationResultView], Awaitable[None]],
            ]
        ] = []
        self._scheduler: Optional[AbstractScheduler] = None
        self._shutdown_flag: bool = False
        self._running: bool = False
        self._is_task_cancel_requested: bool = False
        self._dry_run: bool = dry_run
        self._current_tasks: List[SimulationTask] = []

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

    async def _cancel_task_if_possible(
        self, progress_id: str, tasks: List[SimulationTask]
    ) -> bool:
        """尝试取消任务。

        当工作者关闭且请求取消任务时，尝试取消正在执行的任务。

        Args:
            progress_id: 要取消的任务进度 ID

        Returns:
            bool: 如果成功取消则返回 True，否则返回 False
        """
        # 添加详细的调试日志，便于跟踪取消任务的流程
        logger.debug(
            f"尝试取消任务，进度 ID: {progress_id}, shutdown: {self._shutdown_flag}, "
            f"cancel_tasks: {self._is_task_cancel_requested}"
        )
        # 仅在工作者关闭且明确请求取消任务时执行取消操作
        if self._shutdown_flag and self._is_task_cancel_requested:
            await logger.aerror(f"工作者已关闭，尝试取消任务，进度 ID: {progress_id}")
            success = await self._client.delete_simulation(progress_id=progress_id)
            if success:
                await logger.ainfo(f"任务取消成功，进度 ID: {progress_id}")

                async with get_db_session(DB_SIMULATION) as session:
                    for task in tasks:
                        task.status = SimulationTaskStatus.CANCELLED
                        await session.merge(task)
                    await session.commit()

                return True
            await logger.aerror(f"任务取消失败，进度 ID: {progress_id}")
        return False

    async def _handle_task_completion(
        self, task: SimulationTask, result: SingleSimulationResultView
    ) -> None:
        """处理任务完成后的逻辑。

        处理任务完成后的回调逻辑，包括任务状态更新和完成通知。

        Args:
            task: 已完成的模拟任务对象
            result: 任务执行的结果数据

        Todo:
            * 更新任务状态和必要的字段
            * 为失败的任务记录错误信息并通知其他服务
            * 调用注册的回调函数通知任务完成
        """
        # 1. 更新任务状态和必要的字段
        # 2. 如果是失败的任务，可能需要记录错误信息并通知其他服务
        # 3. 确认任务是否已成功完成，并更新相关统计信息
        logger.info(f"任务 {task.id} 完成，结果: {result}")

        task.result = result.model_dump(mode="python")  # 保存原始结果，用户后续评估分析
        task.child_progress_id = result.id
        try:
            task.status = SimulationTaskStatus(result.status)
        except ValueError:
            await logger.aerror(
                f"未知任务状态，任务 ID: {task.id}，状态: {result.status}"
            )
        task.completed_at = datetime.now()
        if task.status == SimulationTaskStatus.COMPLETE:
            task.alpha_id = result.alpha

        async with get_db_session(DB_SIMULATION) as session:
            await session.merge(task)
            await session.commit()
            await logger.ainfo(
                f"更新任务状态成功，任务 ID: {task.id}，状态: {task.status}"
            )
            # 因为这里数据更新是个很低频的操作，每次都提交事务即可

        if self._dry_run:
            logger.debug("dry-run: 回调方法不调用。")

        for callback in self._task_complete_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 如果是异步函数，使用 await 调用
                    await callback(task, result)
                else:
                    # 如果是同步函数，直接调用
                    callback(task, result)
            except Exception as e:
                await logger.aerror(
                    f"调用任务完成回调函数失败，任务 ID: {task.id}，错误: {e}"
                )

    async def _process_single_simulation_task(self, task: SimulationTask) -> None:
        """处理单个模拟任务。

        创建并监控单个模拟任务的执行过程，包括创建任务、检查进度和处理结果。
        该方法会一直运行直到任务完成或被取消。

        Args:
            task: 要处理的模拟任务对象

        Returns:
            None
        """
        logger.debug(f"开始处理单个模拟任务，任务 ID: {task.id}")
        if self._shutdown_flag:
            await logger.aerror(f"工作者已关闭，无法处理任务 ID: {task.id}")
            return

        if self._dry_run:
            logger.debug("dry-run 模式下跳过实际请求。")
            task.status = SimulationTaskStatus.RUNNING
            mock_result = SingleSimulationResultView(
                id="dry-run-single-id",
                status="COMPLETE",
                alpha=None,
                type=AlphaType.REGULAR,
            )
            await self._handle_task_completion(task, mock_result)
            return

        # 构建任务负载数据
        payload = build_single_simulation_payload(task)

        async with self._client:
            try:
                # 创建模拟任务
                logger.debug(
                    f"发送创建单个模拟任务请求，任务 ID: {task.id}, 请求体: {payload}"
                )
                success, progress_id, retry_after = (
                    await self._client.create_single_simulation(payload=payload)
                )

                # 处理创建失败的情况
                if not success:
                    await logger.awarning(
                        f"创建单个模拟任务失败，任务 ID: {task.id}，请求体: {payload}"
                    )
                    return

                await logger.ainfo(
                    f"创建单个模拟任务成功，任务 ID: {task.id}，进度 ID: {progress_id}"
                )

                # 等待指定时间后开始检查进度
                await asyncio.sleep(retry_after)

                # 循环检查任务进度直到完成
                prev_progress: float = 0.0
                while True:
                    logger.debug(
                        f"检查任务进度，任务 ID: {task.id}, 进度 ID: {progress_id}"
                    )
                    if await self._cancel_task_if_possible(progress_id, tasks=[task]):
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
                        await self._handle_task_completion(task, progress_or_result)
                        break

                    if isinstance(progress_or_result, SimulationProgressView):
                        progress: float = progress_or_result.progress
                        if progress != prev_progress:
                            await logger.ainfo(
                                f"单个模拟任务进行中，任务 ID: {task.id}，"
                                + f"进度 ID: {progress_id}，进度: {progress * 100:.2f}%"
                            )
                            prev_progress = progress
                    else:
                        await logger.aerror(
                            f"未知返回值组合，任务 ID: {task.id}，进度 ID: {progress_id}，"
                            f"返回值: finished={finished}, progress_or_result={progress_or_result}, "
                            + f"retry_after={retry_after}"
                        )
                    await asyncio.sleep(retry_after)
            except Exception as e:
                # 记录异常信息，确保异常不会中断工作者主循环
                await logger.aerror(
                    f"处理单个模拟任务失败，任务 ID: {task.id}，错误: {e}"
                )

    async def _handle_multi_task_completion(
        self, tasks: List[SimulationTask], result: MultiSimulationResultView
    ) -> None:
        """处理完成多个模拟任务后的逻辑。

        为每个子任务创建异步任务以获取其结果，并安排处理完成回调。

        Args:
            tasks: 模拟任务列表
            result: 多个模拟任务的结果，包含子任务ID信息
        """

        async def handle_completed_task(task: SimulationTask, child_id: str) -> None:
            """处理多个模拟任务的子任务。

            获取单个子任务的结果并调用相应的完成处理逻辑。

            Args:
                task: 模拟任务
                child_id: 子任务 ID
            """
            if self._dry_run:
                logger.debug("dry-run: 跳过 get_multi_simulation_child_result 调用。")
                mock_result = SingleSimulationResultView(
                    id=f"dry-run-child-{child_id}",
                    status="COMPLETE",
                    alpha=None,
                    type=AlphaType.REGULAR,
                )
                await self._handle_task_completion(task, mock_result)
                return

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
                    await self._handle_task_completion(task, result)
                except Exception as e:
                    # 捕获所有异常并记录错误，不向上继续抛出
                    await logger.aerror(
                        f"处理多个模拟任务的子任务失败，任务实体 ID {task.id} 子任务 ID {child_id} 错误: {e}"
                    )

        # 使用锁确保并发安全地管理异步任务
        async with self._post_handler_lock:
            # 为每个子任务创建异步任务并添加到待处理列表
            self._post_handler_futures.extend(
                [
                    asyncio.create_task(handle_completed_task(task, child_id))
                    for task, child_id in zip(tasks, result.children)
                ]
            )

    async def _process_multi_simulation_task(self, tasks: List[SimulationTask]) -> None:
        """处理多个模拟任务的方法。

        创建并监控多个模拟任务的集合，适用于顾问角色用户。该方法会验证用户权限、
        任务数量限制，并处理任务完成后的结果。

        Args:
            tasks: 模拟任务列表
        """
        logger.debug(
            f"开始处理多个模拟任务，任务 ID 列表: {[task.id for task in tasks]}"
        )
        if self._user_role != ROLE_CONSULTANT:
            await logger.aerror("当前用户角色不是顾问，无法处理多个模拟任务")
            return

        if self._shutdown_flag:
            await logger.aerror("工作者已关闭，无法处理多个模拟任务")
            return

        if len(tasks) > MAX_CONSULTANT_SIMULATION_SLOTS:
            await logger.aerror(
                f"任务数量超出限制，当前任务数量: {len(tasks)}，最大数量: {MAX_CONSULTANT_SIMULATION_SLOTS}"
            )
            return

        if self._dry_run:
            logger.debug("dry-run 模式下跳过实际请求。")
            for t in tasks:
                t.status = SimulationTaskStatus.RUNNING
            mock_result = MultiSimulationResultView(
                children=[f"dry-run-child-{t.id}" for t in tasks],
                type=AlphaType.REGULAR,
                status="COMPLETE",
            )
            await self._handle_multi_task_completion(tasks, mock_result)
            return

        single_simu_payloads: List[SingleSimulationPayload] = [
            build_single_simulation_payload(task) for task in tasks
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

                for task in tasks:
                    task.status = SimulationTaskStatus.RUNNING
                    task.parent_progress_id = progress_id

                async with get_db_session(DB_SIMULATION) as session:
                    for task in tasks:
                        await session.merge(task)
                    await session.commit()
                    await logger.ainfo(
                        f"更新多个模拟任务状态成功，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}"
                        + f"，状态: {SimulationTaskStatus.RUNNING}"
                    )

                await asyncio.sleep(retry_after)

                prev_progress: float = 0.0
                while True:
                    logger.debug(
                        f"检查多个任务进度，任务 ID 列表: {task_ids_str}, 进度 ID: {progress_id}"
                    )
                    if await self._cancel_task_if_possible(progress_id, tasks=tasks):
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
                                f"多个模拟任务进行中，任务 ID 列表: {task_ids_str}，"
                                + f"进度 ID: {progress_id}，进度: {progress * 100:.2f}%"
                            )
                            prev_progress = progress
                    else:
                        await logger.aerror(
                            f"未知返回值组合，任务 ID 列表: {task_ids_str}，进度 ID: {progress_id}，"
                            + f"返回值: finished={finished}, progress_or_result={progress_or_result}, "
                            + f"retry_after={retry_after}"
                        )
                    await asyncio.sleep(retry_after)
            except Exception as e:
                await logger.aerror(
                    f"处理多个模拟任务失败，任务 ID 列表: {task_ids_str}，错误: {e}"
                )

    async def _do_work(self) -> None:
        """执行工作的主循环方法。

        不断从调度器获取任务并根据用户角色执行单个或多个模拟任务，
        直到工作者被关闭。

        Raises:
            Exception: 当调度器未设置时
            ValueError: 当遇到未知用户角色时
        """
        logger.debug("开始执行工作循环")
        while not self._shutdown_flag:
            # 验证调度器是否已设置
            if self._scheduler is None:
                await logger.aerror("调度器未设置，无法执行工作")
                raise Exception("调度器未设置，无法执行工作")

            # 根据用户角色确定任务批量大小
            scheduled_task_count: int = (
                MAX_CONSULTANT_SIMULATION_SLOTS
                if self._user_role == ROLE_CONSULTANT
                else 1
            )

            # 从调度器获取任务
            tasks: List[SimulationTask] = await self._scheduler.schedule(
                batch_size=scheduled_task_count
            )
            self._current_tasks = tasks

            # 如果没有可用任务，等待后重试
            if not tasks:
                logger.debug("调度器未返回任务，等待 5 秒后重试")
                await logger.ainfo("没有可执行的任务")
                await asyncio.sleep(5)
                continue

            for task in tasks:
                task.scheduled_at = datetime.now()
                task.status = SimulationTaskStatus.SCHEDULED

            async with get_db_session(DB_SIMULATION) as session:
                for task in tasks:
                    await session.merge(task)
                await session.commit()
                task_info = "\n".join(
                    f"任务 ID: {task.id}, 优先级: {task.priority}, 分组键: {task.settings_group_key}"
                    for task in tasks
                )
                await logger.ainfo(f"调度器返回任务，任务详情:\n{task_info}")

            # 根据用户角色执行不同的任务处理逻辑
            if self._user_role == ROLE_USER:
                await self._process_single_simulation_task(tasks[0])
            elif self._user_role == ROLE_CONSULTANT:
                await self._process_multi_simulation_task(tasks)
            else:
                await logger.aerror(f"未知用户角色 {self._user_role}，无法处理任务")
                raise ValueError(f"未知用户角色 {self._user_role}，无法处理任务")

            self._current_tasks = []

    async def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """设置任务调度器。

        Args:
            scheduler: 调度器实例，用于获取待执行的任务
        """
        logger.debug(
            f"设置调度器，当前调度器: {self._scheduler}, 新调度器: {scheduler}"
        )
        await logger.adebug(f"设置调度器 {scheduler}，当前调度器 {self._scheduler}")
        self._scheduler = scheduler

    async def run(self) -> None:
        """运行工作者，开始执行任务循环。

        重置关闭标志并启动工作循环，直到被明确停止。
        """
        if self._running:
            await logger.aerror("工作者已在运行中，无法重复启动")
            return
        self._running = True

        logger.debug("启动工作者")
        self._shutdown_flag = False
        await self._do_work()
        await logger.ainfo("工作者已停止")
        self._running = False

    async def stop(self, cancel_tasks: bool = False) -> None:
        """停止工作者，清理资源。

        设置关闭标志，等待所有挂起的任务完成，并可选择取消正在执行的任务。

        Args:
            cancel_tasks: 如果为True，将尝试取消所有正在执行的任务
        """
        logger.debug(f"停止工作者，cancel_tasks: {cancel_tasks}")
        self._shutdown_flag = True
        self._is_task_cancel_requested = cancel_tasks
        await asyncio.gather(*self._post_handler_futures)
        await logger.ainfo("工作者已停止")

    async def add_task_complete_callback(
        self,
        callback: Union[
            Callable[[SimulationTask, SingleSimulationResultView], None],
            Callable[[SimulationTask, SingleSimulationResultView], Awaitable[None]],
        ],
    ) -> None:
        """添加任务完成回调函数。

        注册一个将在任务完成时调用的回调函数。支持同步和异步回调函数。

        Args:
            callback: 任务完成时的回调函数，接收任务和结果作为参数。
                     可以是同步函数或异步函数

        Raises:
            RuntimeError: 当工作者未关闭时无法添加回调
        """
        logger.debug(f"添加任务完成回调函数: {callback}")
        if not self._shutdown_flag:
            await logger.aerror("工作者未关闭，无法添加任务完成回调函数")
        self._task_complete_callbacks.append(callback)
        await logger.adebug(
            f"添加任务完成回调函数 {callback}， 当前回调函数数量 {len(self._task_complete_callbacks)}"
        )

    async def get_current_tasks(self) -> List[SimulationTask]:
        """获取当前任务的信息。
        返回当前正在执行的任务列表。
        """
        logger.debug(f"获取当前任务，当前任务数量: {len(self._current_tasks)}")
        return self._current_tasks
