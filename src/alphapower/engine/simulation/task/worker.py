import asyncio
import random
from datetime import datetime
from typing import Awaitable, Callable, List, Optional, Union

from structlog.stdlib import BoundLogger

from alphapower.client import (
    AlphaPropertiesPayload,
    MultiSimulationPayload,
    MultiSimulationResultView,
    SimulationProgressView,
    SimulationSettingsView,
    SingleSimulationPayload,
    WorldQuantClient,
)
from alphapower.constants import (
    MAX_CONSULTANT_SIMULATION_SLOTS,
    ROLE_CONSULTANT,
    ROLE_USER,
    AlphaType,
    Database,
    SimulationResultStatus,
    SimulationTaskStatus,
    UserRole,
)
from alphapower.dal import simulation_task_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity import SimulationTask
from alphapower.internal.logging import get_logger
from alphapower.view.simulation import SingleSimulationResultView

from .scheduler_abc import AbstractScheduler
from .worker_abc import AbstractWorker


def build_single_simulation_payload(
    task: SimulationTask, logger: BoundLogger
) -> SingleSimulationPayload:

    logger.debug(
        event="进入 build_single_simulation_payload",
        emoji="🔧",
        task_id=task.id,
    )
    # 详细记录构建负载数据过程，便于调试
    setting = SimulationSettingsView.model_construct(
        region=task.region,
        delay=task.delay,
        language=task.language,
        instrument_type=task.instrument_type,
        universe=task.universe,
        neutralization=task.neutralization,
        pasteurization=task.pasteurization,
        unit_handling=task.unit_handling,
        max_trade=task.max_trade,
        decay=task.decay,
        truncation=task.truncation,
        visualization=task.visualization,
        test_period=task.test_period,
        nan_handling=task.nan_handling,
    )

    logger.debug(
        event="生成单个模拟任务负载数据",
        emoji="🛠️",
        task_id=task.id,
        task_type=task.type.value,
        settings=setting.model_dump(mode="json"),
    )
    payload: SingleSimulationPayload = SingleSimulationPayload(
        type=task.type.value,
        settings=setting,
        regular=task.regular,
    )
    logger.debug(
        event="生成的负载数据",
        emoji="📦",
        task_id=task.id,
        payload=payload.model_dump(mode="json", by_alias=True),
    )
    logger.debug(
        event="退出 build_single_simulation_payload",
        emoji="🚪",
        task_id=task.id,
    )
    return payload


class Worker(AbstractWorker):

    def __init__(self, client: WorldQuantClient, dry_run: bool = False) -> None:

        self._client: WorldQuantClient = client
        self._post_handler_lock: asyncio.Lock = asyncio.Lock()
        self._post_handler_futures: List[asyncio.Task] = []
        self._task_complete_callbacks: List[
            Union[
                Callable[[SimulationTask, SingleSimulationResultView], None],
                Callable[[SimulationTask, SingleSimulationResultView], Awaitable[None]],
            ]
        ] = []
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_callbacks: List[
            Union[
                Callable[["AbstractWorker"], None],
                Callable[["AbstractWorker"], Awaitable[None]],
            ]
        ] = []
        self._scheduler: Optional[AbstractScheduler] = None
        self._shutdown_flag: bool = False
        self._running: bool = False
        self._run_lock: asyncio.Lock = asyncio.Lock()
        self._is_task_cancel_requested: bool = False
        self._dry_run: bool = dry_run
        self._current_tasks: List[SimulationTask] = []
        self._user_role: UserRole = UserRole.DEFAULT
        self.log: BoundLogger = get_logger(
            module_name=f"{__name__}.{self.__class__.__name__}"
        )

        if not isinstance(self._client, WorldQuantClient):
            raise ValueError("Client must be an instance of WorldQuantClient.")

    async def _cancel_task_if_possible(
        self, progress_id: str, tasks: List[SimulationTask]
    ) -> bool:

        await self.log.adebug(
            event="判断是否应该取消任务",
            emoji="❓",
            progress_id=progress_id,
            shutdown=self._shutdown_flag,
            cancel_tasks=self._is_task_cancel_requested,
            task_count=len(tasks),
        )
        # 仅在工作者关闭且明确请求取消任务时执行取消操作
        if self._shutdown_flag and self._is_task_cancel_requested:
            await self.log.ainfo(
                event="工作者已关闭，尝试取消任务",
                emoji="🚫",
                progress_id=progress_id,
                task_count=len(tasks),
            )

            jitter = random.uniform(0, 1)  # 随机生成 0 到 1 秒之间的抖动时间
            await asyncio.sleep(jitter)
            success = await self._client.simulation_delete(progress_id=progress_id)
            if success:
                await self.log.ainfo(
                    event="任务取消成功",
                    emoji="✅",
                    progress_id=progress_id,
                )

                async with (
                    session_manager.get_session(Database.SIMULATION) as session,
                    session.begin(),
                ):
                    for task in tasks:
                        task.status = SimulationTaskStatus.CANCELLED
                    await simulation_task_dal.update_all(
                        entities=tasks, session=session
                    )
                    await self.log.ainfo(
                        event="数据库中任务状态更新为已取消",
                        emoji="💾",
                        progress_id=progress_id,
                        task_ids=[t.id for t in tasks],
                    )

                return True
            await self.log.aerror(
                event="任务取消失败",
                emoji="❌",
                progress_id=progress_id,
            )

        await self.log.adebug(
            event="任务取消请求未满足条件，跳过取消操作",
            emoji="⏭️",
            progress_id=progress_id,
        )
        return False

    async def _handle_task_finish(
        self, task: SimulationTask, result: SingleSimulationResultView
    ) -> None:

        await self.log.ainfo(
            event="任务完成，开始处理结果",
            emoji="🎉",
            task_id=task.id,
            result_status=result.status,
            result_id=result.id,
        )

        task.result = result.model_dump(mode="json")  # 保存原始结果，用户后续评估分析
        task.child_progress_id = result.id
        try:
            task.status = SimulationTaskStatus(result.status.value)
        except ValueError:
            await self.log.aerror(
                event="收到未知的任务状态",
                emoji="❓",
                task_id=task.id,
                received_status=result.status,
            )
            # 可以考虑设置一个默认错误状态或保持原状态
            task.status = SimulationTaskStatus.ERROR  # 假设有一个错误状态
        task.completed_at = datetime.now()
        task.alpha_id = result.alpha

        async with (
            session_manager.get_session(Database.SIMULATION) as session,
            session.begin(),
        ):
            await simulation_task_dal.update(task, session=session)
            await self.log.ainfo(
                event="数据库中任务状态更新成功",
                emoji="💾",
                task_id=task.id,
                new_status=task.status.value,
            )
            # 因为这里数据更新是个很低频的操作，每次都提交事务即可

        # 更新完成的因子标签，有 alpha_id 才有意义
        if task.alpha_id:
            if task.status != SimulationTaskStatus.COMPLETE:
                await self.log.awarning(
                    event="任务状态不是 COMPLETE，有错误信息",
                    emoji="⚠️",
                    task_id=task.id,
                    alpha_id=task.alpha_id,
                    status=task.status.value,
                    result=task.result,
                )

            async with self._client:
                try:
                    await self.log.adebug(
                        event="尝试更新因子属性",
                        emoji="🏷️",
                        task_id=task.id,
                        alpha_id=task.alpha_id,
                        tags=task.tags,
                    )
                    await self._client.alpha_update_properties(
                        alpha_id=task.alpha_id,
                        properties=AlphaPropertiesPayload(
                            name=task.alpha_id,
                            tags=task.tags,
                        ),
                    )
                    await self.log.ainfo(
                        event="更新因子属性成功",
                        emoji="✅",
                        task_id=task.id,
                        alpha_id=task.alpha_id,
                    )
                except Exception:
                    await self.log.aexception(
                        event="更新因子属性时发生异常",
                        emoji="❌",
                        task_id=task.id,
                        alpha_id=task.alpha_id,
                    )
                    # 注意：这里仅记录异常，不影响后续回调执行

        if self._dry_run:
            await self.log.adebug(
                event="Dry-run 模式，跳过任务完成回调",
                emoji="🚫",
                task_id=task.id,
            )
            return  # 在 dry-run 模式下直接返回

        await self.log.adebug(
            event="开始调用任务完成回调",
            emoji="📞",
            task_id=task.id,
            callback_count=len(self._task_complete_callbacks),
        )
        for callback in self._task_complete_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 如果是异步函数，使用 await 调用
                    await callback(task, result)
                else:
                    # 如果是同步函数，直接调用
                    callback(task, result)
            except Exception:
                await self.log.aexception(
                    event="调用任务完成回调函数时发生异常",
                    emoji="💥",
                    task_id=task.id,
                    callback_name=getattr(callback, "__name__", repr(callback)),
                )
        await self.log.adebug(
            event="任务完成回调调用结束",
            emoji="🏁",
            task_id=task.id,
        )

    async def _heartbeat(self, name: str) -> None:

        await self.log.adebug(
            event="心跳检查开始",
            emoji="💓",
            node_name=name,
            callback_count=len(self._heartbeat_callbacks),
        )

        async def _heartbeat_async_task() -> None:

            for callback in self._heartbeat_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        # 如果是异步函数，使用 await 调用
                        await callback(self)
                    else:
                        # 如果是同步函数，直接调用
                        callback(self)
                except Exception:
                    await self.log.aexception(
                        event="心跳回调执行失败",
                        emoji="💔",
                        node_name=name,
                        callback_name=getattr(callback, "__name__", repr(callback)),
                    )

        # 创建并启动异步任务
        if self._heartbeat_task and not self._heartbeat_task.done():
            task_name = self._heartbeat_task.get_name()
            await self.log.awarning(
                event="上一个心跳任务尚未完成，等待其结束",
                emoji="⏳",
                current_node_name=name,
                previous_task_name=task_name,
                # task_stack=self._heartbeat_task.get_stack(), # 堆栈信息可能过长，谨慎使用
            )
            try:
                await asyncio.wait_for(
                    self._heartbeat_task, timeout=10.0
                )  # 设置超时等待
            except asyncio.TimeoutError:
                await self.log.aerror(
                    event="等待上一个心跳任务超时",
                    emoji="⌛",
                    previous_task_name=task_name,
                )
            except Exception:
                await self.log.aexception(
                    event="等待上一个心跳任务时发生异常",
                    emoji="💥",
                    previous_task_name=task_name,
                )

        self._heartbeat_task = asyncio.create_task(
            _heartbeat_async_task(), name=f"heartbeat-{name}"
        )
        await self.log.adebug(
            event="心跳检查任务已创建",
            emoji="✅",
            node_name=name,
            task_name=self._heartbeat_task.get_name(),
        )

    async def _process_single_simulation_task(self, task: SimulationTask) -> None:

        await self.log.ainfo(
            event="开始处理单个模拟任务",
            emoji="🚀",
            task_id=task.id,
            task_type=task.type.value,
            user_role=self._user_role.value,
        )
        if self._shutdown_flag:
            await self.log.awarning(
                event="工作者已关闭，无法处理新任务",
                emoji="🛑",
                task_id=task.id,
            )
            return

        if self._dry_run:
            await self.log.ainfo(
                event="Dry-run 模式，模拟单个任务执行",
                emoji="🧪",
                task_id=task.id,
            )
            task.status = SimulationTaskStatus.RUNNING  # 模拟运行状态
            # 模拟一个成功的响应
            mock_result = SingleSimulationResultView(
                id=f"dry-run-single-{task.id}",
                status=SimulationResultStatus.COMPLETE,  # 使用枚举值
                alpha=f"dry-run-alpha-{task.id}",  # 模拟 Alpha ID
                type=task.type,  # 使用任务的类型
            )
            await self._handle_task_finish(task, mock_result)
            return

        # 构建任务负载数据
        payload = build_single_simulation_payload(task, self.log)

        async with self._client:
            progress_id: Optional[str] = None  # 初始化 progress_id
            try:
                # 创建模拟任务
                await self.log.adebug(
                    event="发送创建单个模拟任务请求",
                    emoji="📤",
                    task_id=task.id,
                )
                success, progress_id, retry_after = (
                    await self._client.simulation_create_single(payload=payload)
                )

                # 处理创建失败的情况
                if not success or not progress_id:
                    await self.log.aerror(
                        event="创建单个模拟任务失败",
                        emoji="❌",
                        task_id=task.id,
                        progress_id=progress_id,  # 记录返回的 progress_id (可能为 None)
                    )
                    # 可以考虑更新任务状态为失败
                    task.status = SimulationTaskStatus.ERROR
                    task.completed_at = datetime.now()
                    async with (
                        session_manager.get_session(Database.SIMULATION) as session,
                        session.begin(),
                    ):
                        await simulation_task_dal.update(task, session=session)
                    return

                await self.log.ainfo(
                    event="创建单个模拟任务成功，等待首次进度检查",
                    emoji="✅",
                    task_id=task.id,
                    progress_id=progress_id,
                    retry_after=f"{retry_after}s",
                )

                # 更新任务状态为运行中，并保存 progress_id
                task.status = SimulationTaskStatus.RUNNING
                task.parent_progress_id = (
                    progress_id  # 单任务也用 parent_progress_id 存储
                )
                async with (
                    session_manager.get_session(Database.SIMULATION) as session,
                    session.begin(),
                ):
                    await simulation_task_dal.update(task, session=session)
                    await self.log.ainfo(
                        event="数据库中任务状态更新为运行中",
                        emoji="💾",
                        task_id=task.id,
                        progress_id=progress_id,
                    )

                # 等待指定时间后开始检查进度
                await asyncio.sleep(retry_after)

                # 循环检查任务进度直到完成
                prev_progress: float = -1.0  # 初始化为-1，确保第一次进度会被记录
                while True:
                    #! 4. 心跳检查
                    await self._heartbeat(name=f"single_task_poll_{task.id}")
                    await self.log.adebug(
                        event="检查任务进度",
                        emoji="🔍",
                        task_id=task.id,
                        progress_id=progress_id,
                    )
                    if await self._cancel_task_if_possible(progress_id, tasks=[task]):
                        await self.log.ainfo(
                            event="任务已被取消，停止进度检查",
                            emoji="🚫",
                            task_id=task.id,
                            progress_id=progress_id,
                        )
                        break  # 任务已取消，退出循环

                    finished, progress_or_result, retry_after = (
                        await self._client.simulation_get_progress_single(
                            progress_id=progress_id
                        )
                    )

                    if finished:
                        if isinstance(progress_or_result, SingleSimulationResultView):
                            await self.log.ainfo(
                                event="单个模拟任务完成",
                                emoji="🏁",
                                task_id=task.id,
                                progress_id=progress_id,
                                result_status=progress_or_result.status,
                            )
                            await self._handle_task_finish(task, progress_or_result)
                        else:
                            # finished 为 True 但结果类型不匹配，记录错误
                            await self.log.aerror(
                                event="任务完成但结果类型不匹配",
                                emoji="❓",
                                task_id=task.id,
                                progress_id=progress_id,
                                expected_type="SingleSimulationResultView",
                                received_type=type(progress_or_result).__name__,
                                received_value=progress_or_result,
                            )
                            # 可以在这里将任务标记为错误状态
                        break  # 任务完成，退出循环
                    elif isinstance(progress_or_result, SimulationProgressView):
                        progress: float = progress_or_result.progress
                        if abs(progress - prev_progress) > 1e-6:  # 比较浮点数
                            await self.log.ainfo(
                                event="单个模拟任务进行中",
                                emoji="⏳",
                                task_id=task.id,
                                progress_id=progress_id,
                                progress=f"{progress * 100:.2f}%",
                            )
                            prev_progress = progress
                        else:
                            # 进度未变化，可以考虑使用 DEBUG 级别记录
                            await self.log.adebug(
                                event="任务进度未变化",
                                emoji="🧘",
                                task_id=task.id,
                                progress_id=progress_id,
                                progress=f"{progress * 100:.2f}%",
                            )
                    else:
                        # 返回值组合未知，记录错误
                        await self.log.aerror(
                            event="获取任务进度时返回未知组合",
                            emoji="❓",
                            task_id=task.id,
                            progress_id=progress_id,
                            finished=finished,
                            progress_or_result_type=type(progress_or_result).__name__,
                            progress_or_result=progress_or_result,
                            retry_after=retry_after,
                        )
                        # 考虑是否需要退出循环或重试

                    await self.log.adebug(
                        event="等待下次进度检查",
                        emoji="😴",
                        task_id=task.id,
                        progress_id=progress_id,
                        retry_after=f"{retry_after}s",
                    )
                    await asyncio.sleep(retry_after)
            except Exception:
                # 记录异常信息，确保异常不会中断工作者主循环
                await self.log.aexception(
                    event="处理单个模拟任务时发生异常",
                    emoji="💥",
                    task_id=task.id,
                    progress_id=progress_id,  # 记录当前的 progress_id
                )
                # 可以在这里将任务标记为错误状态
                task.status = SimulationTaskStatus.ERROR
                task.completed_at = datetime.now()
                async with (
                    session_manager.get_session(Database.SIMULATION) as session,
                    session.begin(),
                ):
                    await simulation_task_dal.update(task, session=session)
            finally:
                await self.log.ainfo(
                    event="单个模拟任务处理结束",
                    emoji="🔚",
                    task_id=task.id,
                    final_status=task.status.value,
                )

    async def _handle_multi_task_finish(
        self, tasks: List[SimulationTask], result: MultiSimulationResultView
    ) -> None:
        # 记录处理多任务结果的入口，INFO 级别，避免与后续详细日志重复
        parent_progress_id: Optional[str] = (
            tasks[0].parent_progress_id if tasks else None
        )
        task_ids: List[int] = [t.id for t in tasks]
        await self.log.ainfo(
            event="多个模拟任务集合完成，准备处理所有子任务结果",
            emoji="🧩",
            parent_progress_id=parent_progress_id,
            task_ids=task_ids,
            child_progress_ids=result.children,
            result_status=result.status,
        )

        # 只在错误时输出 ERROR 日志，避免内容重复
        if result.status == "ERROR":
            await self.log.aerror(
                event="多个模拟任务处理失败，终止后续子任务处理",
                emoji="❌",
                parent_progress_id=parent_progress_id,
                task_ids=task_ids,
                result_status=result.status,
            )
            # 不要返回，还有子任务需要处理

        # 内部函数处理单个子任务结果，DEBUG 级别详细跟踪
        async def handle_finished_task(task: SimulationTask, child_id: str) -> None:
            await self.log.adebug(
                event="开始处理多任务中的单个子任务",
                emoji="🔧",
                task_id=task.id,
                child_progress_id=child_id,
            )
            if self._dry_run:
                await self.log.ainfo(
                    event="Dry-run 模式，模拟获取子任务结果",
                    emoji="🧪",
                    task_id=task.id,
                    child_progress_id=child_id,
                )
                mock_result: SingleSimulationResultView = SingleSimulationResultView(
                    id=child_id,  # 使用传入的 child_id
                    status=SimulationResultStatus.COMPLETE,
                    alpha=f"dry-run-alpha-{task.id}",
                    type=task.type,
                )
                await self._handle_task_finish(task, mock_result)
                return

            async with self._client:
                try:
                    await self.log.adebug(
                        event="尝试获取子任务结果",
                        emoji="📥",
                        task_id=task.id,
                        child_progress_id=child_id,
                    )
                    finished: bool
                    child_result: Optional[SingleSimulationResultView]
                    finished, child_result = (
                        await self._client.simulation_get_child_result(
                            child_progress_id=child_id,
                        )
                    )
                    if not finished or not isinstance(
                        child_result, SingleSimulationResultView
                    ):
                        await self.log.aerror(
                            event="获取子任务结果失败或类型不匹配",
                            emoji="❌",
                            task_id=task.id,
                            child_progress_id=child_id,
                            finished=finished,
                            result_type=type(child_result).__name__,
                        )
                        # 标记任务为错误状态
                        task.status = SimulationTaskStatus.ERROR
                        task.completed_at = datetime.now()
                        async with (
                            session_manager.get_session(Database.SIMULATION) as session,
                            session.begin(),
                        ):
                            await simulation_task_dal.update(task, session=session)
                        return

                    await self.log.ainfo(
                        event="获取子任务结果成功",
                        emoji="✅",
                        task_id=task.id,
                        child_progress_id=child_id,
                        result_status=child_result.status,
                    )
                    await self._handle_task_finish(task, child_result)
                except Exception as ex:
                    # 捕获所有异常并记录错误，不向上继续抛出
                    await self.log.aexception(
                        event="处理子任务时发生异常",
                        emoji="💥",
                        task_id=task.id,
                        child_progress_id=child_id,
                        exception=str(ex),
                    )
                    # 标记任务为错误状态
                    task.status = SimulationTaskStatus.ERROR
                    task.completed_at = datetime.now()
                    async with (
                        session_manager.get_session(Database.SIMULATION) as session,
                        session.begin(),
                    ):
                        await simulation_task_dal.update(task, session=session)
                finally:
                    await self.log.adebug(
                        event="单个子任务处理结束",
                        emoji="🔚",
                        task_id=task.id,
                        child_progress_id=child_id,
                        final_status=task.status.value,
                    )

        # 使用锁确保并发安全地管理异步任务
        async with self._post_handler_lock:
            await self.log.adebug(
                event="获取 post_handler_lock，准备创建子任务处理协程",
                emoji="🔒",
                parent_progress_id=parent_progress_id,
                task_count=len(tasks),
                child_count=len(result.children),
            )
            if len(tasks) != len(result.children):
                await self.log.awarning(
                    event="任务数量与子任务 ID 数量不匹配，仅处理可配对部分",
                    emoji="⚠️",
                    parent_progress_id=parent_progress_id,
                    task_count=len(tasks),
                    child_count=len(result.children),
                )
                # 只处理可配对部分，剩余任务建议在调度层补偿

            # 为每个子任务创建异步任务并添加到待处理列表
            new_futures = [
                asyncio.create_task(
                    handle_finished_task(task, child_id),
                    name=f"handle_child_{task.id}_{child_id}",
                )
                for task, child_id in zip(
                    tasks, result.children
                )  # 使用 zip 保证一一对应
            ]
            self._post_handler_futures.extend(new_futures)
            await self.log.adebug(
                event="子任务处理协程已创建并添加",
                emoji="➕",
                parent_progress_id=parent_progress_id,
                new_future_count=len(new_futures),
                total_future_count=len(self._post_handler_futures),
            )

    async def _process_multi_simulation_task(self, tasks: List[SimulationTask]) -> None:

        task_ids: List[int] = [task.id for task in tasks]
        await self.log.ainfo(
            event="开始处理多个模拟任务",
            emoji="🚀",
            task_count=len(tasks),
            task_ids=task_ids,
            user_role=self._user_role.value,
        )

        if self._user_role != UserRole.CONSULTANT:
            await self.log.aerror(
                event="权限不足，无法处理多个模拟任务",
                emoji="🚫",
                required_role=UserRole.CONSULTANT.value,
                current_role=self._user_role.value,
                task_ids=task_ids,
            )
            # 可以考虑将这些任务标记为失败
            return

        if self._shutdown_flag:
            await self.log.awarning(
                event="工作者已关闭，无法处理新任务",
                emoji="🛑",
                task_ids=task_ids,
            )
            return

        if len(tasks) > MAX_CONSULTANT_SIMULATION_SLOTS:
            await self.log.aerror(
                event="任务数量超出顾问角色限制",
                emoji="📈",
                task_count=len(tasks),
                limit=MAX_CONSULTANT_SIMULATION_SLOTS,
                task_ids=task_ids,
            )
            # 可以考虑将这些任务标记为失败
            return

        if self._dry_run:
            await self.log.ainfo(
                event="Dry-run 模式，模拟多个任务执行",
                emoji="🧪",
                task_ids=task_ids,
            )
            for t in tasks:
                t.status = SimulationTaskStatus.RUNNING  # 模拟运行状态
            # 模拟一个成功的响应
            mock_result = MultiSimulationResultView(
                children=[f"dry-run-child-{t.id}" for t in tasks],
                type=(
                    tasks[0].type if tasks else AlphaType.REGULAR
                ),  # 取第一个任务的类型或默认
                status=SimulationTaskStatus.COMPLETE.value,
            )
            await self._handle_multi_task_finish(tasks, mock_result)
            return

        single_simu_payloads: List[SingleSimulationPayload] = [
            build_single_simulation_payload(task, self.log) for task in tasks
        ]
        payload: MultiSimulationPayload = MultiSimulationPayload(
            root=single_simu_payloads
        )

        async with self._client:
            progress_id: Optional[str] = None
            try:
                await self.log.adebug(
                    event="发送创建多个模拟任务请求",
                    emoji="📤",
                    task_ids=task_ids,
                )
                success, progress_id, retry_after = (
                    await self._client.simulation_create_multi(payload=payload)
                )

                if not success or not progress_id:
                    await self.log.aerror(
                        event="创建多个模拟任务失败",
                        emoji="❌",
                        task_ids=task_ids,
                        progress_id=progress_id,
                    )
                    # 标记任务失败
                    async with (
                        session_manager.get_session(Database.SIMULATION) as session,
                        session.begin(),
                    ):
                        for task in tasks:
                            task.status = SimulationTaskStatus.ERROR
                            task.completed_at = datetime.now()
                        await simulation_task_dal.update_all(
                            entities=tasks, session=session
                        )
                    return

                await self.log.ainfo(
                    event="创建多个模拟任务成功，等待首次进度检查",
                    emoji="✅",
                    task_ids=task_ids,
                    progress_id=progress_id,
                    retry_after=f"{retry_after}s",
                )

                # 更新任务状态为运行中，并保存父进度 ID
                async with (
                    session_manager.get_session(Database.SIMULATION) as session,
                    session.begin(),
                ):
                    for task in tasks:
                        task.status = SimulationTaskStatus.RUNNING
                        task.parent_progress_id = progress_id
                    await simulation_task_dal.update_all(
                        entities=tasks, session=session
                    )
                    await self.log.ainfo(
                        event="数据库中多个任务状态更新为运行中",
                        emoji="💾",
                        task_ids=task_ids,
                        progress_id=progress_id,
                    )

                await asyncio.sleep(retry_after)

                prev_progress: float = -1.0
                while True:
                    #! 5. 心跳检查
                    await self._heartbeat(name=f"multi_task_poll_{progress_id}")
                    await self.log.adebug(
                        event="检查多个任务进度",
                        emoji="🔍",
                        task_ids=task_ids,
                        progress_id=progress_id,
                    )
                    if await self._cancel_task_if_possible(progress_id, tasks=tasks):
                        await self.log.ainfo(
                            event="多个任务已被取消，停止进度检查",
                            emoji="🚫",
                            task_ids=task_ids,
                            progress_id=progress_id,
                        )
                        break  # 任务已取消，退出循环

                    finished, progress_or_result, retry_after = (
                        await self._client.simulation_get_progress_multi(
                            progress_id=progress_id
                        )
                    )

                    if finished:
                        if isinstance(progress_or_result, MultiSimulationResultView):
                            await self.log.ainfo(
                                event="多个模拟任务集合完成",
                                emoji="🏁",
                                task_ids=task_ids,
                                progress_id=progress_id,
                                result_status=progress_or_result.status,
                            )
                            await self._handle_multi_task_finish(
                                tasks, progress_or_result
                            )
                        else:
                            await self.log.aerror(
                                event="多个任务完成但结果类型不匹配",
                                emoji="❓",
                                task_ids=task_ids,
                                progress_id=progress_id,
                                expected_type="MultiSimulationResultView",
                                received_type=type(progress_or_result).__name__,
                                received_value=progress_or_result,
                            )
                            # 标记任务失败
                            async with (
                                session_manager.get_session(
                                    Database.SIMULATION
                                ) as session,
                                session.begin(),
                            ):
                                for task in tasks:
                                    task.status = SimulationTaskStatus.ERROR
                                    task.completed_at = datetime.now()
                                await simulation_task_dal.update_all(
                                    entities=tasks, session=session
                                )
                        break  # 任务完成，退出循环

                    elif isinstance(progress_or_result, SimulationProgressView):
                        progress: float = progress_or_result.progress
                        if abs(progress - prev_progress) > 1e-6:
                            await self.log.ainfo(
                                event="多个模拟任务进行中",
                                emoji="⏳",
                                task_ids=task_ids,
                                progress_id=progress_id,
                                progress=f"{progress * 100:.2f}%",
                            )
                            prev_progress = progress
                        else:
                            await self.log.adebug(
                                event="多个任务进度未变化",
                                emoji="🧘",
                                task_ids=task_ids,
                                progress_id=progress_id,
                                progress=f"{progress * 100:.2f}%",
                            )
                    else:
                        await self.log.aerror(
                            event="获取多个任务进度时返回未知组合",
                            emoji="❓",
                            task_ids=task_ids,
                            progress_id=progress_id,
                            finished=finished,
                            progress_or_result_type=type(progress_or_result).__name__,
                            progress_or_result=progress_or_result,
                            retry_after=retry_after,
                        )

                    await self.log.adebug(
                        event="等待下次多个任务进度检查",
                        emoji="😴",
                        task_ids=task_ids,
                        progress_id=progress_id,
                        retry_after=f"{retry_after}s",
                    )
                    await asyncio.sleep(retry_after)
            except Exception:
                await self.log.aexception(
                    event="处理多个模拟任务时发生异常",
                    emoji="💥",
                    task_ids=task_ids,
                    progress_id=progress_id,
                )
                # 标记任务失败
                async with (
                    session_manager.get_session(Database.SIMULATION) as session,
                    session.begin(),
                ):
                    for task in tasks:
                        task.status = SimulationTaskStatus.ERROR
                        task.completed_at = datetime.now()
                    await simulation_task_dal.update_all(
                        entities=tasks, session=session
                    )
            finally:
                final_statuses = {t.id: t.status.value for t in tasks}
                await self.log.ainfo(
                    event="多个模拟任务处理结束",
                    emoji="🔚",
                    task_ids=task_ids,
                    progress_id=progress_id,
                    final_statuses=final_statuses,
                )

    async def _do_work(self) -> None:

        await self.log.ainfo(event="工作者开始执行工作循环", emoji="🔄")
        #! 2. 心跳检查
        await self._heartbeat(name="_do_work_start")
        while not self._shutdown_flag:
            await self.log.adebug(event="开始新的工作循环迭代", emoji="➡️")
            # 验证调度器是否已设置
            if self._scheduler is None:
                await self.log.acritical(
                    event="调度器未设置，工作者无法继续执行",
                    emoji="🚨",
                )
                # 抛出异常会导致工作者停止，符合 CRITICAL 级别定义
                raise Exception("调度器未设置，无法执行工作")

            # 根据用户角色确定任务批量大小
            scheduled_task_count: int = (
                MAX_CONSULTANT_SIMULATION_SLOTS
                if self._user_role == UserRole.CONSULTANT
                else 1
            )
            await self.log.adebug(
                event="确定调度任务数量",
                emoji="🔢",
                user_role=self._user_role.value,
                batch_size=scheduled_task_count,
            )

            # 从调度器获取任务
            try:
                await self.log.adebug(event="尝试从调度器获取任务", emoji="📥")
                tasks: List[SimulationTask] = await self._scheduler.schedule(
                    batch_size=scheduled_task_count
                )
                self._current_tasks = tasks  # 保存当前处理的任务列表
                await self.log.adebug(
                    event="从调度器获取任务成功",
                    emoji="✅",
                    task_count=len(tasks),
                    task_ids=[t.id for t in tasks],
                )
            except Exception:
                await self.log.aexception(
                    event="从调度器获取任务时发生异常",
                    emoji="💥",
                    batch_size=scheduled_task_count,
                )
                await asyncio.sleep(5)  # 发生异常时等待一段时间再重试
                continue  # 继续下一次循环

            # 如果没有可用任务，等待后重试
            if not tasks:
                #! 心跳检查，防止长时间无响应误判
                await self._heartbeat(name="_fetch_tasks_no_tasks")
                await self.log.ainfo(
                    event="调度器未返回任务，等待重试",
                    emoji="⏳",
                    retry_delay=5,
                )
                self._current_tasks = []  # 清空当前任务列表
                await asyncio.sleep(5)
                continue  # 继续下一次循环

            await self.log.ainfo(
                event="开始处理调度到的任务",
                emoji="⚙️",
                task_count=len(tasks),
                task_ids=[t.id for t in tasks],
            )

            #! 3. 心跳检查
            await self._heartbeat(name="_do_work_before_process")

            # 根据用户角色和任务数量执行不同的任务处理逻辑
            try:
                if self._user_role == UserRole.USER or len(tasks) == 1:
                    if len(tasks) != 1:
                        await self.log.aerror(
                            event="用户角色调度到多个任务",
                            emoji="❗",
                            user_role=self._user_role.value,
                            task_count=len(tasks),
                            task_ids=[t.id for t in tasks],
                        )
                        # 处理第一个任务，或标记全部错误
                    await self._process_single_simulation_task(tasks[0])
                elif self._user_role == UserRole.CONSULTANT:
                    await self._process_multi_simulation_task(tasks)
                else:
                    # 这是一个严重错误，因为角色应该在启动时验证
                    await self.log.acritical(
                        event="遇到未知用户角色，无法处理任务",
                        emoji="🚨",
                        user_role=self._user_role,
                        task_ids=[t.id for t in tasks],
                    )
                    # 抛出异常停止工作者
                    raise ValueError(f"未知用户角色 {self._user_role}，无法处理任务")
            except Exception:
                # 捕获任务处理过程中未被捕获的异常
                await self.log.aexception(
                    event="任务处理过程中发生未捕获异常",
                    emoji="💥",
                    user_role=self._user_role.value,
                    task_ids=[t.id for t in tasks],
                )
                # 异常已记录，循环继续

            self._current_tasks = []  # 清空当前处理的任务列表
            await self.log.adebug(event="当前批次任务处理完成", emoji="🏁")

        await self.log.ainfo(event="工作者工作循环正常结束", emoji="🚪")

    async def set_scheduler(self, scheduler: AbstractScheduler) -> None:

        await self.log.ainfo(
            event="设置新的任务调度器",
            emoji="🔧",
            new_scheduler=repr(scheduler),
            previous_scheduler=repr(self._scheduler),
        )
        self._scheduler = scheduler

    async def run(self) -> None:

        await self.log.ainfo(event="尝试启动工作者", emoji="▶️")
        if self._running:
            await self.log.awarning(event="工作者已在运行中，忽略启动请求", emoji="⚠️")
            return

        async with self._run_lock:  # 使用锁确保 run 方法不并发执行
            if self._running:  # 再次检查，防止锁等待期间状态变化
                await self.log.awarning(event="工作者在获取锁后发现已在运行", emoji="⚠️")
                return
            self._running = True  # 在锁内设置运行状态

            await self.log.ainfo(event="工作者成功获取运行锁并启动", emoji="🚀")

            #! 1. 心跳检查
            await self._heartbeat(name="run_start")

            self._shutdown_flag = False  # 重置关闭标志
            self._is_task_cancel_requested = False  # 重置取消请求标志

            try:
                # 验证客户端认证和角色
                async with self._client:
                    if not self._client.authentication_info:
                        await self.log.acritical(
                            event="客户端未认证，无法启动工作者", emoji="🚨"
                        )
                        raise ValueError("客户端必须经过有效凭证认证。")

                    if ROLE_CONSULTANT in self._client.authentication_info.permissions:
                        self._user_role = UserRole.CONSULTANT
                    elif ROLE_USER in self._client.authentication_info.permissions:
                        self._user_role = UserRole.USER
                    else:
                        await self.log.acritical(
                            event="客户端无有效用户角色，无法启动工作者",
                            emoji="🚨",
                            permissions=self._client.authentication_info.permissions,
                        )
                        raise ValueError(
                            "客户端必须具有有效的用户角色 (CONSULTANT 或 USER)。"
                        )
                await self.log.ainfo(
                    event="客户端认证和角色验证通过",
                    emoji="✅",
                    user_role=self._user_role.value,
                )

                # 启动主工作循环
                await self._do_work()

            except Exception:
                # 捕获 run 方法中的意外错误
                await self.log.aexception(
                    event="工作者运行期间发生未捕获异常", emoji="💥"
                )
            finally:
                self._running = False  # 确保运行状态在退出时重置
                await self.log.ainfo(
                    event="工作者运行结束",
                    emoji="🏁",
                    shutdown_flag=self._shutdown_flag,
                )
        # 锁在此处自动释放

    async def stop(self, cancel_tasks: bool = False) -> None:

        await self.log.ainfo(
            event="收到停止工作者请求",
            emoji="🛑",
            cancel_tasks=cancel_tasks,
            running=self._running,
            shutdown_flag=self._shutdown_flag,
            post_handler_futures=len(self._post_handler_futures),
        )

        if self._shutdown_flag:
            await self.log.awarning(event="工作者已在停止过程中", emoji="⚠️")
            # 可以考虑是否需要等待之前的停止完成或直接返回
            return

        self._shutdown_flag = True
        self._is_task_cancel_requested = cancel_tasks

        # 尝试中断主循环 (如果正在运行)
        # 注意：这依赖于 _do_work 循环检查 _shutdown_flag
        # 如果 _do_work 卡在某个长时间操作（如网络请求），可能不会立即停止
        await self.log.adebug(event="关闭标志已设置", emoji="🚩")

        # 等待挂起的任务完成（例如 _handle_multi_task_completion 创建的任务）
        if self._post_handler_futures:
            await self.log.ainfo(
                event="开始等待挂起的任务完成",
                emoji="⏳",
                task_count=len(self._post_handler_futures),
                timeout=30,
            )
            try:
                # 使用 asyncio.gather 等待所有任务，设置超时
                done, pending = await asyncio.wait(
                    self._post_handler_futures,
                    timeout=30,
                    return_when=asyncio.ALL_COMPLETED,
                )
                if pending:
                    await self.log.awarning(
                        event="等待挂起任务超时",
                        emoji="⌛",
                        timeout=30,
                        completed_count=len(done),
                        pending_count=len(pending),
                    )
                    # 可以考虑取消挂起的任务
                    for task in pending:
                        task.cancel()
                else:
                    await self.log.ainfo(
                        event="所有挂起任务已完成",
                        emoji="✅",
                        task_count=len(done),
                    )
                # 检查任务结果是否有异常
                for future in done:
                    try:
                        future.result()  # 获取结果以触发可能的异常
                    except asyncio.CancelledError:
                        await self.log.awarning(
                            event="挂起的任务被取消",
                            emoji="🚫",
                            task_name=future.get_name(),
                        )
                    except Exception:
                        await self.log.aexception(
                            event="挂起的任务在完成时抛出异常",
                            emoji="💥",
                            task_name=future.get_name(),
                        )

            except Exception:
                await self.log.aexception(
                    event="等待挂起任务时发生意外错误",
                    emoji="💥",
                )
            finally:
                self._post_handler_futures.clear()  # 清理列表
        else:
            await self.log.ainfo(event="没有需要等待的挂起任务", emoji="👍")

        # 确保运行锁最终被释放 (如果 run 方法仍在运行)
        # 这部分逻辑比较复杂，因为 run 方法可能仍在 _do_work 中循环
        # 一个更健壮的方法是让 run 方法在退出时自行释放锁
        # 这里的尝试获取锁更多是为了日志记录和确认状态
        if self._run_lock.locked():
            await self.log.adebug(
                event="运行锁当前被持有，等待释放",
                emoji="🔒",
            )
            # 不在此处强制获取锁，让 run 方法自然退出并释放
            # 可以设置一个超时等待 run 方法结束
            try:
                # 等待 run 方法结束，这需要 run 方法能响应 shutdown_flag
                # 注意：如果 run 卡住，这里也会卡住
                # 可以考虑给 run 方法本身加入超时机制或更复杂的取消逻辑
                pass  # 暂时不加复杂等待逻辑
            except asyncio.TimeoutError:
                await self.log.aerror(
                    event="等待 run 方法结束超时",
                    emoji="⌛",
                )
        else:
            await self.log.adebug(
                event="运行锁未被持有",
                emoji="🔓",
            )

        # 最终确认状态
        self._running = False  # 强制设置运行状态为 False
        await self.log.ainfo(
            event="工作者停止流程完成",
            emoji="🏁",
            shutdown_flag=self._shutdown_flag,
            running=self._running,
        )

    async def add_task_complete_callback(
        self,
        callback: Union[
            Callable[[SimulationTask, SingleSimulationResultView], None],
            Callable[[SimulationTask, SingleSimulationResultView], Awaitable[None]],
        ],
    ) -> None:

        callback_name = getattr(callback, "__name__", repr(callback))
        await self.log.adebug(
            event="尝试添加任务完成回调函数",
            emoji="➕",
            callback_name=callback_name,
            is_async=asyncio.iscoroutinefunction(callback),
            running=self._running,
        )
        # 允许在运行时添加回调，但需注意线程安全（此处是异步环境，主要关注并发访问列表）
        # Python 列表的 append 是线程安全的，但在异步环境中，如果回调列表在迭代时被修改，可能出问题
        # 但此处的添加操作相对简单，风险较低
        # if self._running:
        #     await self.log.awarning(
        #         event="工作者正在运行中添加回调函数，请注意并发风险",
        #         emoji="⚠️",
        #         callback_name=callback_name,
        #     )
        self._task_complete_callbacks.append(callback)
        await self.log.ainfo(
            event="任务完成回调函数添加成功",
            emoji="✅",
            callback_name=callback_name,
            total_callbacks=len(self._task_complete_callbacks),
        )

    async def get_current_tasks(self) -> List[SimulationTask]:

        await self.log.adebug(
            event="获取当前正在处理的任务列表",
            emoji="📋",
            task_count=len(self._current_tasks),
        )
        # 返回列表的副本以防止外部修改内部状态
        return list(self._current_tasks)

    async def add_heartbeat_callback(
        self,
        callback: Union[
            Callable[["AbstractWorker"], None],
            Callable[["AbstractWorker"], Awaitable[None]],
        ],
    ) -> None:

        callback_name = getattr(callback, "__name__", repr(callback))
        await self.log.adebug(
            event="尝试添加心跳回调函数",
            emoji="➕",
            callback_name=callback_name,
            is_async=asyncio.iscoroutinefunction(callback),
            running=self._running,
        )
        # 同样允许在运行时添加
        # if self._running:
        #     await self.log.awarning(
        #         event="工作者正在运行中添加心跳回调函数，请注意并发风险",
        #         emoji="⚠️",
        #         callback_name=callback_name,
        #     )
        self._heartbeat_callbacks.append(callback)
        await self.log.ainfo(
            event="心跳回调函数添加成功",
            emoji="✅",
            callback_name=callback_name,
            total_callbacks=len(self._heartbeat_callbacks),
        )
