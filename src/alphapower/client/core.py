"""
WorldQuant AlphaPower Client
"""

import asyncio
from contextlib import suppress
from typing import Any, Dict, Optional, Tuple, Union

from aiohttp import BasicAuth, ClientSession

from alphapower.constants import CorrelationType
from alphapower.internal.logging import get_logger
from alphapower.internal.wraps import exception_handler
from alphapower.settings import settings

from .checks_view import BeforeAndAfterPerformanceView, SubmissionCheckResultView
from .common_view import TableView
from .models import (
    AlphaDetailView,
    AlphaPropertiesPayload,
    AuthenticationView,
    CompetitionListView,
    DataCategoriesListView,
    DataFieldListView,
    DatasetDataFieldsView,
    DatasetDetailView,
    DatasetListView,
    DataSetsQueryParams,
    GetDataFieldsQueryParams,
    MultiSimulationPayload,
    MultiSimulationResultView,
    Operators,
    RateLimit,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
    SimulationProgressView,
    SingleSimulationPayload,
    SingleSimulationResultView,
)
from .raw_api import (
    alpha_fetch_before_and_after_performance,
    alpha_fetch_competitions,
    alpha_fetch_correlations,
    alpha_fetch_submission_check_result,
    authentication,
    create_multi_simulation,
    create_single_simulation,
    delete_simulation,
    fetch_data_categories,
    fetch_data_field_detail,
    fetch_dataset_data_fields,
    fetch_dataset_detail,
    fetch_datasets,
    get_all_operators,
    get_self_alphas,
    get_simulation_progress,
    set_alpha_properties,
)
from .utils import rate_limit_handler

logger = get_logger(__name__)


class WorldQuantClient:
    """
    WorldQuant 客户端类，用于与 AlphaPower API 交互。
    """

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:
        """
        初始化 WorldQuantClient 实例。

        参数:
        username (str): 用户名。
        password (str): 密码。
        """
        self._is_closed: bool = True
        self._auth: BasicAuth = BasicAuth(username, password)
        self.session: Optional[ClientSession] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._usage_count: int = 0
        self._usage_lock: asyncio.Lock = asyncio.Lock()
        self.authentication_info: Optional[AuthenticationView] = None
        logger.info("WorldQuantClient 实例已创建", emoji="🆕")

    async def _start_refresh_task(self, expiry: float) -> None:
        """
        后台异步循环刷新会话。
        """
        await logger.ainfo("启动会话刷新任务", expiry=expiry, emoji="🔄")
        self._refresh_task = asyncio.create_task(self._run_session_refresh_loop(expiry))

    async def _run_session_refresh_loop(self, expiry: float) -> None:
        """定期循环刷新认证会话"""
        while not self._is_closed:
            try:
                await self._wait_for_refresh_time(expiry)
                expiry = await self._perform_session_refresh()
            except asyncio.CancelledError:
                await logger.awarning("会话刷新任务被取消", emoji="⚠️")
                break
            except Exception as e:
                await logger.aerror("刷新会话时发生错误", error=str(e), emoji="❌")

    async def _wait_for_refresh_time(self, expiry: float) -> None:
        """等待直到接近会话过期时间"""
        refresh_interval = max(expiry - 60, 0)
        await logger.ainfo(
            "计划刷新会话", refresh_interval=refresh_interval, emoji="⏳"
        )
        await asyncio.sleep(refresh_interval)

    async def _perform_session_refresh(self) -> float:
        """执行实际的会话刷新操作并返回新的过期时间"""
        old_session = self.session
        await logger.ainfo("开始刷新会话", emoji="🔄")

        # 创建新会话并获取认证信息
        self.session = ClientSession(auth=self._auth)
        session_info = await authentication(self.session)
        self.authentication_info = session_info

        # 关闭旧会话
        if old_session:
            await old_session.close()
            await logger.ainfo("旧会话已关闭", emoji="🛑")

        expiry = session_info.token.expiry
        await logger.ainfo("会话刷新成功", new_expiry=expiry, emoji="✅")
        return expiry

    async def initialize(self) -> None:
        """
        初始化客户端会话。
        """
        await logger.ainfo("初始化客户端会话", emoji="🚀")
        self.session = ClientSession(auth=self._auth)
        self.authentication_info = await authentication(self.session)
        self._is_closed = False
        self._refresh_task = asyncio.create_task(
            self._start_refresh_task(self.authentication_info.token.expiry)
        )
        await logger.ainfo("客户端会话初始化完成", emoji="✅")

    async def close(self) -> None:
        """
        关闭客户端会话。
        """
        await logger.ainfo("关闭客户端会话", emoji="🛑")
        if self.session and not self.session.closed:
            await self.session.close()
            await logger.ainfo("会话已关闭", emoji="✅")
        self._is_closed = True
        if self._refresh_task:
            self._refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._refresh_task
            await logger.ainfo("刷新任务已取消", emoji="🛑")
        self._refresh_task = None

    async def _is_initialized(self) -> bool:
        """
        检查客户端是否已初始化。

        返回:
        bool: 如果已初始化，则返回 True，否则返回 False。
        """
        await logger.adebug(
            "检查客户端是否已初始化", is_closed=self._is_closed, emoji="🔍"
        )
        if self.session and self.session.closed:
            if self._is_closed:
                await logger.awarning("客户端未初始化", emoji="⚠️")
                return False
            else:
                await self.close()
                return False

        return not self._is_closed

    async def __aenter__(self) -> "WorldQuantClient":
        """
        异步上下文管理器的进入方法。

        返回:
        WorldQuantClient: 客户端实例。
        """
        async with self._usage_lock:
            await logger.adebug(
                "进入异步上下文管理器", usage_count=self._usage_count, emoji="🔑"
            )
            if self._usage_count == 0:
                if self._is_closed:
                    await self.initialize()
                if self.session is None:
                    raise RuntimeError("会话未初始化")
            self._usage_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[BaseException],
    ) -> None:
        """
        异步上下文管理器的退出方法。

        参数:
        exc_type (Optional[type]): 异常类型。
        exc_val (Optional[BaseException]): 异常值。
        exc_tb (Optional[BaseException]): 异常回溯。
        """
        async with self._usage_lock:
            self._usage_count -= 1
            await logger.adebug(
                "退出异步上下文管理器", usage_count=self._usage_count, emoji="🔑"
            )
            if self._usage_count == 0:
                await self.close()

    def __del__(self) -> None:
        """
        确保在对象销毁时清理资源。
        """
        logger.warning("WorldQuantClient 实例被销毁", emoji="🗑️")
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

    # -------------------------------
    # Simulation-related methods
    # -------------------------------
    @exception_handler
    async def simulation_create_single(
        self, payload: SingleSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建单次模拟。

        参数:
        payload (SingleSimulationPayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        success, progress_id, retry_after = await create_single_simulation(
            self.session, payload.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def simulation_create_multi(
        self, payload: MultiSimulationPayload
    ) -> tuple[bool, str, float]:
        """
        创建多次模拟。

        参数:
        payload (MultiSimulationPayload): 模拟数据。

        返回:
        tuple: 包含成功状态、进度 ID 和重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        success, progress_id, retry_after = await create_multi_simulation(
            self.session, payload.to_params()
        )
        return success, progress_id, retry_after

    @exception_handler
    async def simulation_delete(self, progress_id: str) -> bool:
        """
        删除模拟。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        bool: 删除是否成功。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        await delete_simulation(self.session, progress_id)
        return True

    @exception_handler
    async def simulation_get_progress_single(
        self, progress_id: str
    ) -> tuple[bool, Union[SingleSimulationResultView, SimulationProgressView], float]:
        """
        获取单次模拟的进度。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        tuple: 包含完成状态、进度或结果对象以及重试时间的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, retry_after = await get_simulation_progress(
            self.session, progress_id, is_multi=False
        )
        if not isinstance(
            progress_or_result,
            (SingleSimulationResultView, SimulationProgressView),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def simulation_get_progress_multi(
        self, progress_id: str
    ) -> tuple[bool, Union[MultiSimulationResultView, SimulationProgressView], float]:
        """
        获取多次模拟的进度。

        参数:
        progress_id (str): 模拟进度 ID。

        返回:
        tuple: 包含完成状态、进度或结果对象以及重试时间的元组。
        """

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, retry_after = await get_simulation_progress(
            self.session, progress_id, is_multi=True
        )
        if not isinstance(
            progress_or_result,
            (MultiSimulationResultView, SimulationProgressView),
        ):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result, retry_after

    @exception_handler
    async def simulation_get_child_result(
        self, child_progress_id: str
    ) -> tuple[bool, SingleSimulationResultView]:
        """
        获取多次模拟的子结果。

        参数:
        child_progress_id (str): 子模拟进度 ID。

        返回:
        tuple: 包含完成状态和单次模拟结果的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, progress_or_result, _ = await get_simulation_progress(
            self.session, child_progress_id, is_multi=False
        )
        if not isinstance(progress_or_result, SingleSimulationResultView):
            raise ValueError("模拟结果尚未准备好，或者进度 ID 无效")
        return finished, progress_or_result

    # -------------------------------
    # Alpha-related methods
    # -------------------------------
    @exception_handler
    @rate_limit_handler
    async def alpha_get_self_list(
        self, query: SelfAlphaListQueryParams
    ) -> tuple[SelfAlphaListView, RateLimit]:
        """
        获取用户的 Alpha 列表。

        参数:
        query (SelfAlphaListQueryParams): 查询参数。

        返回:
        tuple: 包含 Alpha 列表视图和速率限制信息的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await get_self_alphas(self.session, query.to_params())
        return resp

    @exception_handler
    @rate_limit_handler
    async def alpha_update_properties(
        self,
        alpha_id: str,
        properties: AlphaPropertiesPayload,
    ) -> Tuple[AlphaDetailView, RateLimit]:
        """
        更新 Alpha 属性。

        参数:
        alpha_id (str): Alpha ID。
        payload (dict): 属性数据。

        返回:
        bool: 更新是否成功。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await set_alpha_properties(self.session, alpha_id, properties)
        return resp

    @exception_handler
    async def alpha_fetch_competitions(
        self, params: Optional[Dict[str, Any]] = None
    ) -> CompetitionListView:
        """
        获取 Alpha 竞赛列表。

        返回:
        CompetitionListView: 竞赛列表视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await alpha_fetch_competitions(self.session, params=params)
        return resp

    @exception_handler
    @rate_limit_handler
    async def alpha_correlation_check(
        self, alpha_id: str, corr_type: CorrelationType
    ) -> Tuple[bool, Optional[float], Optional[TableView]]:
        """
        检查 Alpha 的相关性。

        参数:
        alpha_id (str): Alpha ID。
        corr_type (CorrelationType): 相关性类型。

        返回:
        bool: 检查是否成功。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result, _ = await alpha_fetch_correlations(
            self.session, alpha_id, corr_type
        )
        return finished, retry_after, result

    @exception_handler
    @rate_limit_handler
    async def alpha_fetch_before_and_after_performance(
        self, competition_id: Optional[str], alpha_id: str
    ) -> Tuple[
        bool, Optional[float], Optional[BeforeAndAfterPerformanceView], RateLimit
    ]:
        """
        获取 Alpha 的提交前后性能数据。

        参数:
        alpha_id (str): Alpha ID。

        返回:
        Tuple: 包含完成状态、重试时间、性能数据和速率限制信息的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result, rate_limit = (
            await alpha_fetch_before_and_after_performance(
                self.session, competition_id, alpha_id
            )
        )
        return finished, retry_after, result, rate_limit

    @exception_handler
    @rate_limit_handler
    async def alpha_fetch_submission_check_result(
        self, alpha_id: str
    ) -> Tuple[bool, Optional[float], Optional[SubmissionCheckResultView], RateLimit]:
        """
        获取 Alpha 的提交检查结果。

        参数:
        alpha_id (str): Alpha ID。

        返回:
        Tuple: 包含完成状态、重试时间、检查结果和速率限制信息的元组。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result, rate_limit = (
            await alpha_fetch_submission_check_result(self.session, alpha_id)
        )
        return finished, retry_after, result, rate_limit

    # -------------------------------
    # Data-related methods
    # -------------------------------
    @exception_handler
    async def data_get_categories(self) -> DataCategoriesListView:
        """
        获取数据类别。

        返回:
        DataCategoriesListView: 数据类别列表。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_data_categories(self.session)
        return resp

    @exception_handler
    @rate_limit_handler
    async def data_get_datasets(
        self, query: DataSetsQueryParams
    ) -> Optional[DatasetListView]:
        """
        获取数据集列表。

        参数:
        query (DataSetsQueryParams): 查询参数。

        返回:
        Optional[DatasetListView]: 数据集列表视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_datasets(self.session, query.to_params())
        return resp

    @exception_handler
    @rate_limit_handler
    async def data_get_dataset_detail(
        self, dataset_id: str
    ) -> Optional[DatasetDetailView]:
        """
        获取数据集详情。

        参数:
        dataset_id (str): 数据集 ID。

        返回:
        Optional[DatasetDetail]: 数据集详情。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_dataset_detail(self.session, dataset_id)
        return resp

    @exception_handler
    async def data_get_field_detail(self, data_field_id: str) -> DatasetDataFieldsView:
        """
        获取数据字段详情。

        参数:
        data_field_id (str): 数据字段 ID。

        返回:
        DatasetDataFieldsView: 数据字段详情视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_data_field_detail(self.session, data_field_id)
        return resp

    @exception_handler
    @rate_limit_handler
    async def data_get_fields_in_dataset(
        self, query: GetDataFieldsQueryParams
    ) -> Optional[DataFieldListView]:
        """
        获取数据集中包含的数据字段。

        参数:
        query (GetDataFieldsQueryParams): 查询参数。

        返回:
        Optional[DataFieldListView]: 数据字段列表视图。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_dataset_data_fields(self.session, query.to_params())
        return resp

    # -------------------------------
    # Other utility methods
    # -------------------------------
    @exception_handler
    async def operators_get_all(self) -> Operators:
        """
        获取所有操作符。

        返回:
        Operators: 操作符对象。
        """
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await get_all_operators(self.session)
        return resp


wq_client: WorldQuantClient = WorldQuantClient(
    username=settings.credential.username,
    password=settings.credential.password,
)
