import asyncio
from contextlib import suppress
from typing import Any, Dict, Optional, Tuple, Union

from aiohttp import BasicAuth, ClientSession

from alphapower.constants import CorrelationType
from alphapower.internal.logging import get_logger
from alphapower.internal.wraps import exception_handler
from alphapower.settings import settings
from alphapower.view.activities import (
    DiversityView,
    PyramidAlphasQuery,
    PyramidAlphasView,
)
from alphapower.view.alpha import (
    AlphaDetailView,
    SelfAlphaListQueryParams,
    SelfAlphaListView,
)

from .checks_view import BeforeAndAfterPerformanceView, SubmissionCheckResultView
from .common_view import TableView
from .models import (
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
    SimulationProgressView,
    SingleSimulationPayload,
    SingleSimulationResultView,
)
from .raw_api import (
    alpha_fetch_before_and_after_performance,
    alpha_fetch_competitions,
    alpha_fetch_correlations,
    alpha_fetch_record_set_pnl,
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
    user_fetch_diversity,
    user_fetch_pyramid_alphas,
)
from .utils import rate_limit_handler

logger = get_logger(__name__)


class WorldQuantClient:

    def __init__(
        self,
        username: str,
        password: str,
    ) -> None:

        self._is_closed: bool = True
        self._auth: BasicAuth = BasicAuth(username, password)
        self.session: Optional[ClientSession] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._usage_count: int = 0
        self._usage_lock: asyncio.Lock = asyncio.Lock()
        self.authentication_info: Optional[AuthenticationView] = None
        logger.info("WorldQuantClient 实例已创建", emoji="🆕")

    async def _start_refresh_task(self, expiry: float) -> None:

        await logger.ainfo("启动会话刷新任务", expiry=expiry, emoji="🔄")
        self._refresh_task = asyncio.create_task(self._run_session_refresh_loop(expiry))

    async def _run_session_refresh_loop(self, expiry: float) -> None:

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

        refresh_interval = max(expiry - 60, 0)
        await logger.ainfo(
            "计划刷新会话", refresh_interval=refresh_interval, emoji="⏳"
        )
        await asyncio.sleep(refresh_interval)

    async def _perform_session_refresh(self) -> float:

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

        await logger.ainfo("初始化客户端会话", emoji="🚀")
        self.session = ClientSession(auth=self._auth)
        self.authentication_info = await authentication(self.session)
        self._is_closed = False
        self._refresh_task = asyncio.create_task(
            self._start_refresh_task(self.authentication_info.token.expiry)
        )
        await logger.ainfo("客户端会话初始化完成", emoji="✅")

    async def close(self) -> None:

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

        async with self._usage_lock:
            self._usage_count -= 1
            await logger.adebug(
                "退出异步上下文管理器", usage_count=self._usage_count, emoji="🔑"
            )
            if self._usage_count == 0:
                await self.close()

    def __del__(self) -> None:

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

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result, _ = await alpha_fetch_correlations(
            self.session, alpha_id, corr_type
        )
        return finished, retry_after, result

    @exception_handler
    async def alpha_fetch_before_and_after_performance(
        self, competition_id: Optional[str], alpha_id: str
    ) -> Tuple[
        bool,
        Optional[float],
        Optional[BeforeAndAfterPerformanceView],
    ]:

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result = await alpha_fetch_before_and_after_performance(
            self.session, competition_id, alpha_id
        )
        return finished, retry_after, result

    @exception_handler
    @rate_limit_handler
    async def alpha_fetch_submission_check_result(
        self, alpha_id: str
    ) -> Tuple[bool, Optional[float], Optional[SubmissionCheckResultView], RateLimit]:

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, retry_after, result, rate_limit = (
            await alpha_fetch_submission_check_result(self.session, alpha_id)
        )
        return finished, retry_after, result, rate_limit

    @exception_handler
    @rate_limit_handler
    async def alpha_fetch_record_set_pnl(
        self, alpha_id: str
    ) -> Tuple[bool, Optional[TableView], float, RateLimit]:

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        finished, table, retry_after, rate_limit = await alpha_fetch_record_set_pnl(
            self.session, alpha_id
        )
        return finished, table, retry_after, rate_limit

    # -------------------------------
    # Data-related methods
    # -------------------------------
    @exception_handler
    async def data_get_categories(self) -> DataCategoriesListView:

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

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await fetch_dataset_detail(self.session, dataset_id)
        return resp

    @exception_handler
    async def data_get_field_detail(self, data_field_id: str) -> DatasetDataFieldsView:

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

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await get_all_operators(self.session)
        return resp

    @exception_handler
    async def user_fetch_pyramid_alphas(
        self,
        query: PyramidAlphasQuery,
    ) -> PyramidAlphasView:

        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")

        if self.session is None:
            raise RuntimeError("会话未初始化")

        resp = await user_fetch_pyramid_alphas(
            self.session,
            query.model_dump(
                mode="json",
                by_alias=True,
            ),
        )
        return resp

    @exception_handler
    async def user_fetch_diversity(
        self,
    ) -> DiversityView:
        if not await self._is_initialized():
            raise RuntimeError("客户端未初始化")
        if self.session is None:
            raise RuntimeError("会话未初始化")
        resp = await user_fetch_diversity(self.session)
        return resp


wq_client: WorldQuantClient = WorldQuantClient(
    username=settings.credential.username,
    password=settings.credential.password,
)
