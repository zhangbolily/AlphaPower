from typing import Any, Dict, List, Optional

from alphapower.client.worldquant_brain_client import WorldQuantBrainClientFactory
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import LoggingEmoji
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.view.data import DataCategoryView, DatasetsQuery, DatasetView

from .datasets_manager_abc import AbstractDatasetsManager


class DatasetsManager(BaseProcessSafeClass, AbstractDatasetsManager):
    """
    Concrete implementation of DatasetManagerABC.
    This class manages datasets and data categories.
    """

    def __init__(self, brain_client: AbstractWorldQuantBrainClient) -> None:
        self._brain_client: Optional[AbstractWorldQuantBrainClient] = brain_client

    async def brain_client(self) -> AbstractWorldQuantBrainClient:
        await self.log.ainfo(
            event=f"获取 {AbstractWorldQuantBrainClient.__name__} 实例",
            message=f"进入 {self.brain_client.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        if self._brain_client is None:
            await self.log.aerror(
                event=f"{AbstractWorldQuantBrainClient.__name__} 实例未设置",
                message=f"{self.brain_client.__qualname__} 方法中发现未设置客户端",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractWorldQuantBrainClient.__name__} 实例未设置")
        await self.log.ainfo(
            event=f"获取 {AbstractWorldQuantBrainClient.__name__} 实例成功",
            message=f"退出 {self.brain_client.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return self._brain_client

    @async_exception_handler
    async def fetch_categories_from_platform(self) -> List[DataCategoryView]:
        await self.log.ainfo(
            event="开始从平台获取数据类别",
            message=f"进入 {self.fetch_categories_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        data_categories: List[DataCategoryView] = (
            await brain_client.fetch_data_categories()
        )
        await self.log.ainfo(
            event="成功获取数据类别",
            message=f"退出 {self.fetch_categories_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return data_categories

    @async_exception_handler
    async def fetch_datasets_from_platform(self, **kwargs: Any) -> List[DatasetView]:
        await self.log.ainfo(
            event="开始从平台获取数据集",
            message=f"进入 {self.fetch_datasets_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.ainfo(
            event="准备构建数据集查询对象",
            message=f"使用参数 {kwargs} 创建 DatasetsQuery 实例",
            emoji=LoggingEmoji.INFO.value,
        )

        query: DatasetsQuery = DatasetsQuery(**kwargs)

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        datasets: List[DatasetView] = await brain_client.fetch_datasets(query=query)
        await self.log.ainfo(
            event="成功获取数据集",
            message=f"退出 {self.fetch_datasets_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return datasets


class DatasetsManagerFactory(BaseProcessSafeFactory[AbstractDatasetsManager]):
    def __init__(
        self,
        brain_client_factory: WorldQuantBrainClientFactory,
        **kwargs: Any,
    ) -> None:
        """
        初始化工厂类。
        """
        super().__init__(**kwargs)
        self._brain_client: Optional[AbstractWorldQuantBrainClient] = None
        self._brain_client_factory: WorldQuantBrainClientFactory = brain_client_factory

    def __getstate__(self) -> Dict[str, Any]:
        state: Dict[str, Any] = super().__getstate__()
        state.pop("_brain_client", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._brain_client = None

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        """
        返回依赖的工厂列表。
        """
        factories: Dict[str, BaseProcessSafeFactory] = {
            "_brain_client": self._brain_client_factory
        }
        await self.log.adebug(
            event=f"{self._dependency_factories.__qualname__} 出参",
            message="依赖工厂列表生成成功",
            factories=list(factories.keys()),
            emoji=LoggingEmoji.DEBUG.value,
        )
        return factories

    @async_exception_handler
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractDatasetsManager:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__} 方法",
            message="开始构建 AbstractDatasetManager 实例",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        await self.log.adebug(
            event=f"{self._build.__qualname__} 入参",
            message="构建方法参数",
            args=args,
            kwargs=kwargs,
            emoji=LoggingEmoji.DEBUG.value,
        )

        if self._brain_client is None:
            await self.log.aerror(
                event="WorldQuant Brain client 未设置",
                message="无法构建 AbstractDatasetManager 实例，缺少必要的客户端依赖",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("WorldQuant Brain client is not set.")

        manager: AbstractDatasetsManager = DatasetsManager(
            brain_client=self._brain_client
        )
        await self.log.adebug(
            event=f"{self._build.__qualname__} 出参",
            message="AbstractDatasetManager 实例构建成功",
            manager_type=type(manager).__name__,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__} 方法",
            message="完成 AbstractDatasetManager 实例构建",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return manager
