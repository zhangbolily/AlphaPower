from typing import Any, Dict, List, Optional, Tuple

from alphapower.constants import Delay, InstrumentType, LoggingEmoji, Region, Universe
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.manager.datasets_manager import DatasetsManagerFactory
from alphapower.manager.datasets_manager_abc import AbstractDatasetsManager
from alphapower.manager.options_manager import OptionsManagerFactory
from alphapower.manager.options_manager_abc import AbstractOptionsManager
from alphapower.view.data import DataCategoryView, DatasetView
from alphapower.view.options import AlphasOptions, SimulationsOptions

from .datasets_abc import AbstractDatasetsService


class DatasetsService(AbstractDatasetsService, BaseProcessSafeClass):
    def __init__(
        self,
        datasets_manager: AbstractDatasetsManager,
        options_manager: AbstractOptionsManager,
    ) -> None:
        self._datasets_manager = datasets_manager
        self._options_manager = options_manager

    async def sync_datasets(
        self,
        category: Optional[str] = None,
        delay: Optional[Delay] = None,
        instrumentType: Optional[InstrumentType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        **kwargs: Any,
    ) -> None:
        await self.log.ainfo(
            event=f"进入 {self.sync_datasets.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        categories: List[DataCategoryView] = (
            await self._datasets_manager.fetch_categories_from_platform()
        )
        await self.log.ainfo(
            event=f"获取到 {len(categories)} 个数据类别",
            message=f"类别: {[category.name for category in categories]}",
            emoji=LoggingEmoji.INFO.value,
        )

        alphas_options: AlphasOptions = (
            await self._options_manager.fetch_alphas_options_from_platform(
                user_id="BZ71543"
            )
        )
        await self.log.ainfo(
            event=f"获取 AlphasOptions 成功",
            message=f"AlphasOptions: {alphas_options}",
            emoji=LoggingEmoji.INFO.value,
        )

        simulations_options: SimulationsOptions = (
            await self._options_manager.fetch_simulations_options_from_platform()
        )
        await self.log.ainfo(
            event=f"获取 SimulationsOptions 成功",
            message=f"SimulationsOptions: {simulations_options}",
            emoji=LoggingEmoji.INFO.value,
        )

        combinations: List[Tuple[InstrumentType, Region, Delay, Universe]] = (
            await self._options_manager.universe_combinations(
                simulations_options=simulations_options
            )
        )
        await self.log.ainfo(
            event=f"成功生成 Universe 组合",
            message=f"Universe 组合: {combinations}",
            emoji=LoggingEmoji.INFO.value,
        )

        for instrument_type, region, delay, universe in combinations:
            await self.log.ainfo(
                event=f"处理 {instrument_type} - {region} - {delay} - {universe}",
                emoji=LoggingEmoji.INFO.value,
            )

            datasets: List[DatasetView] = (
                await self._datasets_manager.fetch_datasets_from_platform(
                    category=category,
                    delay=delay,
                    instrumentType=instrument_type,
                    limit=limit,
                    offset=offset,
                    region=region,
                    universe=universe,
                )
            )
            await self.log.ainfo(
                event=f"获取到 {len(datasets)} 个数据集",
                message=f"数据集: {[dataset.name for dataset in datasets]}",
                emoji=LoggingEmoji.INFO.value,
            )

        await self.log.ainfo(
            event=f"退出 {self.sync_datasets.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )


class DatasetsServiceFactory(BaseProcessSafeFactory[AbstractDatasetsService]):
    """
    Factory class for creating DatasetsService instances.
    """

    def __init__(
        self,
        datasets_manager_factory: DatasetsManagerFactory,
        options_manager_factory: OptionsManagerFactory,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the factory with a DatasetsManager instance.
        """
        super().__init__(**kwargs)
        self._datasets_manager: Optional[AbstractDatasetsManager] = None
        self._datasets_manager_factory: DatasetsManagerFactory = (
            datasets_manager_factory
        )
        self._options_manager: Optional[AbstractOptionsManager] = None
        self._options_manager_factory: OptionsManagerFactory = options_manager_factory

    def __getstate__(self) -> Dict[str, Any]:
        state: Dict[str, Any] = super().__getstate__()
        state.pop("_datasets_manager", None)
        state.pop("_options_manager", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        self._datasets_manager = None
        self._options_manager = None

    async def _dependency_factories(self) -> Dict[str, BaseProcessSafeFactory]:
        await self.log.ainfo(
            event=f"进入 {self._dependency_factories.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )
        factories: Dict[str, BaseProcessSafeFactory[Any]] = {
            "_datasets_manager": self._datasets_manager_factory,
            "_options_manager": self._options_manager_factory,
        }
        await self.log.ainfo(
            event=f"退出 {self._dependency_factories.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return factories

    async def _build(self, *args: Any, **kwargs: Any) -> AbstractDatasetsService:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        if self._datasets_manager is None:
            await self.log.aerror(
                event=f"{AbstractDatasetsManager.__name__} 未初始化",
                message=(
                    f"{AbstractDatasetsManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractDatasetsService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractDatasetsManager.__name__} 未初始化")

        if self._options_manager is None:
            await self.log.aerror(
                event=f"{AbstractOptionsManager.__name__} 未初始化",
                message=(
                    f"{AbstractOptionsManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractDatasetsService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractOptionsManager.__name__} 未初始化")

        service: AbstractDatasetsService = DatasetsService(
            datasets_manager=self._datasets_manager,
            options_manager=self._options_manager,
        )

        await self.log.ainfo(
            event=f"{AbstractDatasetsService.__name__} 实例创建成功",
            message=(
                f"成功创建 {AbstractDatasetsService.__name__} 实例，"
                f"使用的 {AbstractDatasetsManager.__name__} 工厂为 {self._datasets_manager_factory.__class__.__name__}"
            ),
            emoji=LoggingEmoji.SUCCESS.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return service
