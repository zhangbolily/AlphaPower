import asyncio
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from structlog.stdlib import BoundLogger

from alphapower.constants import Delay, InstrumentType, LoggingEmoji, Region, Universe
from alphapower.entity.data import DataField
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import get_logger
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
    process_async_runner,
)
from alphapower.manager.data_sets_manager import DataSetsManagerFactory
from alphapower.manager.data_sets_manager_abc import AbstractDataSetsManager
from alphapower.manager.options_manager import OptionsManagerFactory
from alphapower.manager.options_manager_abc import AbstractOptionsManager
from alphapower.view.data import DataCategoryView, DataFieldView, DatasetView
from alphapower.view.options import AlphasOptions, SimulationsOptions

from .datasets_abc import AbstractDatasetsService


@async_exception_handler
async def sync_data_fields_process_runner(
    data_sets: List[Tuple[InstrumentType, DatasetView]],
    data_sets_manager_factory: DataSetsManagerFactory,
) -> None:
    logger: BoundLogger = get_logger(sync_data_fields_process_runner.__qualname__)

    await logger.ainfo(
        event=f"进入 {sync_data_fields_process_runner.__qualname__}",
        emoji=LoggingEmoji.STEP_IN_FUNC.value,
    )

    data_sets_manager: AbstractDataSetsManager = await data_sets_manager_factory()
    await logger.ainfo(
        event=f"获取 {AbstractDataSetsManager.__name__} 实例成功",
        message=(
            f"使用的 {AbstractDataSetsManager.__name__} 工厂为 {data_sets_manager_factory.__class__.__name__}"
        ),
        emoji=LoggingEmoji.SUCCESS.value,
    )

    for instrument_type, data_set in data_sets:
        await logger.ainfo(
            event=f"处理数据集: {data_set.name}",
            emoji=LoggingEmoji.INFO.value,
        )

        all_data_field_views: List[DataFieldView] = []
        offset: int = 0

        while True:
            data_field_views: List[DataFieldView] = (
                await data_sets_manager.fetch_data_fields_from_platform(
                    dataset_id=data_set.id,
                    instrument_type=instrument_type,
                    region=data_set.region,
                    universe=data_set.universe,
                    delay=data_set.delay,
                    limit=50,
                    offset=offset,
                )
            )

            await logger.ainfo(
                event=f"获取到 {len(data_field_views)} 个数据字段",
                emoji=LoggingEmoji.INFO.value,
            )

            if not data_field_views or len(data_field_views) < 50:
                break

            all_data_field_views.extend(data_field_views)
            offset += 50

        data_fields: List[DataField] = (
            await data_sets_manager.build_data_field_entities_from_views(
                data_field_views=all_data_field_views,
            )
        )
        await logger.ainfo(
            event=f"成功构建 {len(data_fields)} 个数据字段实体",
            emoji=LoggingEmoji.SUCCESS.value,
        )

        await data_sets_manager.bulk_save_data_fields_to_db(data_fields=data_fields)

        await logger.ainfo(
            event=f"成功保存 {len(data_fields)} 个数据字段实体到数据库",
            emoji=LoggingEmoji.SUCCESS.value,
        )

    await logger.ainfo(
        event=f"退出 {sync_data_fields_process_runner.__qualname__}",
        emoji=LoggingEmoji.STEP_OUT_FUNC.value,
    )


class DatasetsService(AbstractDatasetsService, BaseProcessSafeClass):
    def __init__(
        self,
        datasets_manager: AbstractDataSetsManager,
        options_manager: AbstractOptionsManager,
    ) -> None:
        self._datasets_manager = datasets_manager
        self._options_manager = options_manager

    async def sync_datasets(
        self,
        data_sets_manager_factory: DataSetsManagerFactory,
        category: Optional[str] = None,
        delay: Optional[Delay] = None,
        instrument_type: Optional[InstrumentType] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        parallel: int = 1,
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

        await self._datasets_manager.bulk_save_categories_to_db(categories=categories)
        await self.log.ainfo(
            event=f"成功保存 {len(categories)} 个数据类别到数据库",
            emoji=LoggingEmoji.SUCCESS.value,
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

        all_data_sets: List[Tuple[InstrumentType, DatasetView]] = []
        for instrument_type, region, delay, universe in combinations:
            await self.log.ainfo(
                event=f"处理 {instrument_type} - {region} - {delay} - {universe}",
                emoji=LoggingEmoji.INFO.value,
            )

            data_sets: List[DatasetView] = (
                await self._datasets_manager.fetch_data_sets_from_platform(
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
                event=f"成功获取 {len(data_sets)} 个数据集",
                message=f"数据集: {[data_set.name for data_set in data_sets]}",
                emoji=LoggingEmoji.INFO.value,
            )

            await self._datasets_manager.bulk_save_data_sets_to_db(data_sets=data_sets)

            await self.log.ainfo(
                event=f"成功保存 {len(data_sets)} 个数据集到数据库",
                emoji=LoggingEmoji.SUCCESS.value,
            )

            all_data_sets.extend(
                [(instrument_type, data_set) for data_set in data_sets]
            )

            await self.log.ainfo(
                event=f"获取到 {len(data_sets)} 个数据集",
                message=f"数据集: {[data_set.name for data_set in data_sets]}",
                emoji=LoggingEmoji.INFO.value,
            )

        # 将 all_data_sets 均匀分成 parallel 个组别
        data_sets_groups: List[List[Tuple[InstrumentType, DatasetView]]] = [
            [] for _ in range(parallel)
        ]
        for idx, item in enumerate(all_data_sets):
            data_sets_groups[idx % parallel].append(item)

        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            tasks = [ 
                loop.run_in_executor(
                    executor,
                    process_async_runner,
                    sync_data_fields_process_runner,
                    group,
                    data_sets_manager_factory,
                )
                for group in data_sets_groups
            ]
            await asyncio.gather(*tasks)

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
        datasets_manager_factory: DataSetsManagerFactory,
        options_manager_factory: OptionsManagerFactory,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the factory with a DatasetsManager instance.
        """
        super().__init__(**kwargs)
        self._datasets_manager: Optional[AbstractDataSetsManager] = None
        self._datasets_manager_factory: DataSetsManagerFactory = (
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
                event=f"{AbstractDataSetsManager.__name__} 未初始化",
                message=(
                    f"{AbstractDataSetsManager.__name__} 依赖未注入，"
                    f"无法创建 {AbstractDatasetsService.__name__} 实例"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{AbstractDataSetsManager.__name__} 未初始化")

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
                f"使用的 {AbstractDataSetsManager.__name__} 工厂为 {self._datasets_manager_factory.__class__.__name__}"
            ),
            emoji=LoggingEmoji.SUCCESS.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__}",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return service
