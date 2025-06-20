from typing import Any, Dict, List, Optional

from alphapower.client.worldquant_brain_client import WorldQuantBrainClientFactory
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import (
    Database,
    Delay,
    InstrumentType,
    LoggingEmoji,
    Region,
    Universe,
)
from alphapower.dal import data_field_dal, data_set_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.data import DataField, DataSet
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.view.data import (
    DataCategoryView,
    DataFieldListQuery,
    DataFieldView,
    DatasetsQuery,
    DatasetView,
)

from .data_sets_manager_abc import AbstractDataSetsManager


class DataSetsManager(BaseProcessSafeClass, AbstractDataSetsManager):
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
    async def fetch_data_sets_from_platform(
        self,
        instrument_type: Optional[InstrumentType] = None,
        region: Optional[Region] = None,
        delay: Optional[Delay] = None,
        universe: Optional[Universe] = None,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        search: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> List[DatasetView]:
        await self.log.ainfo(
            event="开始从平台获取数据集",
            message=f"进入 {self.fetch_data_sets_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.ainfo(
            event="准备构建数据集查询对象",
            message=f"使用参数 {kwargs} 创建 DatasetsQuery 实例",
            emoji=LoggingEmoji.INFO.value,
        )

        query: DatasetsQuery = DatasetsQuery(
            instrument_type=instrument_type,
            region=region,
            delay=delay,
            universe=universe,
            category=category,
            subcategory=subcategory,
            search=search,
            limit=limit,
            offset=offset,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        datasets: List[DatasetView] = await brain_client.fetch_datasets(query=query)
        await self.log.ainfo(
            event="成功获取数据集",
            message=f"退出 {self.fetch_data_sets_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return datasets

    @async_exception_handler
    async def fetch_data_fields_from_platform(
        self,
        dataset_id: str,
        instrument_type: Optional[InstrumentType] = None,
        region: Optional[Region] = None,
        universe: Optional[Universe] = None,
        delay: Optional[Delay] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs: Any,
    ) -> List[DataFieldView]:
        await self.log.ainfo(
            event="开始从平台获取数据字段",
            message=f"进入 {self.fetch_data_fields_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        query: DataFieldListQuery = DataFieldListQuery(
            dataset_id=dataset_id,
            instrument_type=instrument_type,
            region=region,
            universe=universe,
            delay=delay,
            limit=limit,
            offset=offset,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        fields: List[DataFieldView] = await brain_client.fetch_data_fields(
            query=query,
        )
        await self.log.ainfo(
            event="成功获取数据字段",
            message=f"退出 {self.fetch_data_fields_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return fields

    @async_exception_handler
    async def build_data_field_entity_from_view(
        self,
        data_field_view: DataFieldView,
        **kwargs: Any,
    ) -> DataField:
        await self.log.ainfo(
            event="开始构建数据字段实体",
            message=f"进入 {self.build_data_field_entity_from_view.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        async with session_manager.get_session(
            db=Database.DATA,
            readonly=True,
        ) as session:
            data_set: Optional[DataSet] = await data_set_dal.find_one_by(
                session=session,
                data_set_id=data_field_view.dataset.id,
            )

            if data_set is None:
                await self.log.aerror(
                    event="数据集未找到",
                    message=f"数据集 ID {data_field_view.dataset.id} 在数据库中不存在",
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError(
                    f"DataSet with ID {data_field_view.dataset.id} not found."
                )

            data_field: DataField = DataField(
                region=data_field_view.region,
                delay=data_field_view.delay,
                universe=data_field_view.universe,
                type=data_field_view.type,
                field_id=data_field_view.id,
                description=data_field_view.description,
                dataset=data_set,
                coverage=data_field_view.coverage,
                user_count=data_field_view.user_count,
                alpha_count=data_field_view.alpha_count,
                pyramid_multiplier=data_field_view.pyramid_multiplier,
                category_id=data_field_view.category.id,
                subcategory_id=data_field_view.subcategory.id,
            )

        await self.log.ainfo(
            event="成功构建数据字段实体",
            message=f"退出 {self.build_data_field_entity_from_view.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return data_field

    @async_exception_handler
    async def build_data_field_entities_from_views(
        self,
        data_field_views: List[DataFieldView],
        **kwargs: Any,
    ) -> List[DataField]:
        await self.log.ainfo(
            event="开始构建数据字段实体列表",
            message=f"进入 {self.build_data_field_entities_from_views.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        data_fields: List[DataField] = []
        for view in data_field_views:
            data_field: DataField = await self.build_data_field_entity_from_view(
                data_field_view=view, **kwargs
            )
            data_fields.append(data_field)

        await self.log.ainfo(
            event="成功构建数据字段实体列表",
            message=f"退出 {self.build_data_field_entities_from_views.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return data_fields

    @async_exception_handler
    async def bulk_save_data_fields_to_db(
        self, data_fields: List[DataField], **kwargs: Any
    ) -> None:
        await self.log.ainfo(
            event="开始批量保存数据字段到数据库",
            message=f"进入 {self.bulk_save_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        async with (
            session_manager.get_session(db=Database.DATA) as session,
            session.begin(),
        ):
            await data_field_dal.bulk_upsert(
                entities=data_fields,
                session=session,
            )

        await self.log.ainfo(
            event="成功批量保存数据字段到数据库",
            message=f"退出 {self.bulk_save_data_fields_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )

    @async_exception_handler
    async def save_data_field_to_db(self, data_field: DataField, **kwargs: Any) -> None:
        await self.log.ainfo(
            event="开始保存数据字段到数据库",
            message=f"进入 {self.save_data_field_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.bulk_save_data_fields_to_db(
            data_fields=[data_field],
            **kwargs,
        )

        await self.log.ainfo(
            event="成功保存数据字段到数据库",
            message=f"退出 {self.save_data_field_to_db.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )


class DataSetsManagerFactory(BaseProcessSafeFactory[AbstractDataSetsManager]):
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
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractDataSetsManager:
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

        manager: AbstractDataSetsManager = DataSetsManager(
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
