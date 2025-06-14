from typing import Any, Dict, List, Optional, Tuple

from alphapower.client.worldquant_brain_client import WorldQuantBrainClientFactory
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import Delay, InstrumentType, LoggingEmoji, Region, Universe
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.multiprocessing import (
    BaseProcessSafeClass,
    BaseProcessSafeFactory,
)
from alphapower.view.options import (
    AlphasOptions,
    SimulationsOptions,
    SimulationsOptionsSettings,
)

from .options_manager_abc import AbstractOptionsManager


class OptionsManager(AbstractOptionsManager, BaseProcessSafeClass):
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
    async def fetch_alphas_options_from_platform(
        self, user_id: str, **kwargs: Any
    ) -> AlphasOptions:
        await self.log.ainfo(
            event=f"获取 {user_id} 的 AlphasOptions",
            message=f"进入 {self.fetch_alphas_options_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        alphas_options: AlphasOptions = await brain_client.fetch_alphas_options(
            user_id=user_id, **kwargs
        )
        await self.log.ainfo(
            event="成功获取 AlphasOptions",
            message=f"退出 {self.fetch_alphas_options_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return alphas_options

    @async_exception_handler
    async def fetch_simulations_options_from_platform(
        self, **kwargs: Any
    ) -> SimulationsOptions:
        await self.log.ainfo(
            event="获取 SimulationsOptions",
            message=f"进入 {self.fetch_simulations_options_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        brain_client: AbstractWorldQuantBrainClient = await self.brain_client()
        simulations_options: SimulationsOptions = (
            await brain_client.fetch_simulations_options(**kwargs)
        )
        await self.log.ainfo(
            event="成功获取 SimulationsOptions",
            message=f"退出 {self.fetch_simulations_options_from_platform.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return simulations_options

    @async_exception_handler
    async def simulations_options_settings(
        self,
        simulations_options: SimulationsOptions,
        **kwargs: Any,
    ) -> SimulationsOptionsSettings:
        # 进入方法日志
        await self.log.ainfo(
            event=f"生成 {SimulationsOptions.__name__} 设置",
            message=(f"进入 {self.simulations_options_settings.__qualname__} 方法，"),
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        # 校验 settings 是否存在
        settings_children: Any = getattr(
            getattr(getattr(simulations_options, "actions", None), "POST", None),
            "settings",
            None,
        )
        children: Dict[str, Any] = getattr(settings_children, "children", {})
        if not children:
            await self.log.aerror(
                event=f"{SimulationsOptions.__name__} 中缺少 settings",
                message=(
                    f"无法生成设置，settings 为空，"
                    f"方法名 {self.simulations_options_settings.__qualname__}，"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"{SimulationsOptions.__name__} 中缺少 settings")

        # 解析 settings
        try:
            simulations_settings: SimulationsOptionsSettings = (
                SimulationsOptionsSettings.model_validate(
                    children,
                    by_alias=True,
                )
            )
        except Exception as exc:
            await self.log.aerror(
                event=f"{SimulationsOptionsSettings.__name__} 解析失败",
                message=(
                    f"model_validate 失败，异常信息: {exc}，"
                    f"方法名 {self.simulations_options_settings.__qualname__}，"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise

        # 成功日志
        await self.log.ainfo(
            event=f"成功生成 {SimulationsOptions.__name__} 设置",
            message=(f"退出 {self.simulations_options_settings.__qualname__} 方法，"),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return simulations_settings

    @async_exception_handler
    async def region_delays(
        self,
        simulations_options: SimulationsOptions,
        instrument_type: InstrumentType,
        region: Region,
        **kwargs: Any,
    ) -> List[Delay]:
        # 进入方法日志
        await self.log.ainfo(
            event="生成 Region Delays",
            message=(
                f"进入 {self.region_delays.__qualname__} 方法，"
                f"instrument_type={instrument_type.value} region={region.value}"
            ),
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        # 获取 settings
        try:
            simulations_settings: SimulationsOptionsSettings = (
                await self.simulations_options_settings(
                    simulations_options=simulations_options
                )
            )
        except Exception as exc:
            await self.log.aerror(
                event="SimulationsOptionsSettings 获取失败",
                message=(
                    f"simulations_options_settings 异常: {exc}，"
                    f"方法名 {self.region_delays.__qualname__}"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise

        # 解析 instrumentType
        delay_choices: Dict[str, Any] = simulations_settings.delay.choices or {}
        instrument_types: Dict[str, Any] = delay_choices.get("instrumentType", {})
        if not instrument_types:
            await self.log.aerror(
                event="无效的 instrumentType 结构",
                message=(
                    f"instrumentType 为空，" f"方法名 {self.region_delays.__qualname__}"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("无效的 instrumentType 结构")

        instrument_type_dict: Optional[Dict[str, Any]] = instrument_types.get(
            instrument_type.value
        )
        if not instrument_type_dict:
            await self.log.aerror(
                event="无效的 instrumentType",
                message=(
                    f"instrumentType {instrument_type.value} 不存在，"
                    f"方法名 {self.region_delays.__qualname__}"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"无效的 instrumentType: {instrument_type.value}")

        # 解析 region
        regions: Dict[str, Any] = instrument_type_dict.get("region", {})
        if not regions:
            await self.log.aerror(
                event="无效的 region 结构",
                message=(f"region 为空，" f"方法名 {self.region_delays.__qualname__}"),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("无效的 region 结构")

        region_choices: Optional[List[Any]] = regions.get(region.value)
        if not region_choices:
            await self.log.aerror(
                event="无效的 region",
                message=(
                    f"region {region.value} 不存在，"
                    f"方法名 {self.region_delays.__qualname__}"
                ),
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"无效的 region: {region.value}")

        # 解析 delays
        delays: List[Delay] = []
        for choice in region_choices:
            if not isinstance(choice, dict):
                await self.log.aerror(
                    event="无效的 choice 结构",
                    message=(
                        f"choice 类型错误: {type(choice).__name__}，"
                        f"方法名 {self.region_delays.__qualname__}"
                    ),
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError(
                    f"Expected choice to be a dict, got {type(choice).__name__}"
                )
            delay_value: Optional[int] = choice.get("value")
            if delay_value is None:
                await self.log.aerror(
                    event="Delay 缺少 value 字段",
                    message=(
                        f"choice 缺少 value 字段，"
                        f"方法名 {self.region_delays.__qualname__}"
                    ),
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError("Delay 缺少 value 字段")
            delays.append(Delay(delay_value))

        # 成功日志
        await self.log.ainfo(
            event="成功生成 Region Delays",
            message=(
                f"退出 {self.region_delays.__qualname__} 方法，" f"delays={delays}"
            ),
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return delays

    @async_exception_handler
    async def universe_combinations(
        self,
        simulations_options: SimulationsOptions,
        **kwargs: Any,
    ) -> List[Tuple[InstrumentType, Region, Delay, Universe]]:
        await self.log.ainfo(
            event="生成 Universe 组合",
            message=f"进入 {self.universe_combinations.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        settings_dict: Dict[str, Any] = {}
        if not simulations_options.actions.POST.settings.children:
            await self.log.aerror(
                event="SimulationsOptions 中缺少 settings",
                message="无法生成 Universe 组合，settings 为空",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("SimulationsOptions 中缺少 settings")

        settings_dict = simulations_options.actions.POST.settings.children
        simulations_settings: SimulationsOptionsSettings = (
            SimulationsOptionsSettings.model_validate(
                settings_dict,
                by_alias=True,
            )
        )

        instrument_region_delay_universe_dict: Dict[str, Any] = (
            simulations_settings.universe.choices or {}
        )["instrumentType"] or {}

        async def _get_regions(data: Dict[str, Any]) -> Dict[Region, List[Any]]:
            """
            获取 regions 字典。
            """
            region_dict: Dict[str, Any] = data.get("region", {})
            if not isinstance(region_dict, dict):
                await self.log.aerror(
                    event="无效的 region 结构",
                    message=f"Expected regions to be a dict, got {type(region_dict).__name__}",
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError(
                    f"Expected regions to be a dict, got {type(region_dict).__name__}"
                )

            regions: Dict[Region, List[Any]] = {}
            for region, universe_list in region_dict.items():
                if not isinstance(universe_list, list):
                    await self.log.aerror(
                        event="无效的 universes 结构",
                        message=f"Expected universes to be a list, got {type(universe_list).__name__}",
                        emoji=LoggingEmoji.ERROR.value,
                    )
                    raise ValueError(
                        f"Expected universes to be a list, got {type(universe_list).__name__}"
                    )
                regions[Region(region)] = universe_list

            return regions

        async def _get_universes(
            regions: Dict[Region, List[Any]],
        ) -> Dict[Region, List[Universe]]:
            """
            获取 universes 列表。
            """
            universes: Dict[Region, List[Universe]] = {}
            for region, universe_list in regions.items():
                if not isinstance(universe_list, list):
                    await self.log.aerror(
                        event="无效的 universes 结构",
                        message=f"Expected universes to be a list, got {type(universe_list).__name__}",
                        emoji=LoggingEmoji.ERROR.value,
                    )
                    raise ValueError(
                        f"Expected universes to be a list, got {type(universe_list).__name__}"
                    )

                for universe in universe_list:
                    if not isinstance(universe, dict):
                        await self.log.aerror(
                            event="无效的 universe 结构",
                            message=f"Expected universe to be a dict, got {type(universe).__name__}",
                            emoji=LoggingEmoji.ERROR.value,
                        )
                        raise ValueError(
                            f"Expected universe to be a dict, got {type(universe).__name__}"
                        )
                    universes.setdefault(region, []).append(
                        Universe(universe.get("value", ""))
                    )

            return universes

        combinations: List[Tuple[InstrumentType, Region, Delay, Universe]] = []
        for instrument_type, value in instrument_region_delay_universe_dict.items():
            if not isinstance(value, dict):
                await self.log.aerror(
                    event="无效的 instrumentType 结构",
                    message=f"Expected value to be a dict, got {type(value).__name__}",
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError(
                    f"Expected value to be a dict, got {type(value).__name__}"
                )

            regions: Dict[Region, List[Any]] = await _get_regions(value)
            if not regions:
                await self.log.aerror(
                    event="无效的 regions 结构",
                    message="无法生成 Universe 组合，regions 为空",
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError("无效的 regions 结构")

            universes: Dict[Region, List[Universe]] = await _get_universes(regions)
            if not universes:
                await self.log.aerror(
                    event="无效的 universes 结构",
                    message="无法生成 Universe 组合，universes 为空",
                    emoji=LoggingEmoji.ERROR.value,
                )
                raise ValueError("无效的 universes 结构")

            for region, universe_list in universes.items():
                if not isinstance(universe_list, list):
                    await self.log.aerror(
                        event="无效的 universe_list 结构",
                        message=f"Expected universe_list to be a list, got {type(universe_list).__name__}",
                        emoji=LoggingEmoji.ERROR.value,
                    )
                    raise ValueError(
                        f"Expected universe_list to be a list, got {type(universe_list).__name__}"
                    )

                for universe in universe_list:
                    if not isinstance(universe, Universe):
                        await self.log.aerror(
                            event="无效的 universe 结构",
                            message=f"Expected universe to be a Universe instance, got {type(universe).__name__}",
                            emoji=LoggingEmoji.ERROR.value,
                        )
                        raise ValueError(
                            f"Expected universe to be a Universe instance, got {type(universe).__name__}"
                        )

                    delays: List[Delay] = await self.region_delays(
                        simulations_options=simulations_options,
                        instrument_type=InstrumentType(instrument_type),
                        region=region,
                        **kwargs,
                    )

                    if not delays:
                        await self.log.aerror(
                            event="Region Delays 为空",
                            message="无法生成 Universe 组合，Region Delays 为空",
                            emoji=LoggingEmoji.ERROR.value,
                        )
                        raise ValueError("Region Delays 为空")

                    for delay in delays:
                        combinations.append(
                            (
                                InstrumentType(instrument_type),
                                region,
                                delay,
                                universe,
                            )
                        )

        await self.log.ainfo(
            event="成功生成 Universe 组合",
            message=f"退出 {self.universe_combinations.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return combinations


class OptionsManagerFactory(BaseProcessSafeFactory[AbstractOptionsManager]):
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
    async def _build(self, *args: Any, **kwargs: Any) -> AbstractOptionsManager:
        await self.log.ainfo(
            event=f"进入 {self._build.__qualname__} 方法",
            message="开始构建 OptionsManager 实例",
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
                message="无法构建 OptionsManager 实例，缺少必要的客户端依赖",
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("WorldQuant Brain client is not set.")

        manager: AbstractOptionsManager = OptionsManager(
            brain_client=self._brain_client
        )
        await self.log.adebug(
            event=f"{self._build.__qualname__} 出参",
            message="OptionsManager 实例构建成功",
            manager_type=type(manager).__name__,
            emoji=LoggingEmoji.DEBUG.value,
        )
        await self.log.ainfo(
            event=f"退出 {self._build.__qualname__} 方法",
            message="完成 OptionsManager 实例构建",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return manager
