from typing import Optional

from pydantic import AliasChoices, BaseModel, Field

from alphapower.constants import (
    AlphaType,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    SimulationResultStatus,
    Switch,
    UnitHandling,
    Universe,
)


class SimulationSettingsView(BaseModel):

    nan_handling: Optional[Switch] = Field(
        None,
        validation_alias=AliasChoices("nanHandling", "nan_handling"),
        serialization_alias="nanHandling",
    )
    instrument_type: Optional[InstrumentType] = Field(
        None,
        validation_alias=AliasChoices("instrumentType", "instrument_type"),
        serialization_alias="instrumentType",
    )
    delay: Optional[Delay] = Delay.DEFAULT
    universe: Optional[Universe] = None
    truncation: Optional[float] = None
    unit_handling: Optional[UnitHandling] = Field(
        None,
        validation_alias=AliasChoices("unitHandling", "unit_handling"),
        serialization_alias="unitHandling",
    )
    test_period: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("testPeriod", "test_period"),
        serialization_alias="testPeriod",
    )
    pasteurization: Optional[Switch] = None
    region: Optional[Region] = None
    language: Optional[RegularLanguage] = None
    decay: Optional[int] = None
    neutralization: Optional[Neutralization] = None
    visualization: Optional[bool] = None
    max_trade: Optional[Switch] = Field(
        None,
        validation_alias=AliasChoices("maxTrade", "max_trade"),
        serialization_alias="maxTrade",
    )


class SingleSimulationResultView(BaseModel):

    id: str
    type: AlphaType
    status: SimulationResultStatus
    message: Optional[str] = None
    location: Optional["SingleSimulationResultView.ErrorLocation"] = None
    settings: Optional[SimulationSettingsView] = None
    regular: Optional[str] = None
    alpha: Optional[str] = None
    parent: Optional[str] = None

    class ErrorLocation(BaseModel):

        line: Optional[int] = None
        start: Optional[int] = None
        end: Optional[int] = None
        property: Optional[str] = None
