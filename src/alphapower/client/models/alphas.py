import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union, Any

import pandas as pd
from pydantic import BaseModel, Field

from .base import DeprecatedBaseModel, map_fields
from .sumulation import SimulationSettings


@dataclass
class Pyramid(BaseModel):
    name: str
    multiplier: float


@dataclass
class Classification:
    id: str
    name: str


@dataclass
class Competition(BaseModel):
    id: str
    name: str


@dataclass
class Regular:
    code: str
    description: str
    operatorCount: int


@dataclass
class SelfAlphaListQueryParams:
    hidden: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[str] = None
    status_ne: Optional[str] = None
    date_created_gt: Optional[str] = None
    date_created_lt: Optional[str] = None

    def to_params(self):
        params = {}
        if self.hidden is not None:
            params["hidden"] = "true" if self.hidden else "false"
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
        if self.order is not None:
            params["order"] = self.order
        if self.status_eq is not None:
            params["status"] = self.status_eq
        if self.status_ne is not None:
            params["status//!"] = self.status_ne
        if self.date_created_gt is not None:
            params["dateCreated>"] = self.date_created_gt
        if self.date_created_lt is not None:
            params["dateCreated<"] = self.date_created_lt
        return params


@dataclass
class AlphaCheckItem(BaseModel):
    name: str
    result: str
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[Competition]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[Pyramid]] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    multiplier: Optional[float] = None


@dataclass
class AlphaSample(DeprecatedBaseModel):
    pnl: Optional[float] = None
    bookSize: Optional[float] = None
    longCount: Optional[int] = None
    shortCount: Optional[int] = None
    turnover: Optional[float] = None
    returns: Optional[float] = None
    drawdown: Optional[float] = None
    margin: Optional[float] = None
    sharpe: Optional[float] = None
    fitness: Optional[float] = None
    startDate: Optional[datetime] = None
    checks: Optional[List[AlphaCheckItem]] = field(default_factory=list)
    selfCorrelation: Optional[float] = None
    prodCorrelation: Optional[float] = None
    osISSharpeRatio: Optional[float] = None
    preCloseSharpeRatio: Optional[float] = None

    def __post_init__(self):
        self._convert_fields({"startDate": datetime})


@dataclass
class Alpha(DeprecatedBaseModel):
    id: str
    type: str
    author: str
    settings: Union[SimulationSettings, dict]
    regular: Union[Regular, dict]
    dateCreated: Optional[datetime] = None
    dateSubmitted: Optional[datetime] = None
    dateModified: Optional[datetime] = None
    name: str = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = field(default_factory=list)
    classifications: Optional[List[Union[Classification, dict]]] = field(
        default_factory=list
    )
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    inSample: Optional[Union[AlphaSample, dict]] = None
    outSample: Optional[Union[AlphaSample, dict]] = None
    train: Optional[Union[AlphaSample, dict]] = None
    test: Optional[Union[AlphaSample, dict]] = None
    prod: Optional[Union[AlphaSample, dict]] = None
    competitions: Optional[List[Union[Competition, dict]]] = field(default_factory=list)
    themes: Optional[List[str]] = field(default_factory=list)
    pyramids: Optional[List[Union[Pyramid, dict]]] = field(default_factory=list)
    team: Optional[str] = None

    def __post_init__(self):
        self._convert_fields(
            {
                "settings": SimulationSettings,
                "regular": Regular,
                "classifications": Classification,
                "inSample": AlphaSample,
                "outSample": AlphaSample,
                "train": AlphaSample,
                "test": AlphaSample,
                "prod": AlphaSample,
                "competitions": Competition,
                "pyramids": Pyramid,
                "dateCreated": datetime,
                "dateSubmitted": datetime,
                "dateModified": datetime,
            }
        )


class SelfAlphaList:
    def __init__(self, count, next, previous, results):
        self.count = count
        self.next = next
        self.previous = previous
        self.results = [Alpha(**result) for result in (results or [])]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        field_mapping = {"is": "inSample", "os": "outSample"}
        mapped_data = map_fields(data, field_mapping)
        return cls(**mapped_data)


class AlphaDetail(BaseModel):
    id: str
    type: str
    author: str
    settings: Union[SimulationSettings, dict]
    regular: Union[Regular, dict]
    date_created: Optional[datetime] = Field(default=None, alias="dateCreated")
    date_submitted: Optional[datetime] = Field(default=None, alias="dateSubmitted")
    date_modified: Optional[datetime] = Field(default=None, alias="dateModified")
    name: str = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    classifications: Optional[List[Union[Classification, dict]]] = None
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    in_sample: Optional["AlphaDetail.Sample"] = Field(default=None, alias="is")
    out_sample: Optional["AlphaDetail.Sample"] = Field(default=None, alias="os")
    train: Optional["AlphaDetail.Sample"] = None
    test: Optional["AlphaDetail.Sample"] = None
    prod: Optional["AlphaDetail.Sample"] = None
    competitions: Optional[List[Competition]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[Union[Pyramid, dict]]] = None
    team: Optional[str] = None

    class Sample(BaseModel):
        investability_constrained: Optional["AlphaSample"] = Field(
            default=None, alias="investabilityConstrained"
        )
        risk_neutralized: Optional["AlphaSample"] = Field(
            default=None, alias="riskNeutralized"
        )
        pnl: Optional[float] = None
        bookSize: Optional[float] = None
        longCount: Optional[int] = None
        shortCount: Optional[int] = None
        turnover: Optional[float] = None
        returns: Optional[float] = None
        drawdown: Optional[float] = None
        margin: Optional[float] = None
        sharpe: Optional[float] = None
        fitness: Optional[float] = None
        startDate: Optional[datetime] = None
        checks: Optional[List[AlphaCheckItem]] = None
        selfCorrelation: Optional[float] = None
        prodCorrelation: Optional[float] = None
        osISSharpeRatio: Optional[float] = None
        preCloseSharpeRatio: Optional[float] = None


@dataclass
class AlphaYearlyStats_Property:
    name: str
    title: str
    type: str


@dataclass
class AlphaYearlyStats_Schema:
    name: str
    title: str
    properties: List[AlphaYearlyStats_Property]


@dataclass
class AlphaYearlyStats_Record:
    year: int
    pnl: float
    bookSize: float
    longCount: int
    shortCount: int
    turnover: float
    sharpe: float
    returns: float
    drawdown: float
    margin: float
    fitness: float
    stage: str


@dataclass
class AlphaYearlyStats:
    schema: AlphaYearlyStats_Schema
    records: List[AlphaYearlyStats_Record]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)

    def to_dataframe(self):
        data = []
        for record in self.records or []:
            data.append(
                [getattr(record, prop.name) for prop in (self.schema.properties or [])]
            )
        return pd.DataFrame(
            data, columns=[prop.title for prop in (self.schema.properties or [])]
        )


@dataclass
class AlphaPnL_Property:
    name: str
    title: str
    type: str


@dataclass
class AlphaPnL_Schema:
    name: str
    title: str
    properties: List[AlphaPnL_Property]


@dataclass
class AlphaPnL_Record(DeprecatedBaseModel):
    date: datetime
    pnl: float

    def __post_init__(self):
        self._convert_fields({"date": datetime})


@dataclass
class AlphaPnL:
    schema: AlphaPnL_Schema
    records: List[AlphaPnL_Record]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


@dataclass
class AlphaCorrelationsProperty(BaseModel):
    name: str
    title: str
    type: str


@dataclass
class AlphaCorrelationsSchema(BaseModel):
    name: str
    title: str
    properties: List[AlphaCorrelationsProperty]


@dataclass
class AlphaCorrelationRecord(BaseModel):
    id: str
    name: str
    instrumentType: str
    region: str
    universe: str
    correlation: float
    sharpe: float
    returns: float
    turnover: float
    fitness: float
    margin: float


@dataclass
class AlphaCorrelations(BaseModel):
    table_schema: AlphaCorrelationsSchema = Field(alias="schema")
    records: List[List[Any]]
    min: Optional[float] = 0.0
    max: Optional[float] = 0.0


@dataclass
class AlphaPropertiesBody_Regular:
    description: str


@dataclass
class AlphaPropertiesBody:
    color: str
    name: str
    tags: List[str]
    category: str
    regular: AlphaPropertiesBody_Regular

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


@dataclass
class AlphaCheckResult(BaseModel):
    inSample: Optional["AlphaCheckResult.Sample"] = Field(default=None, alias="is")
    outSample: Optional["AlphaCheckResult.Sample"] = Field(default=None, alias="os")

    @dataclass
    class Sample(BaseModel):
        checks: List[AlphaCheckItem] = None
        selfCorrelated: Optional[AlphaCorrelations] = None
        prodCorrelated: Optional[AlphaCorrelations] = None
