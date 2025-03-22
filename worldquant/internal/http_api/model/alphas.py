import json
from worldquant.internal.http_api.common import *
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd

from .base import BaseModel, map_fields


@dataclass
class Pyramid:
    name: str
    multiplier: float


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
class Alpha_Settings:
    instrumentType: str
    region: str
    universe: str
    delay: int
    decay: float
    neutralization: str
    truncation: float
    pasteurization: bool
    unitHandling: str
    nanHandling: str
    language: str
    visualization: str
    testPeriod: Optional[str] = None
    maxTrade: Optional[int] = None


@dataclass
class Alpha_Regular:
    code: str
    description: str
    operatorCount: int


@dataclass
class Alpha_Sample_Check(BaseModel):
    name: str
    result: str
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[str]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[Pyramid]] = field(default_factory=list)
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    multiplier: Optional[float] = None

    def __post_init__(self):
        self._convert_fields({"date": datetime})


@dataclass
class Alpha_Sample(BaseModel):
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
    checks: Optional[List[Alpha_Sample_Check]] = field(default_factory=list)
    selfCorrelation: Optional[float] = None
    prodCorrelation: Optional[float] = None
    osISSharpeRatio: Optional[float] = None
    preCloseSharpeRatio: Optional[float] = None

    def __post_init__(self):
        self._convert_fields({"startDate": datetime})


@dataclass
class Alpha_Classification:
    id: int
    name: str


@dataclass
class Alpha_Competition:
    id: int
    name: str


@dataclass
class Alpha(BaseModel):
    id: str
    type: str
    author: str
    settings: Union[Alpha_Settings, dict]
    regular: Union[Alpha_Regular, dict]
    dateCreated: Optional[datetime] = None
    dateSubmitted: Optional[datetime] = None
    dateModified: Optional[datetime] = None
    name: str = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = field(default_factory=list)
    classifications: Optional[List[Union[Alpha_Classification, dict]]] = field(
        default_factory=list
    )
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    inSample: Optional[Union[Alpha_Sample, dict]] = None
    outSample: Optional[Union[Alpha_Sample, dict]] = None
    train: Optional[Union[Alpha_Sample, dict]] = None
    test: Optional[Union[Alpha_Sample, dict]] = None
    prod: Optional[Union[Alpha_Sample, dict]] = None
    competitions: Optional[List[Union[Alpha_Competition, dict]]] = field(
        default_factory=list
    )
    themes: Optional[List[str]] = field(default_factory=list)
    pyramids: Optional[List[Union[Pyramid, dict]]] = field(default_factory=list)
    team: Optional[str] = None

    def __post_init__(self):
        self._convert_fields(
            {
                "settings": Alpha_Settings,
                "regular": Alpha_Regular,
                "classifications": Alpha_Classification,
                "inSample": Alpha_Sample,
                "outSample": Alpha_Sample,
                "train": Alpha_Sample,
                "test": Alpha_Sample,
                "prod": Alpha_Sample,
                "competitions": Alpha_Competition,
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


@dataclass
class AlphaDetail_Settings:
    instrumentType: str
    region: str
    universe: str
    delay: int
    decay: float
    neutralization: str
    truncation: float
    pasteurization: bool
    unitHandling: str
    nanHandling: str
    language: str
    visualization: str
    testPeriod: str


@dataclass
class AlphaDetail_Regular:
    code: str
    description: str
    operatorCount: int


@dataclass
class AlphaDetail_IS_Check(BaseModel):
    name: str
    result: str
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[str]] = None
    message: Optional[str] = None

    def __post_init__(self):
        self._convert_fields({"date": datetime})


@dataclass
class AlphaDetail_IS(BaseModel):
    pnl: float
    bookSize: float
    longCount: int
    shortCount: int
    turnover: float
    returns: float
    drawdown: float
    margin: float
    sharpe: float
    fitness: float
    startDate: datetime
    checks: List[AlphaDetail_IS_Check]

    def __post_init__(self):
        self._convert_fields({"startDate": datetime})


@dataclass
class AlphaDetail_Train(BaseModel):
    pnl: float
    bookSize: float
    longCount: int
    shortCount: int
    turnover: float
    returns: float
    drawdown: float
    margin: float
    fitness: float
    sharpe: float
    startDate: str

    def __post_init__(self):
        self._convert_fields({"startDate": datetime})


@dataclass
class AlphaDetail_Test(BaseModel):
    pnl: float
    bookSize: float
    longCount: int
    shortCount: int
    turnover: float
    returns: float
    drawdown: float
    margin: float
    fitness: float
    sharpe: float
    startDate: str

    def __post_init__(self):
        self._convert_fields({"startDate": datetime})


@dataclass
class AlphaDetail_Classification:
    id: int
    name: str


@dataclass
class AlphaDetail(BaseModel):
    id: str
    type: str
    author: str
    settings: Union[AlphaDetail_Settings, dict]
    regular: Union[AlphaDetail_Regular, dict]
    dateCreated: Optional[datetime] = None
    dateSubmitted: Optional[datetime] = None
    dateModified: Optional[datetime] = None
    name: str = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = field(default_factory=list)
    classifications: Optional[List[Union[AlphaDetail_Classification, dict]]] = field(
        default_factory=list
    )
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    inSample: Optional[Union[AlphaDetail_IS, dict]] = None
    outSample: Optional[dict] = None
    train: Optional[Union[AlphaDetail_Train, dict]] = None
    test: Optional[Union[AlphaDetail_Test, dict]] = None
    prod: Optional[dict] = None
    competitions: Optional[List[dict]] = field(default_factory=list)
    themes: Optional[List[str]] = field(default_factory=list)
    pyramids: Optional[List[Union[Pyramid, dict]]] = field(default_factory=list)
    team: Optional[str] = None

    def __post_init__(self):
        self._convert_fields(
            {
                "settings": AlphaDetail_Settings,
                "regular": AlphaDetail_Regular,
                "classifications": AlphaDetail_Classification,
                "inSample": AlphaDetail_IS,
                "train": AlphaDetail_Train,
                "test": AlphaDetail_Test,
                "pyramids": Pyramid,
                "dateCreated": datetime,
                "dateSubmitted": datetime,
                "dateModified": datetime,
            }
        )

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


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
class AlphaPnL_Record(BaseModel):
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
class AlphaSelfCorrelations_Property:
    name: str
    title: str
    type: str


@dataclass
class AlphaSelfCorrelations_Schema:
    name: str
    title: str
    properties: List[AlphaSelfCorrelations_Property]


@dataclass
class AlphaSelfCorrelations_Record:
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
class AlphaSelfCorrelations:
    schema: AlphaSelfCorrelations_Schema
    records: List[AlphaSelfCorrelations_Record]
    min: float
    max: float

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        return cls(**data)


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
class AplhaCheckResult:
    inSample: dict
    outSample: dict

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)
        field_mapping = {"is": "inSample", "os": "outSample"}
        mapped_data = map_fields(data, field_mapping)
        return cls(**mapped_data)
