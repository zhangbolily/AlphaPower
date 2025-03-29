"""
This module contains the data models for the AlphaPower API.
"""

from datetime import datetime
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field

from .sumulation import SimulationSettings


class Pyramid(BaseModel):
    """
    表示金字塔模型的类。
    """

    name: str
    multiplier: float


class Classification(BaseModel):
    """
    表示分类信息的类。
    """

    id: str
    name: str


class Competition(BaseModel):
    """
    表示竞赛信息的类。
    """

    id: str
    name: str


class Regular(BaseModel):
    """
    表示常规信息的类。
    """

    code: str
    description: str
    operatorCount: int


class SelfAlphaListQueryParams(BaseModel):
    """
    表示 Alpha 列表查询参数的类。
    """

    hidden: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[str] = Field(default=None, alias="status")
    status_ne: Optional[str] = Field(default=None, alias="status//!")
    date_created_gt: Optional[str] = Field(default=None, alias="dateCreated>")
    date_created_lt: Optional[str] = Field(default=None, alias="dateCreated<")

    def to_params(self):
        """
        将查询参数转换为字典格式。
        """
        params = {}
        if self.hidden is not None:
            params["hidden"] = "true" if self.hidden else "false"
        if self.limit is not None:
            params["limit"] = str(self.limit)
        if self.offset is not None:
            params["offset"] = str(self.offset)
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


class AlphaCheckItem(BaseModel):
    """
    表示 Alpha 检查项的类。
    """

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


class AlphaSample(BaseModel):
    """
    表示 Alpha 样本数据的类。
    """

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
    checks: Optional[List["AlphaCheckItem"]] = None
    selfCorrelation: Optional[float] = None
    prodCorrelation: Optional[float] = None
    osISSharpeRatio: Optional[float] = None
    preCloseSharpeRatio: Optional[float] = None


class Alpha(BaseModel):
    """
    表示 Alpha 实体的类。
    """

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
    tags: Optional[List[str]] = None
    classifications: Optional[List[Union[Classification, dict]]] = None
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    inSample: Optional[Union[AlphaSample, dict]] = Field(default=None, alias="is")
    outSample: Optional[Union[AlphaSample, dict]] = Field(default=None, alias="os")
    train: Optional[Union[AlphaSample, dict]] = None
    test: Optional[Union[AlphaSample, dict]] = None
    prod: Optional[Union[AlphaSample, dict]] = None
    competitions: Optional[List[Union[Competition, dict]]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[Union[Pyramid, dict]]] = None
    team: Optional[str] = None


class SelfAlphaList(BaseModel):
    """
    表示 Alpha 列表的类。
    """

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[Alpha]


class AlphaDetail(BaseModel):
    """
    表示 Alpha 详细信息的类。
    """

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
        """
        表示 Alpha 样本详细信息的类。
        """

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


class AlphaYearlyStatsProperty(BaseModel):
    """
    表示 Alpha 年度统计属性的类。
    """

    name: str
    title: str
    type: str


class AlphaYearlyStatsSchema(BaseModel):
    """
    表示 Alpha 年度统计模式的类。
    """

    name: str
    title: str
    properties: List[AlphaYearlyStatsProperty]


class AlphaYearlyStatsRecord(BaseModel):
    """
    表示 Alpha 年度统计记录的类。
    """

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


class AlphaYearlyStats(BaseModel):
    """
    表示 Alpha 年度统计的类。
    """

    table_schema: AlphaYearlyStatsSchema = Field(alias="schema")
    records: List[AlphaYearlyStatsRecord]


class AlphaPnLProperty(BaseModel):
    """
    表示 Alpha 盈亏属性的类。
    """

    name: str
    title: str
    type: str


class AlphaPnLSchema(BaseModel):
    """
    表示 Alpha 盈亏模式的类。
    """

    name: str
    title: str
    properties: List[AlphaPnLProperty]


class AlphaPnLRecord(BaseModel):
    """
    表示 Alpha 盈亏记录的类。
    """

    date: datetime
    pnl: float


class AlphaPnL(BaseModel):
    """
    表示 Alpha 盈亏的类。
    """

    table_schema: AlphaPnLSchema = Field(alias="schema")
    records: List[AlphaPnLRecord]


class AlphaCorrelationsProperty(BaseModel):
    """
    表示 Alpha 相关性属性的类。
    """

    name: str
    title: str
    type: str


class AlphaCorrelationsSchema(BaseModel):
    """
    表示 Alpha 相关性模式的类。
    """

    name: str
    title: str
    properties: List[AlphaCorrelationsProperty]


class AlphaCorrelationRecord(BaseModel):
    """
    表示 Alpha 相关性记录的类。
    """

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


class AlphaCorrelations(BaseModel):
    """
    表示 Alpha 相关性的类。
    """

    table_schema: AlphaCorrelationsSchema = Field(alias="schema")
    records: List[List[Any]]
    min: Optional[float] = 0.0
    max: Optional[float] = 0.0


class AlphaPropertiesBodyRegular(BaseModel):
    """
    表示 Alpha 属性主体中的常规信息的类。
    """

    description: str


class AlphaPropertiesBody(BaseModel):
    """
    表示 Alpha 属性主体的类。
    """

    color: str
    name: str
    tags: List[str]
    category: str
    regular: AlphaPropertiesBodyRegular


class AlphaCheckResult(BaseModel):
    """
    表示 Alpha 检查结果的类。
    """

    inSample: Optional["AlphaCheckResult.Sample"] = Field(default=None, alias="is")
    outSample: Optional["AlphaCheckResult.Sample"] = Field(default=None, alias="os")

    class Sample(BaseModel):
        """
        表示 Alpha 检查结果样本的类。
        """

        checks: Optional[List[AlphaCheckItem]] = None
        selfCorrelated: Optional[AlphaCorrelations] = None
        prodCorrelated: Optional[AlphaCorrelations] = None
