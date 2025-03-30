"""
This module contains the data models for the AlphaPower API.
"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from .common import TableSchema
from .simulation import SimulationSettingsView


class PyramidView(BaseModel):
    """
    表示金字塔模型的类。
    """

    name: str
    multiplier: float


class ClassificationView(BaseModel):
    """
    表示分类信息的类。
    """

    id: str
    name: str


class CompetitionView(BaseModel):
    """
    表示竞赛信息的类。
    """

    id: str
    name: str


class RegularView(BaseModel):
    """
    表示常规信息的类。
    """

    code: str
    description: Optional[str] = None
    operator_count: Optional[int] = Field(
        default=None, validation_alias="operatorCount"
    )


class SelfAlphaListQueryParams(BaseModel):
    """
    表示 Alpha 列表查询参数的类。
    """

    hidden: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[str] = Field(default=None, validation_alias="status")
    status_ne: Optional[str] = Field(default=None, validation_alias="status//!")
    date_created_gt: Optional[str] = Field(
        default=None, validation_alias="dateCreated>"
    )
    date_created_lt: Optional[str] = Field(
        default=None, validation_alias="dateCreated<"
    )

    def to_params(self) -> dict:
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


class AlphaCheckItemView(BaseModel):
    """
    表示 Alpha 检查项的类。
    """

    name: str
    result: str
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[CompetitionView]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[PyramidView]] = None
    start_date: Optional[str] = Field(default=None, validation_alias="startDate")
    end_date: Optional[str] = Field(default=None, validation_alias="endDate")
    multiplier: Optional[float] = None


class AlphaSampleView(BaseModel):
    """
    表示 Alpha 样本数据的类。
    """

    pnl: Optional[float] = None
    book_size: Optional[float] = Field(default=None, validation_alias="bookSize")
    long_count: Optional[int] = Field(default=None, validation_alias="longCount")
    short_count: Optional[int] = Field(default=None, validation_alias="shortCount")
    turnover: Optional[float] = None
    returns: Optional[float] = None
    drawdown: Optional[float] = None
    margin: Optional[float] = None
    sharpe: Optional[float] = None
    fitness: Optional[float] = None
    start_date: Optional[datetime] = Field(default=None, validation_alias="startDate")
    checks: Optional[List["AlphaCheckItemView"]] = None
    self_correlation: Optional[float] = Field(
        default=None, validation_alias="selfCorrelation"
    )
    prod_correlation: Optional[float] = Field(
        default=None, validation_alias="prodCorrelation"
    )
    os_is_sharpe_ratio: Optional[float] = Field(
        default=None, validation_alias="osISSharpeRatio"
    )
    pre_close_sharpe_ratio: Optional[float] = Field(
        default=None, validation_alias="preCloseSharpeRatio"
    )


class AlphaView(BaseModel):
    """
    表示 Alpha 实体的类。
    """

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
    regular: RegularView
    date_created: Optional[datetime] = Field(
        default=None, validation_alias="dateCreated"
    )
    date_submitted: Optional[datetime] = Field(
        default=None, validation_alias="dateSubmitted"
    )
    date_modified: Optional[datetime] = Field(
        default=None, validation_alias="dateModified"
    )
    name: str = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    classifications: Optional[List[ClassificationView]] = None
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    in_sample: Optional[AlphaSampleView] = Field(default=None, validation_alias="is")
    out_sample: Optional[AlphaSampleView] = Field(default=None, validation_alias="os")
    train: Optional[AlphaSampleView] = None
    test: Optional[AlphaSampleView] = None
    prod: Optional[AlphaSampleView] = None
    competitions: Optional[List[CompetitionView]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[PyramidView]] = None
    team: Optional[str] = None


class SelfAlphaListView(BaseModel):
    """
    表示 Alpha 列表的类。
    """

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[AlphaView]


class AlphaDetailView(BaseModel):
    """
    表示 Alpha 详细信息的类。
    """

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
    regular: Optional[RegularView] = None
    selection: Optional[RegularView] = None
    combo: Optional[RegularView] = None
    date_created: Optional[datetime] = Field(
        default=None, validation_alias="dateCreated"
    )
    date_submitted: Optional[datetime] = Field(
        default=None, validation_alias="dateSubmitted"
    )
    date_modified: Optional[datetime] = Field(
        default=None, validation_alias="dateModified"
    )
    name: Optional[str] = None
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    classifications: Optional[List[ClassificationView]] = None
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    in_sample: Optional["AlphaDetailView.Sample"] = Field(
        default=None, validation_alias="is"
    )
    out_sample: Optional["AlphaDetailView.Sample"] = Field(
        default=None, validation_alias="os"
    )
    train: Optional["AlphaDetailView.Sample"] = None
    test: Optional["AlphaDetailView.Sample"] = None
    prod: Optional["AlphaDetailView.Sample"] = None
    competitions: Optional[List[CompetitionView]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[PyramidView]] = None
    team: Optional[str] = None

    class Sample(BaseModel):
        """
        表示 Alpha 样本详细信息的类。
        """

        investability_constrained: Optional[AlphaSampleView] = Field(
            default=None, validation_alias="investabilityConstrained"
        )
        risk_neutralized: Optional[AlphaSampleView] = Field(
            default=None, validation_alias="riskNeutralized"
        )
        pnl: Optional[float] = None
        book_size: Optional[float] = Field(default=None, validation_alias="bookSize")
        long_count: Optional[int] = Field(default=None, validation_alias="longCount")
        short_count: Optional[int] = Field(default=None, validation_alias="shortCount")
        turnover: Optional[float] = None
        returns: Optional[float] = None
        drawdown: Optional[float] = None
        margin: Optional[float] = None
        sharpe: Optional[float] = None
        fitness: Optional[float] = None
        start_date: Optional[datetime] = Field(
            default=None, validation_alias="startDate"
        )
        checks: Optional[List[AlphaCheckItemView]] = None
        self_correlation: Optional[float] = Field(
            default=None, validation_alias="selfCorrelation"
        )
        prod_correlation: Optional[float] = Field(
            default=None, validation_alias="prodCorrelation"
        )
        os_is_sharpe_ratio: Optional[float] = Field(
            default=None, validation_alias="osISSharpeRatio"
        )
        pre_close_sharpe_ratio: Optional[float] = Field(
            default=None, validation_alias="preCloseSharpeRatio"
        )


class AlphaYearlyStatsRecordView(BaseModel):
    """
    表示 Alpha 年度统计记录的类。
    """

    year: int
    pnl: float
    book_size: float = Field(alias="bookSize")
    long_count: int = Field(alias="longCount")
    short_count: int = Field(alias="shortCount")
    turnover: float
    sharpe: float
    returns: float
    drawdown: float
    margin: float
    fitness: float
    stage: str


class AlphaYearlyStatsView(BaseModel):
    """
    表示 Alpha 年度统计的类。
    """

    table_schema: TableSchema = Field(alias="schema")
    records: List[AlphaYearlyStatsRecordView]


class AlphaPnLRecordView(BaseModel):
    """
    表示 Alpha 盈亏记录的类。
    """

    date: datetime
    pnl: float


class AlphaPnLView(BaseModel):
    """
    表示 Alpha 盈亏的类。
    """

    table_schema: TableSchema = Field(alias="schema")
    records: List[AlphaPnLRecordView]


class AlphaCorrelationRecordView(BaseModel):
    """
    表示 Alpha 相关性记录的类。
    """

    id: str
    name: str
    instrument_type: str = Field(alias="instrumentType")
    region: str
    universe: str
    correlation: float
    sharpe: float
    returns: float
    turnover: float
    fitness: float
    margin: float


class AlphaCorrelationsView(BaseModel):
    """
    表示 Alpha 相关性的类。
    """

    table_schema: TableSchema = Field(alias="schema")
    records: List[List[Any]]
    min: Optional[float] = 0.0
    max: Optional[float] = 0.0


class AlphaPropertiesPayload(BaseModel):
    """
    表示 Alpha 属性主体的类。
    """

    color: str
    name: str
    tags: List[str]
    category: str
    regular: "AlphaPropertiesPayload.Regular"

    class Regular(BaseModel):
        """
        表示 Alpha 属性主体中的常规信息的类。
        """

        description: str


class AlphaCheckResultView(BaseModel):
    """
    表示 Alpha 检查结果的类。
    """

    in_sample: Optional["AlphaCheckResultView.Sample"] = Field(
        default=None, validation_alias="is"
    )
    out_sample: Optional["AlphaCheckResultView.Sample"] = Field(
        default=None, validation_alias="os"
    )

    class Sample(BaseModel):
        """
        表示 Alpha 检查结果样本的类。
        """

        checks: Optional[List[AlphaCheckItemView]] = None
        self_correlated: Optional[AlphaCorrelationsView] = Field(
            default=None, validation_alias="selfCorrelated"
        )
        prod_correlated: Optional[AlphaCorrelationsView] = Field(
            default=None, validation_alias="prodCorrelated"
        )
