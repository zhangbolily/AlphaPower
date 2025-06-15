from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, BaseModel, Field, TypeAdapter

from alphapower.constants import (
    AlphaType,
    CodeLanguage,
    Color,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    Stage,
    Status,
    SubmissionCheckResult,
    Switch,
    TagType,
    UnitHandling,
    Universe,
)

from .common import PayloadBase, QueryBase


class IdNameRefView(BaseModel):
    id: str
    name: Optional[str]


CompetitionRefViewListAdapter: TypeAdapter[List[IdNameRefView]] = TypeAdapter(
    List[IdNameRefView],
)


class ClassificationRefView(BaseModel):
    id: str
    name: str


ClassificationRefViewListAdapter: TypeAdapter[List[ClassificationRefView]] = (
    TypeAdapter(
        List[ClassificationRefView],
    )
)


class ThemeRefView(BaseModel):
    id: str
    multiplier: float = Field(default=1.0)
    name: str


ThemeRefViewListAdapter: TypeAdapter[List[ThemeRefView]] = TypeAdapter(
    List[ThemeRefView],
)


class PyramidRefView(BaseModel):
    name: str
    multiplier: float


PyramidRefViewListAdapter: TypeAdapter[List[PyramidRefView]] = TypeAdapter(
    List[PyramidRefView],
)


class SubmissionCheckView(BaseModel):
    name: str  # 已知的提交检查名称都定义在了 constants.py 中的 SubmissionCheckType 枚举值中
    result: SubmissionCheckResult
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[IdNameRefView]] = None
    themes: Optional[List[ThemeRefView]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[PyramidRefView]] = None
    start_date: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("startDate", "start_date"),
        serialization_alias="startDate",
    )
    end_date: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("endDate", "end_date"),
        serialization_alias="endDate",
    )
    multiplier: Optional[float] = None
    effective: Optional[float] = None  # Pyramid PASS 的时候这个字段应该有值


SubmissionCheckViewListAdapter: TypeAdapter[List[SubmissionCheckView]] = TypeAdapter(
    List[SubmissionCheckView],
)


class ExpressionView(BaseModel):
    """常规信息。

    表示常规Alpha信息，包括代码、描述和操作符计数。

    Attributes:
        code: Alpha或策略的代码。
        description: Alpha或策略的描述，可选。
        operator_count: Alpha中使用的操作符数量，可选。
    """

    code: str
    description: Optional[str] = None
    operator_count: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("operatorCount", "operator_count"),
        serialization_alias="operatorCount",
    )


class AggregateDataView(BaseModel):
    pnl: Optional[float] = None
    book_size: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("bookSize", "book_size"),
        serialization_alias="bookSize",
    )
    long_count: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("longCount", "long_count"),
        serialization_alias="longCount",
    )
    short_count: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("shortCount", "short_count"),
        serialization_alias="shortCount",
    )
    turnover: Optional[float] = None
    returns: Optional[float] = None
    drawdown: Optional[float] = None
    margin: Optional[float] = None
    sharpe: Optional[float] = None
    fitness: Optional[float] = None
    start_date: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("startDate", "start_date"),
        serialization_alias="startDate",
    )
    checks: Optional[List[SubmissionCheckView]] = None
    self_correlation: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("selfCorrelation", "self_correlation"),
        serialization_alias="selfCorrelation",
    )
    prod_correlation: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("prodCorrelation", "prod_correlation"),
        serialization_alias="prodCorrelation",
    )
    os_is_sharpe_ratio: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("osISSharpeRatio", "os_is_sharpe_ratio"),
        serialization_alias="osISSharpeRatio",
    )
    pre_close_sharpe_ratio: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("preCloseSharpeRatio", "pre_close_sharpe_ratio"),
        serialization_alias="preCloseSharpeRatio",
    )


class SettingsView(BaseModel):

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
    language: Optional[CodeLanguage] = None
    decay: Optional[int] = None
    neutralization: Optional[Neutralization] = None
    visualization: Optional[bool] = None
    max_trade: Optional[Switch] = Field(
        None,
        validation_alias=AliasChoices("maxTrade", "max_trade"),
        serialization_alias="maxTrade",
    )


class AlphaView(BaseModel):

    id: str
    type: str
    author: str
    settings: SettingsView
    regular: Optional[ExpressionView] = None
    combo: Optional[ExpressionView] = None
    selection: Optional[ExpressionView] = None
    date_created: datetime = Field(
        validation_alias=AliasChoices("dateCreated", "date_created")
    )
    date_submitted: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("dateSubmitted", "date_submitted")
    )
    date_modified: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("dateModified", "date_modified")
    )
    name: Optional[str] = ""
    favorite: bool = False
    hidden: bool = False
    color: Optional[Color] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    classifications: Optional[List[ClassificationRefView]] = None
    grade: Optional[Grade] = None
    stage: Optional[Stage] = None
    status: Optional[Status] = None
    in_sample: Optional[AggregateDataView] = Field(
        default=None,
        validation_alias=AliasChoices("is", "in_sample"),
        serialization_alias="is",
    )
    out_sample: Optional[AggregateDataView] = Field(
        default=None,
        validation_alias=AliasChoices("os", "out_sample"),
        serialization_alias="os",
    )
    train: Optional[AggregateDataView] = None
    test: Optional[AggregateDataView] = None
    prod: Optional[AggregateDataView] = None
    competitions: Optional[List[IdNameRefView]] = None
    themes: Optional[List[ThemeRefView]] = None
    pyramids: Optional[List[PyramidRefView]] = None
    team: Optional[str] = None


StringListAdapter: TypeAdapter[List[str]] = TypeAdapter(
    List[str],
)


class AlphaDetailView(BaseModel):

    id: str
    type: str
    author: str
    settings: SettingsView
    regular: Optional[ExpressionView] = None
    selection: Optional[ExpressionView] = None
    combo: Optional[ExpressionView] = None
    date_created: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("dateCreated", "date_created")
    )
    date_submitted: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("dateSubmitted", "date_submitted")
    )
    date_modified: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("dateModified", "date_modified")
    )
    name: Optional[str] = None
    favorite: bool = False
    hidden: bool = False
    color: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    classifications: Optional[List[ClassificationRefView]] = None
    grade: Optional[str] = None
    stage: Optional[str] = None
    status: Optional[str] = None
    in_sample: Optional["AlphaDetailView.Sample"] = Field(
        default=None, validation_alias=AliasChoices("is", "in_sample")
    )
    out_sample: Optional["AlphaDetailView.Sample"] = Field(
        default=None, validation_alias=AliasChoices("os", "out_sample")
    )
    train: Optional["AlphaDetailView.Sample"] = None
    test: Optional["AlphaDetailView.Sample"] = None
    prod: Optional["AlphaDetailView.Sample"] = None
    competitions: Optional[List[IdNameRefView]] = None
    themes: Optional[List[ThemeRefView]] = None
    pyramids: Optional[List[PyramidRefView]] = None
    team: Optional[str] = None

    class Sample(BaseModel):

        investability_constrained: Optional[AggregateDataView] = Field(
            default=None,
            validation_alias=AliasChoices(
                "investabilityConstrained", "investability_constrained"
            ),
            serialization_alias="investabilityConstrained",
        )
        risk_neutralized: Optional[AggregateDataView] = Field(
            default=None,
            validation_alias=AliasChoices("riskNeutralized", "risk_neutralized"),
            serialization_alias="riskNeutralized",
        )
        pnl: Optional[float] = None
        book_size: Optional[float] = Field(
            default=None,
            validation_alias=AliasChoices("bookSize", "book_size"),
            serialization_alias="bookSize",
        )
        long_count: Optional[int] = Field(
            default=None,
            validation_alias=AliasChoices("longCount", "long_count"),
            serialization_alias="longCount",
        )
        short_count: Optional[int] = Field(
            default=None,
            validation_alias=AliasChoices("shortCount", "short_count"),
            serialization_alias="shortCount",
        )
        turnover: Optional[float] = None
        returns: Optional[float] = None
        drawdown: Optional[float] = None
        margin: Optional[float] = None
        sharpe: Optional[float] = None
        fitness: Optional[float] = None
        start_date: Optional[datetime] = Field(
            default=None,
            validation_alias=AliasChoices("startDate", "start_date"),
            serialization_alias="startDate",
        )
        checks: Optional[List[SubmissionCheckView]] = None
        self_correlation: Optional[float] = Field(
            default=None,
            validation_alias=AliasChoices("selfCorrelation", "self_correlation"),
            serialization_alias="selfCorrelation",
        )
        prod_correlation: Optional[float] = Field(
            default=None,
            validation_alias=AliasChoices("prodCorrelation", "prod_correlation"),
            serialization_alias="prodCorrelation",
        )
        os_is_sharpe_ratio: Optional[float] = Field(
            default=None,
            validation_alias=AliasChoices("osISSharpeRatio", "os_is_sharpe_ratio"),
            serialization_alias="osISSharpeRatio",
        )
        pre_close_sharpe_ratio: Optional[float] = Field(
            default=None,
            validation_alias=AliasChoices(
                "preCloseSharpeRatio", "pre_close_sharpe_ratio"
            ),
            serialization_alias="preCloseSharpeRatio",
        )


class UserAlphasSummaryView(BaseModel):
    active: Optional[int] = None
    decommissioned: Optional[int] = None
    unsubmitted: Optional[int] = None


class UserAlphasQuery(QueryBase):
    competition: Optional[Any] = None
    date_created_gt: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("dateCreated>", "date_created_gt"),
        serialization_alias="dateCreated>",
    )
    date_created_lt: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("dateCreated<", "date_created_lt"),
        serialization_alias="dateCreated<",
    )
    date_submitted_gt: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("dateSubmitted>", "date_submitted_gt"),
        serialization_alias="dateSubmitted>",
    )
    date_submitted_lt: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("dateSubmitted<", "date_submitted_lt"),
        serialization_alias="dateSubmitted<",
    )
    hidden: Optional[bool] = None
    limit: Optional[int] = None
    name: Optional[str] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[Status] = Field(
        default=None,
        validation_alias=AliasChoices("status", "status_eq"),
        serialization_alias="status",
    )
    status_ne: Optional[Status] = Field(
        default=None,
        validation_alias=AliasChoices("status//!", "status_ne"),
        serialization_alias="status//!",
    )
    tag: Optional[str] = None
    type: Optional[AlphaType] = None

    settings_universe: Optional[Universe] = Field(
        default=None,
        validation_alias=AliasChoices("settingsUniverse", "settings_universe"),
        serialization_alias="settings.universe",
    )

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        params = super().model_dump(
            **kwargs,
        )
        if self.hidden is not None:
            params["hidden"] = "true" if self.hidden else "false"
        if isinstance(self.competition, bool):
            params["competition"] = "true" if self.competition else "false"
        return params


class UserAlphasView(BaseModel):
    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[AlphaView]


class SelfAlphaListView(BaseModel):
    """
    已废弃（Deprecated）: 此类用于封装 Alpha 列表视图。
    """

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[AlphaView]


class SelfAlphaListQueryParams(BaseModel):
    """
    已废弃（Deprecated）: 此类用于封装 Alpha 列表查询参数。
    Attributes:
        hidden (Optional[bool]): 是否隐藏，True 表示隐藏，False 表示不隐藏。
        limit (Optional[int]): 返回结果的最大数量。
        offset (Optional[int]): 查询结果的偏移量。
        order (Optional[str]): 排序方式。
        status_eq (Optional[str]): 精确匹配的状态（status），支持别名 "status" 或 "status_eq"。
        status_ne (Optional[str]): 不等于的状态（status），支持别名 "status//!" 或 "status_ne"。
        date_created_gt (Optional[str]): 创建时间大于指定值，支持别名 "dateCreated>" 或 "date_created_gt"。
        date_created_lt (Optional[str]): 创建时间小于指定值，支持别名 "dateCreated<" 或 "date_created_lt"。
    Methods:
        to_params() -> dict:
            将当前对象的属性转换为用于请求的参数字典。
    """

    hidden: Optional[bool] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    order: Optional[str] = None
    status_eq: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("status", "status_eq")
    )
    status_ne: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("status//!", "status_ne")
    )
    date_created_gt: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("dateCreated>", "date_created_gt")
    )
    date_created_lt: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("dateCreated<", "date_created_lt")
    )

    def to_params(self) -> dict:

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


class CreateTagsPayload(PayloadBase):
    alphas: List[str]
    name: str
    type: TagType


class TagView(BaseModel):
    id: str
    type: TagType
    name: str
    alphas: List[IdNameRefView]


class SelfTagListQuery(QueryBase):
    pass


class SelfTagListView(BaseModel):
    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[TagView]


class AlphaPropertiesPayload(PayloadBase):

    class Code(BaseModel):
        description: Optional[str] = None

    color: Optional[Color] = None
    name: Optional[str] = None
    tags: List[str] = []  # tags 不能为 null
    category: Optional[str] = None
    regular: Code = Code()
    selection: Code = Code()
    combo: Code = Code()


class SelfCorrelationView(BaseModel):
    class CorrelationItem(BaseModel):
        alpha_id: str
        correlation: float

    alpha_id: str
    min: float
    max: float
    correlations: List[CorrelationItem]


class ProdCorrelationView(BaseModel):
    class CorrelationInterval(BaseModel):
        lower: float
        upper: float
        alphas: int

    alpha_id: str
    min: float
    max: float
    intervals: List[CorrelationInterval]
