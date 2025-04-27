# pylint: disable=C0302


from datetime import datetime
from typing import Any, Dict, List, Optional

from multidict import CIMultiDictProxy
from pydantic import AliasChoices, BaseModel, Field, RootModel

from alphapower.constants import (
    AlphaType,
    Color,
    CompetitionScoring,
    CompetitionStatus,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Stage,
    Status,
    SubmissionCheckResult,
    Switch,
    UnitHandling,
    Universe,
)

from .common_view import TableView


class PyramidRefView(BaseModel):

    name: str
    multiplier: float


class ClassificationView(BaseModel):

    id: str
    name: str


class CompetitionView(BaseModel):

    class Leaderboard(BaseModel):

        rank: int
        user: str
        power_pool_alphas: int = Field(
            default=0,
            validation_alias=AliasChoices("powerPoolAlphas", "power_pool_alphas"),
        )
        merged_pnl_score: float = Field(
            default=0.0,
            validation_alias=AliasChoices("mergedPNLScore", "merged_pnl_score"),
        )
        before_cost_score: float = Field(
            default=0.0,
            validation_alias=AliasChoices("beforeCostScore", "before_cost_score"),
        )
        after_cost_score: float = Field(
            default=0.0,
            validation_alias=AliasChoices("afterCostScore", "after_cost_score"),
        )
        is_score: float = Field(
            default=0.0, validation_alias=AliasChoices("isScore", "is_score")
        )
        university: Optional[str] = None
        country: Optional[str] = None

    id: str
    name: str
    description: Optional[str] = None
    universities: Optional[List[str]] = None
    countries: Optional[List[str]] = None
    excluded_countries: Optional[List[str]] = Field(
        default=None,
        validation_alias=AliasChoices("excludedCountries", "excluded_countries"),
    )
    status: CompetitionStatus
    team_based: bool = Field(
        default=False, validation_alias=AliasChoices("teamBased", "team_based")
    )
    start_date: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("startDate", "start_date")
    )
    end_date: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("endDate", "end_date")
    )
    sign_up_start_date: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("signUpStartDate", "sign_up_start_date"),
    )
    sign_up_end_date: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("signUpEndDate", "sign_up_end_date")
    )
    sign_up_date: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("signUpDate", "sign_up_date")
    )
    team: Optional[str] = None
    scoring: CompetitionScoring
    leaderboard: Optional[Leaderboard] = None
    prize_board: bool = Field(
        default=False, validation_alias=AliasChoices("prizeBoard", "prize_board")
    )
    university_board: bool = Field(
        default=False,
        validation_alias=AliasChoices("universityBoard", "university_board"),
    )
    submissions: bool = Field(
        default=False, validation_alias=AliasChoices("submissions", "submissions")
    )
    faq: str
    progress: Optional[float] = None


class CompetitionRefView(BaseModel):

    id: str
    name: str


class ThemeRefView(BaseModel):
    id: str
    multiplier: float = Field(default=1.0)
    name: str


class CompetitionListView(BaseModel):

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[CompetitionView]


class ExpressionView(BaseModel):

    code: str
    description: Optional[str] = None
    operator_count: Optional[int] = Field(
        default=None, validation_alias=AliasChoices("operatorCount", "operator_count")
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


class SelfAlphaListQueryParams(BaseModel):

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


class AlphaCheckItemView(BaseModel):

    name: str
    result: SubmissionCheckResult
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[CompetitionRefView]] = None
    themes: Optional[List[ThemeRefView]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[PyramidRefView]] = None
    start_date: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("startDate", "start_date")
    )
    end_date: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("endDate", "end_date")
    )
    multiplier: Optional[float] = None


class AlphaSampleView(BaseModel):

    pnl: Optional[float] = None
    book_size: Optional[float] = Field(
        default=None, validation_alias=AliasChoices("bookSize", "book_size")
    )
    long_count: Optional[int] = Field(
        default=None, validation_alias=AliasChoices("longCount", "long_count")
    )
    short_count: Optional[int] = Field(
        default=None, validation_alias=AliasChoices("shortCount", "short_count")
    )
    turnover: Optional[float] = None
    returns: Optional[float] = None
    drawdown: Optional[float] = None
    margin: Optional[float] = None
    sharpe: Optional[float] = None
    fitness: Optional[float] = None
    start_date: Optional[datetime] = Field(
        default=None, validation_alias=AliasChoices("startDate", "start_date")
    )
    checks: Optional[List[AlphaCheckItemView]] = None
    self_correlation: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("selfCorrelation", "self_correlation"),
    )
    prod_correlation: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("prodCorrelation", "prod_correlation"),
    )
    os_is_sharpe_ratio: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("osISSharpeRatio", "os_is_sharpe_ratio"),
    )
    pre_close_sharpe_ratio: Optional[float] = Field(
        default=None,
        validation_alias=AliasChoices("preCloseSharpeRatio", "pre_close_sharpe_ratio"),
    )


class AlphaView(BaseModel):

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
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
    classifications: Optional[List[ClassificationView]] = None
    grade: Optional[Grade] = None
    stage: Optional[Stage] = None
    status: Optional[Status] = None
    in_sample: Optional[AlphaSampleView] = Field(
        default=None, validation_alias=AliasChoices("is", "in_sample")
    )
    out_sample: Optional[AlphaSampleView] = Field(
        default=None, validation_alias=AliasChoices("os", "out_sample")
    )
    train: Optional[AlphaSampleView] = None
    test: Optional[AlphaSampleView] = None
    prod: Optional[AlphaSampleView] = None
    competitions: Optional[List[CompetitionRefView]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[PyramidRefView]] = None
    team: Optional[str] = None


class SelfAlphaListView(BaseModel):

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[AlphaView]


class AlphaDetailView(BaseModel):

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
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
    classifications: Optional[List[ClassificationView]] = None
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
    competitions: Optional[List[CompetitionRefView]] = None
    themes: Optional[List[str]] = None
    pyramids: Optional[List[PyramidRefView]] = None
    team: Optional[str] = None

    class Sample(BaseModel):

        investability_constrained: Optional[AlphaSampleView] = Field(
            default=None,
            validation_alias=AliasChoices(
                "investabilityConstrained", "investability_constrained"
            ),
            serialization_alias="investabilityConstrained",
        )
        risk_neutralized: Optional[AlphaSampleView] = Field(
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
        checks: Optional[List[AlphaCheckItemView]] = None
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


class AlphaYearlyStatsRecordView(BaseModel):

    year: int
    pnl: float
    book_size: float = Field(
        validation_alias=AliasChoices("bookSize", "book_size"),
        serialization_alias="bookSize",
    )
    long_count: int = Field(
        validation_alias=AliasChoices("longCount", "long_count"),
        serialization_alias="longCount",
    )
    short_count: int = Field(
        validation_alias=AliasChoices("shortCount", "short_count"),
        serialization_alias="shortCount",
    )
    turnover: float
    sharpe: float
    returns: float
    drawdown: float
    margin: float
    fitness: float
    stage: str


class AlphaPropertiesPayload(BaseModel):

    class Regular(BaseModel):

        description: Optional[str] = None

    color: Optional[str] = None
    name: Optional[str] = None
    tags: List[str]
    category: Optional[str] = None
    regular: Regular = Regular()


class RateLimit:

    def __init__(self, limit: int, remaining: int, reset: int) -> None:

        self.limit: int = limit
        self.remaining: int = remaining
        self.reset: int = reset

    @classmethod
    def from_headers(cls, headers: CIMultiDictProxy[str]) -> "RateLimit":

        limit: int = int(headers.get("RateLimit-Limit", 0))
        remaining: int = int(headers.get("RateLimit-Remaining", 0))
        reset: int = int(headers.get("RateLimit-Reset", 0))
        return cls(limit, remaining, reset)

    def __str__(self) -> str:

        return f"RateLimit(limit={self.limit}, remaining={self.remaining}, reset={self.reset})"


class DataCategoriesView(BaseModel):

    id: str
    name: str
    dataset_count: int = Field(
        validation_alias=AliasChoices("datasetCount", "dataset_count"),
        serialization_alias="datasetCount",
    )
    field_count: int = Field(
        validation_alias=AliasChoices("fieldCount", "field_count"),
        serialization_alias="fieldCount",
    )
    alpha_count: int = Field(
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    user_count: int = Field(
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    value_score: float = Field(
        validation_alias=AliasChoices("valueScore", "value_score"),
        serialization_alias="valueScore",
    )
    region: str
    children: List["DataCategoriesView"] = []


class DataCategoriesListView(RootModel):

    root: Optional[List[DataCategoriesView]] = None


class DataSetsQueryParams(BaseModel):

    category: Optional[str] = None
    delay: Optional[int] = None
    instrumentType: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    region: Optional[str] = None
    universe: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:

        params = self.model_dump(mode="python")
        return params


class DataCategoryView(BaseModel):

    id: str
    name: str


class DatasetView(BaseModel):

    id: str
    name: str
    description: str
    category: DataCategoryView
    subcategory: DataCategoryView
    region: str
    delay: int
    universe: str
    coverage: str
    value_score: float = Field(
        validation_alias=AliasChoices("valueScore", "value_score"),
        serialization_alias="valueScore",
    )
    user_count: int = Field(
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    alpha_count: int = Field(
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    field_count: int = Field(
        validation_alias=AliasChoices("fieldCount", "field_count"),
        serialization_alias="fieldCount",
    )
    themes: List[str]
    research_papers: List["ResearchPaperView"] = Field(
        validation_alias=AliasChoices("researchPapers", "research_papers"),
        serialization_alias="researchPapers",
    )
    pyramid_multiplier: Optional[float] = Field(
        validation_alias=AliasChoices("pyramidMultiplier", "pyramid_multiplier"),
        serialization_alias="pyramidMultiplier",
    )


class DatasetListView(BaseModel):

    count: int
    results: List[DatasetView] = []


class DatasetDataView(BaseModel):

    region: str
    delay: int
    universe: str
    coverage: str
    value_score: float = Field(
        validation_alias=AliasChoices("valueScore", "value_score"),
        serialization_alias="valueScore",
    )
    user_count: int = Field(
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    alpha_count: int = Field(
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    field_count: int = Field(
        validation_alias=AliasChoices("fieldCount", "field_count"),
        serialization_alias="fieldCount",
    )
    themes: List[str]
    pyramid_multiplier: Optional[float] = Field(
        validation_alias=AliasChoices("pyramidMultiplier", "pyramid_multiplier"),
        serialization_alias="pyramidMultiplier",
    )


class ResearchPaperView(BaseModel):

    type: str
    title: str
    url: str


class DatasetDetailView(BaseModel):

    name: str
    description: str
    category: DataCategoryView
    subcategory: DataCategoryView
    data: List[DatasetDataView]
    research_papers: List[ResearchPaperView] = Field(
        validation_alias=AliasChoices("researchPapers", "research_papers"),
        serialization_alias="researchPapers",
    )


class DataFieldItemView(BaseModel):

    region: str
    delay: int
    universe: str
    coverage: str
    user_count: int = Field(
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    alpha_count: int = Field(
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    themes: List[str]


class DataFieldDatasetView(BaseModel):

    id: str
    name: str


class DatasetDataFieldsView(BaseModel):

    dataset: DataFieldDatasetView
    category: DataCategoryView
    subcategory: DataCategoryView
    description: str
    type: str
    data: List[DataFieldItemView]


class DataFieldView(BaseModel):

    id: str
    description: str
    dataset: DataFieldDatasetView
    category: DataCategoryView
    subcategory: DataCategoryView
    region: str
    delay: int
    universe: str
    type: str
    coverage: str
    user_count: int = Field(
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    alpha_count: int = Field(
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    themes: List[str]
    pyramid_multiplier: Optional[float] = Field(
        validation_alias=AliasChoices("pyramidMultiplier", "pyramid_multiplier"),
        serialization_alias="pyramidMultiplier",
    )


class DataFieldListView(BaseModel):

    count: int
    results: List[DataFieldView] = []


class GetDataFieldsQueryParams(BaseModel):

    dataset_id: str = Field(validation_alias=AliasChoices("dataset.id", "dataset_id"))
    delay: Optional[int] = None
    instrument_type: Optional[str] = Field(
        validation_alias=AliasChoices("instrumentType", "instrument_type")
    )
    limit: Optional[int] = None
    offset: Optional[int] = None
    region: Optional[str] = None
    universe: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:

        params = self.model_dump(mode="python")
        return params


class Operator(BaseModel):

    name: str
    category: str
    scope: str
    definition: str
    description: str
    documentation: str
    level: str


class Operators(BaseModel):

    operators: List[Operator]


class SimulationProgressView(BaseModel):

    progress: float


class SingleSimulationResultView(BaseModel):

    id: str
    type: AlphaType
    status: str
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


class MultiSimulationResultView(BaseModel):

    children: List[str]
    status: str
    type: AlphaType
    settings: Optional[SimulationSettingsView] = None


class SingleSimulationPayload(BaseModel):

    type: str
    settings: SimulationSettingsView
    regular: str

    def to_params(self) -> Dict[str, Any]:

        return {
            "type": self.type,
            "settings": self.settings.model_dump(by_alias=True),
            "regular": self.regular,
        }


class MultiSimulationPayload(RootModel):

    root: List[SingleSimulationPayload]

    def to_params(self) -> List[Any]:

        return [s.to_params() for s in self.root]


class SelfSimulationActivitiesView(BaseModel):

    class Period(BaseModel):

        start: str
        end: str
        value: float

    yesterday: Period
    current: Period
    previous: Period
    ytd: Period
    total: Period
    records: TableView
    type: str


class AuthenticationView(BaseModel):

    class User(BaseModel):

        id: str

    class Token(BaseModel):

        expiry: float

    user: User
    token: Token
    permissions: List[str]
