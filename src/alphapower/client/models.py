"""
This module contains the data models for the AlphaPower API.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from multidict import CIMultiDictProxy
from pydantic import BaseModel, Field, RootModel


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


class TableSchema(BaseModel):
    """
    表示记录的模式。
    """

    name: str
    title: str
    properties: List["TableSchema.Property"]

    class Property(BaseModel):
        """
        表示模式中的一个属性。
        """

        name: str
        title: str
        type: str


class SimulationSettingsView(BaseModel):
    """
    表示模拟的设置。
    """

    nan_handling: Optional[str] = Field(None, validation_alias="nanHandling")
    instrument_type: Optional[str] = Field(None, validation_alias="instrumentType")
    delay: Optional[int] = None
    universe: Optional[str] = None
    truncation: Optional[float] = None
    unit_handling: Optional[str] = Field(None, validation_alias="unitHandling")
    test_period: Optional[str] = Field(None, validation_alias="testPeriod")
    pasteurization: Optional[str] = None
    region: Optional[str] = None
    language: Optional[str] = None
    decay: Optional[int] = None
    neutralization: Optional[str] = None
    visualization: Optional[bool] = None
    max_trade: Optional[str] = Field(
        None, validation_alias="maxTrade"
    )  # 无权限字段，默认值为 None


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


class RateLimit:
    """
    表示 API 的速率限制信息。
    """

    def __init__(self, limit: int, remaining: int, reset: int) -> None:
        self.limit: int = limit
        self.remaining: int = remaining
        self.reset: int = reset

    @classmethod
    def from_headers(cls, headers: CIMultiDictProxy[str]) -> "RateLimit":
        """
        从响应头中创建 RateLimit 实例。
        :param headers: 响应头
        :return: RateLimit 实例
        """

        limit: int = int(headers.get("RateLimit-Limit", 0))
        remaining: int = int(headers.get("RateLimit-Remaining", 0))
        reset: int = int(headers.get("RateLimit-Reset", 0))
        return cls(limit, remaining, reset)

    def __str__(self) -> str:
        return f"RateLimit(limit={self.limit}, remaining={self.remaining}, reset={self.reset})"


class DataCategoriesView(BaseModel):
    """
    数据类别视图模型
    表示数据类别的详细信息，包括子类别和统计信息
    """

    id: str  # 类别的唯一标识符
    name: str  # 类别名称
    dataset_count: int = Field(alias="datasetCount")  # 数据集数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    user_count: int = Field(alias="userCount")  # 用户数量
    value_score: float = Field(alias="valueScore")  # 价值评分
    region: str  # 所属区域
    children: List["DataCategoriesView"] = []  # 子类别列表


class DataCategoriesListView(RootModel):
    """
    数据类别列表视图模型
    表示数据类别的集合
    """

    root: Optional[List[DataCategoriesView]] = None  # 数据类别列表


class DataSetsQueryParams(BaseModel):
    """
    数据集查询参数模型
    用于定义查询数据集时的参数
    """

    category: Optional[str] = None  # 数据类别
    delay: Optional[int] = None  # 延迟时间
    instrumentType: Optional[str] = None  # 仪器类型
    limit: Optional[int] = None  # 查询结果限制数量
    offset: Optional[int] = None  # 查询结果偏移量
    region: Optional[str] = None  # 区域
    universe: Optional[str] = None  # 宇宙（范围）

    def to_params(self) -> Dict[str, Any]:
        """
        转换为查询参数字典
        :return: 查询参数字典
        """
        params = self.model_dump(mode="python")
        return params


class DataCategoryView(BaseModel):
    """
    数据类别视图模型
    表示单个数据类别的基本信息
    """

    id: str  # 类别的唯一标识符
    name: str  # 类别名称


class DatasetView(BaseModel):
    """
    数据集视图模型
    表示数据集的详细信息
    """

    id: str  # 数据集的唯一标识符
    name: str  # 数据集名称
    description: str  # 数据集描述
    category: DataCategoryView  # 数据集所属类别
    subcategory: DataCategoryView  # 数据集所属子类别
    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    value_score: float = Field(alias="valueScore")  # 价值评分
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    themes: List[str]  # 主题列表
    research_papers: List["ResearchPaperView"] = Field(
        alias="researchPapers"
    )  # 相关研究论文
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class DatasetListView(BaseModel):
    """
    数据集列表视图模型
    表示数据集的集合
    """

    count: int  # 数据集总数
    results: List[DatasetView] = []  # 数据集列表


class DatasetDataView(BaseModel):
    """
    数据集详细视图模型
    表示数据集的详细统计信息
    """

    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    value_score: float = Field(alias="valueScore")  # 价值评分
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    themes: List[str]  # 主题列表
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class ResearchPaperView(BaseModel):
    """
    研究论文视图模型
    表示研究论文的基本信息
    """

    type: str  # 论文类型
    title: str  # 论文标题
    url: str  # 论文链接


class DatasetDetailView(BaseModel):
    """
    数据集详细信息模型
    包含数据集的基本信息和详细数据
    """

    name: str  # 数据集名称
    description: str  # 数据集描述
    category: DataCategoryView  # 数据集所属类别
    subcategory: DataCategoryView  # 数据集所属子类别
    data: List[DatasetDataView]  # 数据集详细数据
    research_papers: List[ResearchPaperView] = Field(
        alias="researchPapers"
    )  # 相关研究论文


class DataFieldItemView(BaseModel):
    """
    数据字段项视图模型
    表示单个数据字段的详细信息
    """

    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    themes: List[str]  # 主题列表


class DataFieldDatasetView(BaseModel):
    """
    数据字段数据集视图模型
    表示数据字段所属的数据集的基本信息
    """

    id: str  # 数据集的唯一标识符
    name: str  # 数据集名称


class DatasetDataFieldsView(BaseModel):
    """
    数据集字段视图模型
    表示数据集的字段信息
    """

    dataset: DataFieldDatasetView  # 数据字段所属数据集
    category: DataCategoryView  # 数据字段所属类别
    subcategory: DataCategoryView  # 数据字段所属子类别
    description: str  # 数据字段描述
    type: str  # 数据字段类型
    data: List[DataFieldItemView]  # 数据字段详细信息


class DataFieldView(BaseModel):
    """
    数据字段模型
    表示单个数据字段的详细信息
    """

    id: str  # 数据字段的唯一标识符
    description: str  # 数据字段描述
    dataset: DataFieldDatasetView  # 数据字段所属数据集
    category: DataCategoryView  # 数据字段所属类别
    subcategory: DataCategoryView  # 数据字段所属子类别
    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    type: str  # 数据字段类型
    coverage: str  # 覆盖范围
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    themes: List[str]  # 主题列表
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class DataFieldListView(BaseModel):
    """
    数据字段列表视图模型
    表示数据字段的集合
    """

    count: int  # 数据字段总数
    results: List[DataFieldView] = []  # 数据字段列表


class GetDataFieldsQueryParams(BaseModel):
    """
    获取数据字段查询参数模型
    用于定义查询数据字段时的参数
    """

    dataset_id: str = Field(serialization_alias="dataset.id")  # 数据集 ID
    delay: Optional[int] = None  # 延迟时间
    instrument_type: Optional[str] = Field(
        serialization_alias="instrumentType"
    )  # 仪器类型
    limit: Optional[int] = None  # 查询结果限制数量
    offset: Optional[int] = None  # 查询结果偏移量
    region: Optional[str] = None  # 区域
    universe: Optional[str] = None  # 宇宙（范围）

    def to_params(self) -> Dict[str, Any]:
        """
        转换为查询参数字典
        :return: 查询参数字典
        """
        params = self.model_dump(mode="python")
        return params


class Operator(BaseModel):
    """
    操作符类，表示单个操作符的详细信息。

    属性:
        name: 操作符名称
        category: 操作符类别
        scope: 操作符作用范围
        definition: 操作符定义
        description: 操作符描述
        documentation: 操作符文档链接
        level: 操作符级别
    """

    name: str
    category: str
    scope: str
    definition: str
    description: str
    documentation: str
    level: str


class Operators(BaseModel):
    """
    操作符集合类，包含多个操作符。

    属性:
        operators: 操作符列表
    """

    operators: List[Operator]


class SimulationProgressView(BaseModel):
    """
    表示模拟的进度。
    """

    progress: float


class SingleSimulationResultView(BaseModel):
    """
    表示单次模拟的结果。
    """

    id: str
    type: str
    status: str
    message: Optional[str] = None
    location: Optional["SingleSimulationResultView.ErrorLocation"] = None
    settings: Optional[SimulationSettingsView] = None
    regular: Optional[str] = None
    alpha: Optional[str] = None
    parent: Optional[str] = None

    class ErrorLocation(BaseModel):
        """
        表示模拟中错误的位置。
        """

        line: Optional[int] = None
        start: Optional[int] = None
        end: Optional[int] = None
        property: Optional[str] = None


class MultiSimulationResultView(BaseModel):
    """
    表示多次模拟的结果。
    """

    children: List[str]
    status: str
    type: str
    settings: Optional[SimulationSettingsView] = None


class SingleSimulationPayload(BaseModel):
    """
    表示单次模拟的请求。
    """

    type: str
    settings: SimulationSettingsView
    regular: str

    def to_params(self) -> Dict[str, Any]:
        """
        将模拟请求转换为参数字典。

        返回:
            Dict[str, Any]: 参数字典。
        """
        return {
            "type": self.type,
            "settings": self.settings.dict(by_alias=True),
            "regular": self.regular,
        }


class MultiSimulationPayload(RootModel):
    """
    表示多次模拟的请求。
    """

    root: List[SingleSimulationPayload]

    def to_params(self) -> List[Any]:
        """
        将模拟请求转换为参数字典的列表。

        返回:
            List[Any]: 参数字典的列表。
        """
        return [s.to_params() for s in self.root]


class SelfSimulationActivitiesView(BaseModel):
    """
    表示自模拟活动。
    """

    yesterday: "SelfSimulationActivitiesView.Period"
    current: "SelfSimulationActivitiesView.Period"
    previous: "SelfSimulationActivitiesView.Period"
    ytd: "SelfSimulationActivitiesView.Period"
    total: "SelfSimulationActivitiesView.Period"
    records: "SelfSimulationActivitiesView.Records"
    type: str

    class Period(BaseModel):
        """
        表示模拟活动中的一个时间段。
        """

        start: str
        end: str
        value: float

    class Records(BaseModel):
        """
        表示模拟活动的记录。
        """

        table_schema: TableSchema = Field(validation_alias="schema")
        records: List[Dict[str, Any]]


class AuthenticationView(BaseModel):
    """
    表示身份验证视图的主类，包含用户信息、令牌和权限。

    属性:
        user: 用户信息
        token: 令牌信息
        permissions: 权限列表
    """

    user: "AuthenticationView.User"
    token: "AuthenticationView.Token"
    permissions: List[str]

    class User(BaseModel):
        """
        表示用户信息的嵌套类。
        """

        id: str

    class Token(BaseModel):
        """
        表示令牌信息的嵌套类。
        """

        expiry: float
