# pylint: disable=C0302
"""AlphaPower API 的数据模型。

本模块定义了与 AlphaPower API 交互使用的数据结构。
包括 Alpha 指标、分类、竞赛、模拟、数据类别和其他相关实体的模型。

Attributes:
    本模块中的所有类都是 Pydantic 模型，负责数据验证、序列化和反序列化。
"""

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
    Switch,
    UnitHandling,
    Universe,
)

from .common_view import TableView


class PyramidView(BaseModel):
    """金字塔模型。

    表示金字塔模型的基本信息，包括名称和乘数。

    Attributes:
        name: 金字塔的名称。
        multiplier: 金字塔的乘数值。
    """

    name: str
    multiplier: float


class ClassificationView(BaseModel):
    """分类信息。

    表示分类的基本信息，包括ID和名称。

    Attributes:
        id: 分类的唯一标识符。
        name: 分类的名称。
    """

    id: str
    name: str


class CompetitionView(BaseModel):
    """竞赛信息。

    表示竞赛的详细信息，包括ID、名称、状态、团队信息、日期范围等。

    Attributes:
        id: 竞赛的唯一标识符。
        name: 竞赛的名称。
        description: 竞赛的描述，可选。
        universities: 参与的大学列表，可选。
        countries: 参与的国家列表，可选。
        excluded_countries: 排除的国家列表，可选。
        status: 竞赛的状态。
        team_based: 是否基于团队。
        start_date: 竞赛开始日期，可选。
        end_date: 竞赛结束日期，可选。
        sign_up_start_date: 报名开始日期，可选。
        sign_up_end_date: 报名结束日期，可选。
        sign_up_date: 报名日期，可选。
        team: 团队信息，可选。
        scoring: 评分方式。
        leaderboard: 排行榜信息，可选。
        prize_board: 是否有奖品板。
        university_board: 是否有大学板。
        submissions: 是否允许提交。
        faq: 常见问题链接。
        progress: 竞赛进度，可选。
    """

    class Leaderboard(BaseModel):
        """
        排行榜信息。
        """

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
    """竞赛引用。

    表示与Alpha相关的竞赛信息。

    Attributes:
        id: 竞赛的唯一标识符。
        name: 竞赛的名称。
    """

    id: str
    name: str


class CompetitionListView(BaseModel):
    """竞赛列表。

    表示竞赛的基本信息列表，包括分页数据。

    Attributes:
        count: 竞赛总数。
        next: 下一页链接，可选。
        previous: 上一页链接，可选。
        results: 竞赛列表。
    """

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[CompetitionView]


class RegularView(BaseModel):
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
        default=None, validation_alias=AliasChoices("operatorCount", "operator_count")
    )


class SimulationSettingsView(BaseModel):
    """模拟设置。

    包含模拟运行的各种配置参数。

    Attributes:
        nan_handling: NaN值处理方式，可选。
        instrument_type: 工具类型，可选。
        delay: 延迟天数，可选。
        universe: 股票范围，可选。
        truncation: 截断值，可选。
        unit_handling: 单位处理方式，可选。
        test_period: 测试周期，可选。
        pasteurization: 巴氏杀菌法（数据处理方式），可选。
        region: 地区，可选。
        language: 编程语言，可选。
        decay: 衰减值，可选。
        neutralization: 中性化处理方式，可选。
        visualization: 是否可视化，可选。
        max_trade: 最大交易限制，可选。
    """

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
    """Alpha列表查询参数。

    定义查询用户自己的Alpha列表时使用的参数。

    Attributes:
        hidden: 是否隐藏，可选。
        limit: 返回结果数量限制，可选。
        offset: 结果偏移量，可选。
        order: 排序方式，可选。
        status_eq: 状态等于过滤，可选。
        status_ne: 状态不等于过滤，可选。
        date_created_gt: 创建日期大于，可选。
        date_created_lt: 创建日期小于，可选。
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
        """将查询参数转换为字典格式。

        将对象的属性转换为API查询参数的字典格式。

        Returns:
            查询参数字典。
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
    """Alpha检查项。

    表示对Alpha进行检查的单个项目结果。

    Attributes:
        name: 检查项名称。
        result: 检查结果。
        limit: 限制值，可选。
        value: 实际值，可选。
        date: 检查日期，可选。
        competitions: 相关竞赛列表，可选。
        message: 检查消息，可选。
        year: 检查年份，可选。
        pyramids: 金字塔列表，可选。
        start_date: 起始日期，可选。
        end_date: 结束日期，可选。
        multiplier: 乘数，可选。
    """

    name: str
    result: str
    limit: Optional[float] = None
    value: Optional[float] = None
    date: Optional[datetime] = None
    competitions: Optional[List[CompetitionRefView]] = None
    message: Optional[str] = None
    year: Optional[int] = None
    pyramids: Optional[List[PyramidView]] = None
    start_date: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("startDate", "start_date")
    )
    end_date: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("endDate", "end_date")
    )
    multiplier: Optional[float] = None


class AlphaSampleView(BaseModel):
    """Alpha样本数据。

    表示Alpha在某个样本集上的性能数据。

    Attributes:
        pnl: 盈亏值，可选。
        book_size: 账簿规模，可选。
        long_count: 多头数量，可选。
        short_count: 空头数量，可选。
        turnover: 周转率，可选。
        returns: 回报率，可选。
        drawdown: 回撤，可选。
        margin: 利润率，可选。
        sharpe: 夏普比率，可选。
        fitness: 适应度，可选。
        start_date: 开始日期，可选。
        checks: 检查项列表，可选。
        self_correlation: 自相关性，可选。
        prod_correlation: 生产相关性，可选。
        os_is_sharpe_ratio: 样本外-样本内夏普比率，可选。
        pre_close_sharpe_ratio: 收盘前夏普比率，可选。
    """

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
    checks: Optional[List["AlphaCheckItemView"]] = None
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
    """Alpha实体。

    表示完整的Alpha信息，包括基本数据和性能指标。

    Attributes:
        id: Alpha的唯一标识符。
        type: Alpha类型。
        author: Alpha作者。
        settings: 模拟设置。
        regular: 常规信息。
        date_created: 创建日期，可选。
        date_submitted: 提交日期，可选。
        date_modified: 修改日期，可选。
        name: Alpha名称，可选，默认为空字符串。
        favorite: 是否为收藏，默认为False。
        hidden: 是否隐藏，默认为False。
        color: 颜色代码，可选。
        category: 类别，可选。
        tags: 标签列表，可选。
        classifications: 分类列表，可选。
        grade: 等级，可选。
        stage: 阶段，可选。
        status: 状态，可选。
        in_sample: 样本内性能，可选。
        out_sample: 样本外性能，可选。
        train: 训练集性能，可选。
        test: 测试集性能，可选。
        prod: 生产环境性能，可选。
        competitions: 相关竞赛，可选。
        themes: 主题列表，可选。
        pyramids: 金字塔列表，可选。
        team: 团队名称，可选。
    """

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
    regular: RegularView
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
    pyramids: Optional[List[PyramidView]] = None
    team: Optional[str] = None


class SelfAlphaListView(BaseModel):
    """Alpha列表。

    表示用户自己的Alpha列表信息，包含分页数据。

    Attributes:
        count: Alpha总数。
        next: 下一页链接，可选。
        previous: 上一页链接，可选。
        results: Alpha列表。
    """

    count: int
    next: Optional[str]
    previous: Optional[str]
    results: List[AlphaView]


class AlphaDetailView(BaseModel):
    """Alpha详情。

    表示单个Alpha的详细信息，包括各种性能指标和配置。

    Attributes:
        id: Alpha的唯一标识符。
        type: Alpha类型。
        author: Alpha作者。
        settings: 模拟设置。
        regular: 常规信息，可选。
        selection: 选择规则，可选。
        combo: 组合规则，可选。
        date_created: 创建日期，可选。
        date_submitted: 提交日期，可选。
        date_modified: 修改日期，可选。
        name: Alpha名称，可选。
        favorite: 是否为收藏，默认为False。
        hidden: 是否隐藏，默认为False。
        color: 颜色代码，可选。
        category: 类别，可选。
        tags: 标签列表，可选。
        classifications: 分类列表，可选。
        grade: 等级，可选。
        stage: 阶段，可选。
        status: 状态，可选。
        in_sample: 样本内详细性能，可选。
        out_sample: 样本外详细性能，可选。
        train: 训练集详细性能，可选。
        test: 测试集详细性能，可选。
        prod: 生产环境详细性能，可选。
        competitions: 相关竞赛，可选。
        themes: 主题列表，可选。
        pyramids: 金字塔列表，可选。
        team: 团队名称，可选。
    """

    id: str
    type: str
    author: str
    settings: SimulationSettingsView
    regular: Optional[RegularView] = None
    selection: Optional[RegularView] = None
    combo: Optional[RegularView] = None
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
    pyramids: Optional[List[PyramidView]] = None
    team: Optional[str] = None

    class Sample(BaseModel):
        """Alpha样本详细信息。

        表示Alpha在特定样本集上的详细性能数据。

        Attributes:
            investability_constrained: 可投资性受限的性能，可选。
            risk_neutralized: 风险中性化后的性能，可选。
            pnl: 盈亏值，可选。
            book_size: 账簿规模，可选。
            long_count: 多头数量，可选。
            short_count: 空头数量，可选。
            turnover: 周转率，可选。
            returns: 回报率，可选。
            drawdown: 回撤，可选。
            margin: 利润率，可选。
            sharpe: 夏普比率，可选。
            fitness: 适应度，可选。
            start_date: 开始日期，可选。
            checks: 检查项列表，可选。
            self_correlation: 自相关性，可选。
            prod_correlation: 生产相关性，可选。
            os_is_sharpe_ratio: 样本外-样本内夏普比率，可选。
            pre_close_sharpe_ratio: 收盘前夏普比率，可选。
        """

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
    """Alpha年度统计记录。

    表示Alpha在特定年份的性能统计数据。

    Attributes:
        year: 年份。
        pnl: 盈亏值。
        book_size: 账簿规模。
        long_count: 多头数量。
        short_count: 空头数量。
        turnover: 周转率。
        sharpe: 夏普比率。
        returns: 回报率。
        drawdown: 回撤。
        margin: 利润率。
        fitness: 适应度。
        stage: 阶段。
    """

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
    """Alpha属性更新载荷。

    用于更新Alpha属性的请求体。

    Attributes:
        color: 颜色代码，可选。
        name: Alpha名称，可选。
        tags: 标签列表，可选。
        category: 类别，可选。
        regular: 常规信息。
    """

    class Regular(BaseModel):
        """常规信息。

        表示Alpha常规信息的更新内容。

        Attributes:
            description: Alpha描述，可选。
        """

        description: Optional[str] = None

    color: Optional[str] = None
    name: Optional[str] = None
    tags: Optional[List[str]] = None
    category: Optional[str] = None
    regular: Regular = Regular()


class RateLimit:
    """API速率限制。

    表示API的速率限制信息，包括限制数量、剩余请求数和重置时间。

    Attributes:
        limit: 请求限制数量。
        remaining: 剩余可用请求数。
        reset: 限制重置时间（秒）。
    """

    def __init__(self, limit: int, remaining: int, reset: int) -> None:
        """初始化速率限制实例。

        Args:
            limit: 请求限制数量。
            remaining: 剩余可用请求数。
            reset: 限制重置时间（秒）。
        """
        self.limit: int = limit
        self.remaining: int = remaining
        self.reset: int = reset

    @classmethod
    def from_headers(cls, headers: CIMultiDictProxy[str]) -> "RateLimit":
        """从响应头创建速率限制实例。

        从HTTP响应头中提取速率限制信息，创建RateLimit实例。

        Args:
            headers: HTTP响应头。

        Returns:
            RateLimit实例。
        """
        limit: int = int(headers.get("RateLimit-Limit", 0))
        remaining: int = int(headers.get("RateLimit-Remaining", 0))
        reset: int = int(headers.get("RateLimit-Reset", 0))
        return cls(limit, remaining, reset)

    def __str__(self) -> str:
        """返回速率限制的字符串表示。

        Returns:
            速率限制的字符串表示。
        """
        return f"RateLimit(limit={self.limit}, remaining={self.remaining}, reset={self.reset})"


class DataCategoriesView(BaseModel):
    """数据类别。

    表示数据类别的详细信息，包括子类别和统计信息。

    Attributes:
        id: 类别的唯一标识符。
        name: 类别名称。
        dataset_count: 数据集数量。
        field_count: 字段数量。
        alpha_count: Alpha数量。
        user_count: 用户数量。
        value_score: 价值评分。
        region: 所属区域。
        children: 子类别列表，默认为空列表。
    """

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
    """数据类别列表。

    表示数据类别的集合。

    Attributes:
        root: 数据类别列表，可选。
    """

    root: Optional[List[DataCategoriesView]] = None


class DataSetsQueryParams(BaseModel):
    """数据集查询参数。

    定义查询数据集时使用的参数。

    Attributes:
        category: 数据类别，可选。
        delay: 延迟天数，可选。
        instrumentType: 工具类型，可选。
        limit: 返回结果数量限制，可选。
        offset: 结果偏移量，可选。
        region: 区域，可选。
        universe: 股票范围，可选。
    """

    category: Optional[str] = None
    delay: Optional[int] = None
    instrumentType: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    region: Optional[str] = None
    universe: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:
        """转换为查询参数字典。

        将对象的属性转换为API查询参数的字典格式。

        Returns:
            查询参数字典。
        """
        params = self.model_dump(mode="python")
        return params


class DataCategoryView(BaseModel):
    """数据类别基础信息。

    表示单个数据类别的基本信息。

    Attributes:
        id: 类别的唯一标识符。
        name: 类别名称。
    """

    id: str
    name: str


class DatasetView(BaseModel):
    """数据集视图。

    表示数据集的详细信息。

    Attributes:
        id: 数据集的唯一标识符。
        name: 数据集名称。
        description: 数据集描述。
        category: 数据集所属主类别。
        subcategory: 数据集所属子类别。
        region: 所属区域。
        delay: 延迟天数。
        universe: 股票范围。
        coverage: 覆盖范围。
        value_score: 价值评分。
        user_count: 用户数量。
        alpha_count: Alpha数量。
        field_count: 字段数量。
        themes: 主题列表。
        research_papers: 相关研究论文列表。
        pyramid_multiplier: 金字塔乘数，可选。
    """

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
    """数据集列表。

    表示数据集的集合，包含分页信息。

    Attributes:
        count: 数据集总数。
        results: 数据集列表，默认为空列表。
    """

    count: int
    results: List[DatasetView] = []


class DatasetDataView(BaseModel):
    """数据集详细数据。

    表示数据集的详细统计信息。

    Attributes:
        region: 所属区域。
        delay: 延迟天数。
        universe: 股票范围。
        coverage: 覆盖范围。
        value_score: 价值评分。
        user_count: 用户数量。
        alpha_count: Alpha数量。
        field_count: 字段数量。
        themes: 主题列表。
        pyramid_multiplier: 金字塔乘数，可选。
    """

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
    """研究论文。

    表示研究论文的基本信息。

    Attributes:
        type: 论文类型。
        title: 论文标题。
        url: 论文链接。
    """

    type: str
    title: str
    url: str


class DatasetDetailView(BaseModel):
    """数据集详细信息。

    表示数据集的完整详细信息，包括基础信息和各类数据。

    Attributes:
        name: 数据集名称。
        description: 数据集描述。
        category: 数据集所属主类别。
        subcategory: 数据集所属子类别。
        data: 数据集详细数据列表。
        research_papers: 相关研究论文列表。
    """

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
    """数据字段项。

    表示单个数据字段的详细信息。

    Attributes:
        region: 所属区域。
        delay: 延迟天数。
        universe: 股票范围。
        coverage: 覆盖范围。
        user_count: 用户数量。
        alpha_count: Alpha数量。
        themes: 主题列表。
    """

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
    """数据字段所属数据集。

    表示数据字段所属的数据集的基本信息。

    Attributes:
        id: 数据集的唯一标识符。
        name: 数据集名称。
    """

    id: str
    name: str


class DatasetDataFieldsView(BaseModel):
    """数据集字段信息。

    表示数据集的字段信息，包括字段所属的数据集和分类。

    Attributes:
        dataset: 数据字段所属数据集。
        category: 数据字段所属主类别。
        subcategory: 数据字段所属子类别。
        description: 数据字段描述。
        type: 数据字段类型。
        data: 数据字段详细信息列表。
    """

    dataset: DataFieldDatasetView
    category: DataCategoryView
    subcategory: DataCategoryView
    description: str
    type: str
    data: List[DataFieldItemView]


class DataFieldView(BaseModel):
    """数据字段。

    表示单个数据字段的完整信息。

    Attributes:
        id: 数据字段的唯一标识符。
        description: 数据字段描述。
        dataset: 数据字段所属数据集。
        category: 数据字段所属主类别。
        subcategory: 数据字段所属子类别。
        region: 所属区域。
        delay: 延迟天数。
        universe: 股票范围。
        type: 数据字段类型。
        coverage: 覆盖范围。
        user_count: 用户数量。
        alpha_count: Alpha数量。
        themes: 主题列表。
        pyramid_multiplier: 金字塔乘数，可选。
    """

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
    """数据字段列表。

    表示数据字段的集合，包含分页信息。

    Attributes:
        count: 数据字段总数。
        results: 数据字段列表，默认为空列表。
    """

    count: int
    results: List[DataFieldView] = []


class GetDataFieldsQueryParams(BaseModel):
    """数据字段查询参数。

    定义查询数据字段时使用的参数。

    Attributes:
        dataset_id: 数据集ID。
        delay: 延迟天数，可选。
        instrument_type: 工具类型，可选。
        limit: 返回结果数量限制，可选。
        offset: 结果偏移量，可选。
        region: 区域，可选。
        universe: 股票范围，可选。
    """

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
        """转换为查询参数字典。

        将对象的属性转换为API查询参数的字典格式。

        Returns:
            查询参数字典。
        """
        params = self.model_dump(mode="python")
        return params


class Operator(BaseModel):
    """操作符。

    表示单个操作符的详细信息。

    Attributes:
        name: 操作符名称。
        category: 操作符类别。
        scope: 操作符作用范围。
        definition: 操作符定义。
        description: 操作符描述。
        documentation: 操作符文档链接。
        level: 操作符级别。
    """

    name: str
    category: str
    scope: str
    definition: str
    description: str
    documentation: str
    level: str


class Operators(BaseModel):
    """操作符集合。

    包含多个操作符组成的列表。

    Attributes:
        operators: 操作符列表。
    """

    operators: List[Operator]


class SimulationProgressView(BaseModel):
    """模拟进度。

    表示模拟的当前进度。

    Attributes:
        progress: 进度值，范围0-1。
    """

    progress: float


class SingleSimulationResultView(BaseModel):
    """单次模拟结果。

    表示单个Alpha模拟的结果信息。

    Attributes:
        id: 模拟结果的唯一标识符。
        type: Alpha类型。
        status: 模拟状态。
        message: 模拟消息，可选。
        location: 错误位置信息，可选。
        settings: 模拟设置，可选。
        regular: 常规代码，可选。
        alpha: Alpha标识符，可选。
        parent: 父Alpha标识符，可选。
    """

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
        """错误位置。

        表示模拟中出现错误的具体位置。

        Attributes:
            line: 行号，可选。
            start: 开始位置，可选。
            end: 结束位置，可选。
            property: 相关属性，可选。
        """

        line: Optional[int] = None
        start: Optional[int] = None
        end: Optional[int] = None
        property: Optional[str] = None


class MultiSimulationResultView(BaseModel):
    """多次模拟结果。

    表示多个Alpha模拟的结果信息。

    Attributes:
        children: 子模拟结果ID列表。
        status: 模拟状态。
        type: Alpha类型。
        settings: 模拟设置，可选。
    """

    children: List[str]
    status: str
    type: AlphaType
    settings: Optional[SimulationSettingsView] = None


class SingleSimulationPayload(BaseModel):
    """单次模拟请求载荷。

    表示发起单次模拟的请求内容。

    Attributes:
        type: Alpha类型。
        settings: 模拟设置。
        regular: 常规代码。
    """

    type: str
    settings: SimulationSettingsView
    regular: str

    def to_params(self) -> Dict[str, Any]:
        """转换为参数字典。

        将模拟请求转换为API参数字典格式。

        Returns:
            参数字典。
        """
        return {
            "type": self.type,
            "settings": self.settings.model_dump(by_alias=True),
            "regular": self.regular,
        }


class MultiSimulationPayload(RootModel):
    """多次模拟请求载荷。

    表示发起多次模拟的请求内容列表。

    Attributes:
        root: 单次模拟请求列表。
    """

    root: List[SingleSimulationPayload]

    def to_params(self) -> List[Any]:
        """转换为参数列表。

        将多次模拟请求转换为API参数列表格式。

        Returns:
            参数字典的列表。
        """
        return [s.to_params() for s in self.root]


class SelfSimulationActivitiesView(BaseModel):
    """模拟活动统计。

    表示用户的模拟活动统计信息，包括不同时间段的数据。

    Attributes:
        yesterday: 昨日活动统计。
        current: 当前时间段活动统计。
        previous: 上一时间段活动统计。
        ytd: 年度至今活动统计。
        total: 总体活动统计。
        records: 活动记录集合。
        type: 活动类型。
    """

    class Period(BaseModel):
        """活动时间段。

        表示特定时间段的活动统计数据。

        Attributes:
            start: 开始时间。
            end: 结束时间。
            value: 活动值。
        """

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
    """身份验证视图。

    表示身份验证的详细信息，包含用户信息、令牌和权限。

    Attributes:
        user: 用户信息。
        token: 令牌信息。
        permissions: 权限列表。
    """

    class User(BaseModel):
        """用户信息。

        表示已认证用户的基本信息。

        Attributes:
            id: 用户的唯一标识符。
        """

        id: str

    class Token(BaseModel):
        """令牌信息。

        表示身份验证令牌的详细信息。

        Attributes:
            expiry: 令牌过期时间戳。
        """

        expiry: float

    user: User
    token: Token
    permissions: List[str]
