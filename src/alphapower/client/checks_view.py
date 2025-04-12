"""
模块名称: checks_view

模块功能:
    提供与统计数据、年度统计数据和盈亏数据相关的视图模型类。这些模型类用于表示提交前后的数据状态，
    并支持数据的序列化和反序列化。

主要类:
    - StatsView: 表示提交前后的统计数据。
    - YearlyStatsRecordView: 表示单个年度的统计数据记录。
    - YearlyStatsView: 表示年度统计数据集合。
    - PnLRecordView: 表示单个日期的盈亏数据记录。
    - PnLView: 表示盈亏数据集合。
    - SubmissionDataView: 表示提交前后的综合数据，包括统计数据、年度统计和盈亏数据。

注意事项:
    - 所有模型类均基于 Pydantic 的 BaseModel，用于数据验证和字段别名支持。
    - 数据字段使用类型注解，确保数据类型的正确性。
"""

from typing import Any, List, Optional

from pydantic import AliasChoices, BaseModel, Field

from alphapower.constants import CompetitionScoring

from .common_view import TableSchemaView


class CompetitionView(BaseModel):
    """比赛视图。
    表示比赛的基本信息。
    Attributes:
        id: 比赛的唯一标识符。
        name: 比赛名称。
        scoring: 比赛评分方式。
    """

    id: str
    name: str
    scoring: CompetitionScoring


class ScoreView(BaseModel):
    """评分视图。
    表示比赛的评分信息。
    Attributes:
        before: 提交前的评分。
        after: 提交后的评分。
    """

    before: float
    after: float


class StatsView(BaseModel):
    """统计数据视图。

    表示提交前后的统计数据。

    Attributes:
        book_size: 账簿规模。
        pnl: 盈亏值。
        long_count: 多头数量。
        short_count: 空头数量。
        drawdown: 回撤。
        turnover: 周转率。
        returns: 回报率。
        margin: 利润率。
        sharpe: 夏普比率。
        fitness: 适应度。
    """

    book_size: int = Field(..., validation_alias="bookSize")
    pnl: float
    long_count: int = Field(..., validation_alias="longCount")
    short_count: int = Field(..., validation_alias="shortCount")
    drawdown: float
    turnover: float
    returns: float
    margin: float
    sharpe: float
    fitness: float


class YearlyStatsView(BaseModel):
    """年度统计视图。

    表示年度统计数据集合。

    Attributes:
        schema: 表格模式。
        records: 年度统计记录列表。
    """

    table_schema: TableSchemaView = Field(
        validation_alias=AliasChoices("schema", "table_schema"),
        serialization_alias="schema",
    )
    records: List[List[Any]]


class PnLView(BaseModel):
    """盈亏视图。

    表示盈亏数据集合。

    Attributes:
        schema: 表格模式。
        records: 盈亏记录列表。
    """

    table_schema: TableSchemaView = Field(
        validation_alias=AliasChoices("schema", "table_schema"),
        serialization_alias="schema",
    )
    records: List[List[Any]]


class BeforeAndAfterPerformanceView(BaseModel):
    """提交数据视图。

    表示提交前后的统计数据、年度统计和盈亏数据。

    Attributes:
        stats: 提交前后的统计数据。
        yearly_stats: 提交前后的年度统计数据。
        pnl: 提交前后的盈亏数据。
        partition: 数据分区字段列表。
    """

    class Stats(BaseModel):
        """提交前后的统计数据视图。
        Attributes:
            before: 提交前的统计数据。
            after: 提交后的统计数据。
        """

        before: StatsView
        after: StatsView

    class YearlyStats(BaseModel):
        """提交前后的年度统计数据视图。
        Attributes:
            before: 提交前的年度统计数据。
            after: 提交后的年度统计数据。
        """

        before: YearlyStatsView
        after: YearlyStatsView

    stats: Stats
    yearly_stats: YearlyStats = Field(..., validation_alias="yearlyStats")
    pnl: PnLView
    partition: List[str]
    competition: Optional[CompetitionView] = None
    score: Optional[ScoreView] = None
