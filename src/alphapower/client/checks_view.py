"""
模块名称: checks_view

模块功能:
    提供与统计数据、年度统计数据和盈亏数据相关的视图模型类。这些模型类用于表示提交前后的数据状态，
    并支持数据的序列化和反序列化。

主要类:
    - StatsView: 表示提交前后的统计数据。
    - YearlyStatsRecordView: 表示单个年度的统计数据记录。
    - TableView: 表示年度统计数据集合。
    - PnLRecordView: 表示单个日期的盈亏数据记录。
    - TableView: 表示盈亏数据集合。
    - SubmissionDataView: 表示提交前后的综合数据，包括统计数据、年度统计和盈亏数据。

注意事项:
    - 所有模型类均基于 Pydantic 的 BaseModel，用于数据验证和字段别名支持。
    - 数据字段使用类型注解，确保数据类型的正确性。
"""

from typing import Dict, List, Optional, Type

from pydantic import AliasChoices, BaseModel, Field

from alphapower.constants import AlphaCheckType, CompetitionScoring

from .common_view import TableView
from .models import AlphaCheckItemView


class SubmissionCheckResultView(BaseModel):
    """Alpha检查结果。

    表示对Alpha的检查结果，包括样本内和样本外数据。

    Attributes:
        in_sample: 样本内检查结果，可选。
        out_sample: 样本外检查结果，可选。
    """

    class Sample(BaseModel):
        """样本检查结果。

        表示特定样本集上的检查结果数据。

        Attributes:
            checks: 检查项列表，可选。
            self_correlated: 自相关性数据，可选。
            prod_correlated: 与生产环境相关性数据，可选。
        """

        checks: Optional[List[AlphaCheckItemView]] = None
        self_correlated: Optional[TableView] = Field(
            default=None,
            validation_alias=AliasChoices("selfCorrelated", "self_correlated"),
            serialization_alias="selfCorrelated",
        )
        prod_correlated: Optional[TableView] = Field(
            default=None,
            validation_alias=AliasChoices("prodCorrelated", "prod_correlated"),
            serialization_alias="prodCorrelated",
        )

    in_sample: Optional[Sample] = Field(
        default=None, validation_alias=AliasChoices("is", "in_sample")
    )
    out_sample: Optional[Sample] = Field(
        default=None, validation_alias=AliasChoices("os", "out_sample")
    )


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

        before: TableView
        after: TableView

    class CompetitionRefView(BaseModel):
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

    stats: Stats
    yearly_stats: YearlyStats = Field(..., validation_alias="yearlyStats")
    pnl: TableView
    partition: List[str]
    competition: Optional[CompetitionRefView] = None
    score: Optional[ScoreView] = None


CheckTypeViewMap: Dict[AlphaCheckType, Type[BaseModel]] = {
    AlphaCheckType.CORRELATION_SELF: TableView,
    AlphaCheckType.BEFORE_AND_AFTER_PERFORMANCE: BeforeAndAfterPerformanceView,
    AlphaCheckType.SUBMISSION: SubmissionCheckResultView,
}
