# 初始化一些默认不带 session 的 dal，作为默认的单例供其他模块使用

__all__ = [
    "alpha_dal",
    "competition_dal",
    "aggregate_data_dal",
    "dataset_dal",
    "data_field_dal",
    "pyramid_dal",
    "category_dal",
    "stats_data_dal",
    "research_paper_dal",
    "record_set_dal",
    "check_record_dal",
    "correlation_dal",
    "evaluate_record_dal",
    "simulation_task_dal",
]

from .alphas import AggregateDataDAL, AlphaDAL, CompetitionDAL
from .data import (
    CategoryDAL,
    DataFieldDAL,
    DatasetDAL,
    PyramidDAL,
    ResearchPaperDAL,
    StatsDataDAL,
)
from .evaluate import CheckRecordDAL, CorrelationDAL, EvaluateRecordDAL, RecordSetDAL
from .simulation import SimulationTaskDAL

alpha_dal: AlphaDAL = AlphaDAL()
competition_dal: CompetitionDAL = CompetitionDAL()
aggregate_data_dal: AggregateDataDAL = AggregateDataDAL()
dataset_dal: DatasetDAL = DatasetDAL()
data_field_dal: DataFieldDAL = DataFieldDAL()
pyramid_dal: PyramidDAL = PyramidDAL()
category_dal: CategoryDAL = CategoryDAL()
stats_data_dal: StatsDataDAL = StatsDataDAL()
research_paper_dal: ResearchPaperDAL = ResearchPaperDAL()
record_set_dal: RecordSetDAL = RecordSetDAL()
check_record_dal: CheckRecordDAL = CheckRecordDAL()
correlation_dal: CorrelationDAL = CorrelationDAL()
evaluate_record_dal: EvaluateRecordDAL = EvaluateRecordDAL()
simulation_task_dal: SimulationTaskDAL = SimulationTaskDAL()
