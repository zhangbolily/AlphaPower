# 初始化一些默认不带 session 的 dal，作为默认的单例供其他模块使用

__all__ = [
    "aggregate_data_dal",
    "alpha_dal",
    "alpha_profile_dal",
    "alpha_profile_data_fields_dal",
    "category_dal",
    "check_record_dal",
    "competition_dal",
    "correlation_dal",
    "data_field_dal",
    "data_set_dal",
    "evaluate_record_dal",
    "pyramid_dal",
    "record_set_dal",
    "research_paper_dal",
    "session_manager",
    "SessionManager",
    "simulation_task_dal",
    "stats_data_dal",
]

from .alphas import (
    AggregateDataDAL,
    AlphaDAL,
    AlphaProfileDAL,
    AlphaProfileDataFieldsDAL,
    CompetitionDAL,
)
from .data import (
    CategoryDAL,
    DataFieldDAL,
    DataSetDAL,
    PyramidDAL,
    ResearchPaperDAL,
    StatsDataDAL,
)
from .evaluate import CheckRecordDAL, CorrelationDAL, EvaluateRecordDAL, RecordSetDAL
from .session_manager import SessionManager, session_manager
from .simulation import SimulationTaskDAL

aggregate_data_dal: AggregateDataDAL = AggregateDataDAL()
alpha_dal: AlphaDAL = AlphaDAL()
alpha_profile_dal: AlphaProfileDAL = AlphaProfileDAL()
alpha_profile_data_fields_dal: AlphaProfileDataFieldsDAL = AlphaProfileDataFieldsDAL()
category_dal: CategoryDAL = CategoryDAL()
check_record_dal: CheckRecordDAL = CheckRecordDAL()
competition_dal: CompetitionDAL = CompetitionDAL()
correlation_dal: CorrelationDAL = CorrelationDAL()
data_field_dal: DataFieldDAL = DataFieldDAL()
data_set_dal: DataSetDAL = DataSetDAL()
evaluate_record_dal: EvaluateRecordDAL = EvaluateRecordDAL()
pyramid_dal: PyramidDAL = PyramidDAL()
record_set_dal: RecordSetDAL = RecordSetDAL()
research_paper_dal: ResearchPaperDAL = ResearchPaperDAL()
simulation_task_dal: SimulationTaskDAL = SimulationTaskDAL()
stats_data_dal: StatsDataDAL = StatsDataDAL()
