from datetime import datetime

from pydantic import BaseModel, TypeAdapter


class ScoreResult(BaseModel):
    """
    评分结果类
    """

    start_date: datetime
    end_date: datetime
    score: float


ScoreResultListAdapter = TypeAdapter(list[ScoreResult])
