from datetime import date
from typing import List, Optional

from pydantic import AliasChoices, BaseModel, Field

from alphapower.constants import (
    Delay,
    Region,
)

from .data import DataCategoryRefView


class PyramidAlphasQuery(BaseModel):
    start_date: Optional[date] = Field(
        validation_alias=AliasChoices("start_date", "startDate"),
        serialization_alias="startDate",
    )
    end_date: Optional[date] = Field(
        validation_alias=AliasChoices("end_date", "endDate"),
        serialization_alias="endDate",
    )


class PyramidAlphasView(BaseModel):

    class PyramidAlpha(BaseModel):
        alpha_count: int = Field(
            validation_alias=AliasChoices("alpha_count", "alphaCount"),
            serialization_alias="alphaCount",
        )
        delay: Delay
        region: Region
        category: DataCategoryRefView

    pyramids: Optional[List[PyramidAlpha]]
