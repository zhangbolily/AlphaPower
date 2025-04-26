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


class DiversityView(BaseModel):
    class Alpha(BaseModel):
        class DataDiversity(BaseModel):
            check: Optional[str]
            limit: Optional[float]

        region: Region
        delay: Optional[Delay]
        alpha_count: int = Field(
            validation_alias=AliasChoices("alpha_count", "alphaCount"),
            serialization_alias="alphaCount",
        )
        data_diversity: Optional[DataDiversity] = Field(
            validation_alias=AliasChoices("data_diversity", "dataDiversity"),
            serialization_alias="dataDiversity",
        )
        data_category: DataCategoryRefView = Field(
            validation_alias=AliasChoices("data_category", "dataCategory"),
            serialization_alias="dataCategory",
        )

    alphas: List[Alpha]
    count: int
