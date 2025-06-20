from typing import List, Optional

from pydantic import AliasChoices, BaseModel, Field, RootModel

from alphapower.constants import DataFieldType, Delay, InstrumentType, Region, Universe

from .common import QueryBase


class DataCategoryRefView(BaseModel):
    id: Optional[str]
    name: Optional[str]


class DatasetRefView(BaseModel):
    id: str
    name: str


class ResearchPaperView(BaseModel):
    type: str
    title: str
    url: str


class DataCategoryView(BaseModel):

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
    region: List[Region] = []
    children: List["DataCategoryView"] = []


class DataCategoryListView(RootModel):

    root: Optional[List[DataCategoryView]] = None


class DatasetsQuery(QueryBase):

    instrument_type: Optional[InstrumentType] = Field(
        validation_alias=AliasChoices("instrumentType", "instrument_type"),
        serialization_alias="instrumentType",
    )
    region: Optional[Region] = None
    delay: Optional[Delay] = None
    universe: Optional[Universe] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    search: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class DatasetView(BaseModel):

    id: str
    name: str
    description: str
    category: DataCategoryRefView
    subcategory: DataCategoryRefView
    region: Region
    delay: Delay
    universe: Universe
    coverage: Optional[float] = None
    value_score: float = Field(
        validation_alias=AliasChoices("valueScore", "value_score"),
        serialization_alias="valueScore",
    )
    user_count: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("userCount", "user_count"),
        serialization_alias="userCount",
    )
    alpha_count: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("alphaCount", "alpha_count"),
        serialization_alias="alphaCount",
    )
    field_count: int = Field(
        validation_alias=AliasChoices("fieldCount", "field_count"),
        serialization_alias="fieldCount",
    )
    themes: List[str]
    research_papers: List[ResearchPaperView] = Field(
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


class DataFieldListQuery(QueryBase):
    dataset_id: str = Field(
        validation_alias=AliasChoices("datasetId", "dataset_id"),
        serialization_alias="dataset.id",
    )
    instrument_type: Optional[InstrumentType] = Field(
        validation_alias=AliasChoices("instrumentType", "instrument_type"),
        serialization_alias="instrumentType",
    )
    region: Optional[Region] = None
    universe: Optional[Universe] = None
    delay: Optional[Delay] = None
    limit: Optional[int] = Field(
        default=None,
        ge=1,
        le=50,
    )
    offset: Optional[int] = Field(
        default=None,
        ge=0,
    )


class DataFieldView(BaseModel):

    id: str
    description: str
    dataset: DatasetRefView
    category: DataCategoryRefView
    subcategory: DataCategoryRefView
    region: Region
    delay: Delay
    universe: Universe
    type: DataFieldType
    coverage: Optional[float] = None
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


class PaginatedDataFieldListView(BaseModel):
    count: int
    results: List[DataFieldView] = []
