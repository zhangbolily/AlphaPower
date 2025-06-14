# -*- coding: utf-8 -*-
from typing import Any, Dict, Optional

from pydantic import AliasChoices, BaseModel, Field


class Choice(BaseModel):
    value: Any
    label: str


class FieldSchema(BaseModel):
    type: str
    required: bool
    read_only: bool = Field(
        ...,
        validation_alias=AliasChoices("readOnly", "read_only"),
        serialization_alias="readOnly",
    )
    label: str
    choices: Optional[Any] = None
    children: Optional[Dict[str, Any]] = None
    child: Optional[Dict[str, Any]] = None
    min_value: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("minValue", "min_value"),
        serialization_alias="minValue",
    )
    max_value: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("maxValue", "max_value"),
        serialization_alias="maxValue",
    )


class NestedFieldSchema(BaseModel):
    type: str
    required: bool
    readOnly: bool
    label: str
    children: Optional[Dict[str, Any]] = None
    child: Optional[Dict[str, Any]] = None


class SimulationsOptionsSettings(BaseModel):
    instrument_type: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("instrumentType", "instrument_type"),
        serialization_alias="instrumentType",
    )
    region: FieldSchema
    universe: FieldSchema
    delay: FieldSchema
    decay: FieldSchema
    neutralization: FieldSchema
    truncation: FieldSchema
    lookback_days: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("lookbackDays", "lookback_days"),
        serialization_alias="lookbackDays",
    )
    pasteurization: FieldSchema
    unit_handling: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("unitHandling", "unit_handling"),
        serialization_alias="unitHandling",
    )
    nan_handling: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("nanHandling", "nan_handling"),
        serialization_alias="nanHandling",
    )
    selection_handling: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("selectionHandling", "selection_handling"),
        serialization_alias="selectionHandling",
    )
    selection_limit: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("selectionLimit", "selection_limit"),
        serialization_alias="selectionLimit",
    )
    max_trade: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("maxTrade", "max_trade"),
        serialization_alias="maxTrade",
    )
    language: FieldSchema
    visualization: FieldSchema
    test_period: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("testPeriod", "test_period"),
        serialization_alias="testPeriod",
    )


class SimulationsOptionsActionsPOST(BaseModel):

    id: FieldSchema
    parent: FieldSchema
    children: FieldSchema
    type: FieldSchema
    settings: FieldSchema
    regular: FieldSchema
    combo: FieldSchema
    selection: FieldSchema
    status: FieldSchema
    message: FieldSchema
    location: FieldSchema
    links: FieldSchema
    progress: FieldSchema
    alpha: FieldSchema
    visualizations: FieldSchema
    is_: FieldSchema = Field(..., alias="is")


class SimulationsOptionsActions(BaseModel):
    POST: SimulationsOptionsActionsPOST


class SimulationsOptions(BaseModel):
    actions: SimulationsOptionsActions


class ActionsGET(BaseModel):
    id: FieldSchema
    type: FieldSchema
    author: FieldSchema
    settings: NestedFieldSchema
    regular: NestedFieldSchema
    combo: NestedFieldSchema
    selection: NestedFieldSchema
    date_created: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("dateCreated", "date_created"),
        serialization_alias="dateCreated",
    )
    date_submitted: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("dateSubmitted", "date_submitted"),
        serialization_alias="dateSubmitted",
    )
    date_modified: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("dateModified", "date_modified"),
        serialization_alias="dateModified",
    )
    name: FieldSchema
    favorite: FieldSchema
    hidden: FieldSchema
    color: FieldSchema
    category: FieldSchema
    tags: FieldSchema
    classifications: FieldSchema
    grade: FieldSchema
    stage: FieldSchema
    status: FieldSchema
    is_: NestedFieldSchema = Field(..., alias="is")
    os: NestedFieldSchema
    train: NestedFieldSchema
    test: NestedFieldSchema
    prod: NestedFieldSchema
    competitions: FieldSchema
    themes: FieldSchema
    pyramids: FieldSchema
    pyramid_themes: FieldSchema = Field(
        ...,
        validation_alias=AliasChoices("pyramidThemes", "pyramid_themes"),
        serialization_alias="pyramidThemes",
    )
    team: FieldSchema


class Actions(BaseModel):
    GET: ActionsGET


class AlphasOptions(BaseModel):
    actions: Actions
