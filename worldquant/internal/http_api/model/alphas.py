import json
import logging
from worldquant.internal.http_api.common import *
from dataclasses import dataclass
from datetime import datetime as dt

import pandas as pd


@dataclass
class Pyramid:
    name: str
    multiplier: float


class SelfAlphaListQueryParams:
    def __init__(
        self,
        hidden=None,
        limit=None,
        offset=None,
        order=None,
        status_eq=None,
        status_ne=None,
        date_created_gt=None,
        date_created_lt=None,
    ):
        self.hidden = hidden
        self.limit = limit
        self.offset = offset
        self.order = order
        self.status_eq = status_eq
        self.status_ne = status_ne
        self.date_created_gt = date_created_gt
        self.date_created_lt = date_created_lt

    def to_params(self):
        params = {}
        if self.hidden is not None:
            params["hidden"] = "true" if self.hidden else "false"
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
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


class Alpha_Settings:
    def __init__(
        self,
        instrumentType,
        region,
        universe,
        delay,
        decay,
        neutralization,
        truncation,
        pasteurization,
        unitHandling,
        nanHandling,
        language,
        visualization,
        testPeriod=None,
        maxTrade=None,
    ):
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.delay = delay
        self.decay = decay
        self.neutralization = neutralization
        self.truncation = truncation
        self.pasteurization = pasteurization
        self.unitHandling = unitHandling
        self.nanHandling = nanHandling
        self.language = language
        self.visualization = visualization
        self.testPeriod = testPeriod
        self.maxTrade = maxTrade


class Alpha_Regular:
    def __init__(self, code, description, operatorCount):
        self.code = code
        self.description = description
        self.operatorCount = operatorCount


class Alpha_Sample_Check:
    def __init__(
        self,
        name,
        result,
        limit=None,
        value=None,
        date=None,
        competitions=None,
        message=None,
        year=None,
        pyramids=None,
        startDate=None,
        endDate=None,
        multiplier=None,
    ):
        self.name = name
        self.result = result
        self.limit = limit
        self.value = value
        self.date = dt.fromisoformat(date) if date else None
        self.competitions = competitions
        self.message = message
        self.year = year
        self.pyramids = (
            [Pyramid(**pyramid) for pyramid in pyramids] if pyramids else None
        )
        self.startDate = startDate
        self.endDate = endDate
        self.multiplier = multiplier


class Alpha_Sample:
    def __init__(
        self,
        pnl=None,
        bookSize=None,
        longCount=None,
        shortCount=None,
        turnover=None,
        returns=None,
        drawdown=None,
        margin=None,
        sharpe=None,
        fitness=None,
        startDate=None,
        checks=None,
        selfCorrelation=None,
        prodCorrelation=None,
        osISSharpeRatio=None,
        preCloseSharpeRatio=None,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.sharpe = sharpe
        self.fitness = fitness
        self.startDate = dt.fromisoformat(startDate) if startDate else None
        self.checks = (
            [Alpha_Sample_Check(**check) for check in checks] if checks else None
        )
        self.selfCorrelation = selfCorrelation
        self.prodCorrelation = prodCorrelation
        self.osISSharpeRatio = osISSharpeRatio
        self.preCloseSharpeRatio = preCloseSharpeRatio


class Alpha_Classification:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Alpha_Competition:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Alpha:
    def __init__(
        self,
        id,
        type,
        author,
        settings,
        regular,
        dateCreated,
        dateSubmitted,
        dateModified,
        name,
        favorite,
        hidden,
        color,
        category,
        tags,
        classifications,
        grade,
        stage,
        status,
        inSample,
        outSample,
        train,
        test,
        prod,
        competitions,
        themes,
        pyramids,
        team,
    ):
        try:
            self.id = id
            self.type = type
            self.author = author
            self.settings = Alpha_Settings(**settings)
            self.regular = Alpha_Regular(**regular)
            self.dateCreated = dt.fromisoformat(dateCreated) if dateCreated else None
            self.dateSubmitted = (
                dt.fromisoformat(dateSubmitted) if dateSubmitted else None
            )
            self.dateModified = dt.fromisoformat(dateModified) if dateModified else None
            self.name = name
            self.favorite = favorite
            self.hidden = hidden
            self.color = color
            self.category = category
            self.tags = tags
            self.classifications = [
                Alpha_Classification(**classification)
                for classification in classifications
            ]
            self.grade = grade
            self.stage = stage
            self.status = status
            self.inSample = Alpha_Sample(**inSample) if inSample else None
            self.outSample = Alpha_Sample(**outSample) if outSample else None
            self.train = Alpha_Sample(**train) if train else None
            self.test = Alpha_Sample(**test) if test else None
            self.prod = Alpha_Sample(**prod) if prod else None
            self.competitions = (
                [Alpha_Competition(**competition) for competition in competitions]
                if competitions
                else None
            )
            self.themes = themes
            self.pyramids = (
                [Pyramid(**pyramid) for pyramid in pyramids] if pyramids else None
            )
            self.team = team
        except Exception as e:
            logging.error(
                f"Error parsing Alpha object. Field data: "
                f"id={id}, type={type}, author={author}, settings={settings}, "
                f"regular={regular}, dateCreated={dateCreated}, dateSubmitted={dateSubmitted}, "
                f"dateModified={dateModified}, name={name}, favorite={favorite}, hidden={hidden}, "
                f"color={color}, category={category}, tags={tags}, classifications={classifications}, "
                f"grade={grade}, stage={stage}, status={status}, inSample={inSample}, "
                f"outSample={outSample}, train={train}, test={test}, prod={prod}, "
                f"competitions={competitions}, themes={themes}, pyramids={pyramids}, team={team}",
                exc_info=True,
            )
            raise


class SelfAlphaList:
    def __init__(self, count, next, previous, results):
        self.count = count
        self.next = next
        self.previous = previous
        self.results = [Alpha(**result) for result in results]

    @classmethod
    def from_json(cls, json_data):
        data = json.loads(json_data)

        # 字段名映射
        field_mapping = {
            "is": "inSample",
            "os": "outSample",
        }

        def map_fields(obj):
            if isinstance(obj, dict):
                return {field_mapping.get(k, k): map_fields(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [map_fields(i) for i in obj]
            else:
                return obj

        mapped_data = map_fields(data)
        return cls(**mapped_data)


class AlphaDetail_Settings:
    def __init__(
        self,
        instrumentType,
        region,
        universe,
        delay,
        decay,
        neutralization,
        truncation,
        pasteurization,
        unitHandling,
        nanHandling,
        language,
        visualization,
        testPeriod,
    ):
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.delay = delay
        self.decay = decay
        self.neutralization = neutralization
        self.truncation = truncation
        self.pasteurization = pasteurization
        self.unitHandling = unitHandling
        self.nanHandling = nanHandling
        self.language = language
        self.visualization = visualization
        self.testPeriod = testPeriod


class AlphaDetail_Regular:
    def __init__(self, code, description, operatorCount):
        self.code = code
        self.description = description
        self.operatorCount = operatorCount


class AlphaDetail_IS_Check:
    def __init__(
        self,
        name,
        result,
        limit=None,
        value=None,
        date=None,
        competitions=None,
        message=None,
    ):
        self.name = name
        self.result = result
        self.limit = limit
        self.value = value
        self.date = dt.fromisoformat(date) if date else None
        self.competitions = competitions
        self.message = message


class AlphaDetail_IS:
    def __init__(
        self,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        returns,
        drawdown,
        margin,
        sharpe,
        fitness,
        startDate,
        checks,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.sharpe = sharpe
        self.fitness = fitness
        self.startDate = startDate
        self.checks = [AlphaDetail_IS_Check(**check) for check in checks]


class AlphaDetail_Train:
    def __init__(
        self,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        returns,
        drawdown,
        margin,
        fitness,
        sharpe,
        startDate,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.sharpe = sharpe
        self.startDate = startDate


class AlphaDetail_Test:
    def __init__(
        self,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        returns,
        drawdown,
        margin,
        fitness,
        sharpe,
        startDate,
    ):
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.sharpe = sharpe
        self.startDate = startDate


class AlphaDetail_Classification:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class AlphaDetail:
    def __init__(
        self,
        id,
        type,
        author,
        settings,
        regular,
        dateCreated,
        dateSubmitted,
        dateModified,
        name,
        favorite,
        hidden,
        color,
        category,
        tags,
        classifications,
        grade,
        stage,
        status,
        is_,
        os_,
        train,
        test,
        prod,
        competitions,
        themes,
        pyramids,
        team,
    ):
        try:
            self.id = id
            self.type = type
            self.author = author
            self.settings = AlphaDetail_Settings(**settings)
            self.regular = AlphaDetail_Regular(**regular)
            self.dateCreated = dt.fromisoformat(dateCreated) if dateCreated else None
            self.dateSubmitted = (
                dt.fromisoformat(dateSubmitted) if dateSubmitted else None
            )
            self.dateModified = dt.fromisoformat(dateModified) if dateModified else None
            self.name = name
            self.favorite = favorite
            self.hidden = hidden
            self.color = color
            self.category = category
            self.tags = tags
            self.classifications = [
                AlphaDetail_Classification(**classification)
                for classification in classifications
            ]
            self.grade = grade
            self.stage = stage
            self.status = status
            self.is_ = AlphaDetail_IS(**is_) if is_ else None
            self.os_ = os_
            self.train = AlphaDetail_Train(**train) if train else None
            self.test = AlphaDetail_Test(**test) if test else None
            self.prod = prod
            self.competitions = competitions
            self.themes = themes
            self.pyramids = (
                [Pyramid(**pyramid) for pyramid in pyramids] if pyramids else None
            )
            self.team = team
        except Exception as e:
            logging.error(
                f"Error parsing AlphaDetail object. Field data: "
                f"id={id}, type={type}, author={author}, settings={settings}, "
                f"regular={regular}, dateCreated={dateCreated}, dateSubmitted={dateSubmitted}, "
                f"dateModified={dateModified}, name={name}, favorite={favorite}, hidden={hidden}, "
                f"color={color}, category={category}, tags={tags}, classifications={classifications}, "
                f"grade={grade}, stage={stage}, status={status}, is_={is_}, os_={os_}, "
                f"train={train}, test={test}, prod={prod}, competitions={competitions}, "
                f"themes={themes}, pyramids={pyramids}, team={team}",
                exc_info=True,
            )
            raise

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
            return cls(**data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise


class AlphaYearlyStats_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaYearlyStats_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [AlphaYearlyStats_Property(**prop) for prop in properties]


class AlphaYearlyStats_Record:
    def __init__(
        self,
        year,
        pnl,
        bookSize,
        longCount,
        shortCount,
        turnover,
        sharpe,
        returns,
        drawdown,
        margin,
        fitness,
        stage,
    ):
        self.year = year
        self.pnl = pnl
        self.bookSize = bookSize
        self.longCount = longCount
        self.shortCount = shortCount
        self.turnover = turnover
        self.sharpe = sharpe
        self.returns = returns
        self.drawdown = drawdown
        self.margin = margin
        self.fitness = fitness
        self.stage = stage


class AlphaYearlyStats:
    def __init__(self, schema, records):
        self.schema = AlphaYearlyStats_Schema(**schema)
        self.records = [AlphaYearlyStats_Record(*record) for record in records]

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
            return cls(**data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise

    def to_dataframe(self):
        data = []
        for record in self.records:
            data.append([getattr(record, prop.name) for prop in self.schema.properties])
        return pd.DataFrame(
            data, columns=[prop.title for prop in self.schema.properties]
        )


class AlphaPnL_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaPnL_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [AlphaPnL_Property(**prop) for prop in properties]


class AlphaPnL_Record:
    def __init__(self, date, pnl):
        self.date = dt.fromisoformat(date) if date else None
        self.pnl = pnl


class AlphaPnL:
    def __init__(self, schema, records):
        self.schema = AlphaPnL_Schema(**schema)
        self.records = [AlphaPnL_Record(*record) for record in records]

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
            return cls(**data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise


class AlphaSelfCorrelations_Property:
    def __init__(self, name, title, type):
        self.name = name
        self.title = title
        self.type = type


class AlphaSelfCorrelations_Schema:
    def __init__(self, name, title, properties):
        self.name = name
        self.title = title
        self.properties = [
            AlphaSelfCorrelations_Property(**prop) for prop in properties
        ]


class AlphaSelfCorrelations_Record:
    def __init__(
        self,
        id,
        name,
        instrumentType,
        region,
        universe,
        correlation,
        sharpe,
        returns,
        turnover,
        fitness,
        margin,
    ):
        self.id = id
        self.name = name
        self.instrumentType = instrumentType
        self.region = region
        self.universe = universe
        self.correlation = correlation
        self.sharpe = sharpe
        self.returns = returns
        self.turnover = turnover
        self.fitness = fitness
        self.margin = margin


class AlphaSelfCorrelations:
    def __init__(self, schema, records, min, max):
        self.schema = AlphaSelfCorrelations_Schema(**schema)
        self.records = [AlphaSelfCorrelations_Record(*record) for record in records]
        self.min = min
        self.max = max

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
            return cls(**data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise


class AlphaPropertiesBody_Regular:
    def __init__(self, description):
        self.description = description


class AlphaPropertiesBody:
    def __init__(self, color, name, tags, category, regular):
        self.color = color
        self.name = name
        self.tags = tags
        self.category = category
        self.regular = AlphaPropertiesBody_Regular(**regular)

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
            return cls(**data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise


class AplhaCheckResult:
    def __init__(self, inSample, outSample):
        self.inSample = inSample
        self.outSample = outSample

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)

            # 字段名映射
            field_mapping = {
                "is": "inSample",
                "os": "outSample",
            }

            def map_fields(obj):
                if isinstance(obj, dict):
                    return {
                        field_mapping.get(k, k): map_fields(v) for k, v in obj.items()
                    }
                elif isinstance(obj, list):
                    return [map_fields(i) for i in obj]
                else:
                    return obj

            mapped_data = map_fields(data)
            return cls(**mapped_data)
        except Exception as e:
            logging.error(f"Failed to parse JSON: {json_data}", exc_info=True)
            raise
