import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SimulationSettings:
    nanHandling: str
    instrumentType: str
    delay: int
    universe: str
    truncation: str
    unitHandling: str
    testPeriod: str
    pasteurization: str
    region: str
    language: str
    decay: str
    neutralization: str
    visualization: str


@dataclass
class SingleSimulationProgress:
    progress: float

    @classmethod
    def from_json(cls, json_data: str) -> "SingleSimulationProgress":
        try:
            data = json.loads(json_data)
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class SingleSimulationResult:
    id: str
    type: str
    status: str
    message: Optional[str] = None
    location: Optional["SingleSimulationResult.ErrorLocation"] = None
    settings: Optional[SimulationSettings] = None
    regular: Optional[Dict[str, Any]] = None
    alpha: Optional[Dict[str, Any]] = None

    @dataclass
    class ErrorLocation:
        line: int
        start: int
        end: int
        property: str

    @classmethod
    def from_json(cls, json_data: str) -> "SingleSimulationResult":
        try:
            data = json.loads(json_data)
            if "location" in data and data["location"] is not None:
                data["location"] = SingleSimulationResult.ErrorLocation(
                    **data["location"]
                )
            if "settings" in data and data["settings"] is not None:
                data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class CreateSingleSimulationReq:
    type: str
    settings: SimulationSettings
    regular: Dict[str, Any]

    @classmethod
    def from_json(cls, json_data: str) -> "CreateSingleSimulationReq":
        try:
            data = json.loads(json_data)
            if "settings" in data and data["settings"] is not None:
                data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class SelfSimulationActivities:
    yesterday: "SelfSimulationActivities.Period"
    current: "SelfSimulationActivities.Period"
    previous: "SelfSimulationActivities.Period"
    ytd: "SelfSimulationActivities.Period"
    total: "SelfSimulationActivities.Period"
    records: "SelfSimulationActivities.Records"
    type: str

    @dataclass
    class Period:
        start: str
        end: str
        value: float

    @dataclass
    class Records:
        schema: "SelfSimulationActivities.Records.Schema"
        records: List[Dict[str, Any]]

        @dataclass
        class Schema:
            name: str
            title: str
            properties: List["SelfSimulationActivities.Records.Schema.Property"]

            @dataclass
            class Property:
                name: str
                title: str
                type: str

            @classmethod
            def from_json(
                cls, json_data: str
            ) -> "SelfSimulationActivities.Records.Schema":
                try:
                    data = json.loads(json_data)
                    if "properties" in data and data["properties"] is not None:
                        data["properties"] = [
                            SelfSimulationActivities.Records.Schema.Property(**d)
                            for d in data["properties"]
                        ]
                    return cls(**data)
                except (json.JSONDecodeError, TypeError) as e:
                    raise ValueError(f"Invalid JSON data: {e}")

        @classmethod
        def from_json(cls, json_data: str) -> "SelfSimulationActivities.Records":
            try:
                data = json.loads(json_data)
                if "schema" in data and data["schema"] is not None:
                    data["schema"] = SelfSimulationActivities.Records.Schema.from_json(
                        data["schema"]
                    )
                return cls(**data)
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError(f"Invalid JSON data: {e}")

    @classmethod
    def from_json(cls, json_data: str) -> "SelfSimulationActivities":
        try:
            data = json.loads(json_data)
            if "yesterday" in data and data["yesterday"] is not None:
                data["yesterday"] = SelfSimulationActivities.Period(**data["yesterday"])
            if "current" in data and data["current"] is not None:
                data["current"] = SelfSimulationActivities.Period(**data["current"])
            if "previous" in data and data["previous"] is not None:
                data["previous"] = SelfSimulationActivities.Period(**data["previous"])
            if "ytd" in data and data["ytd"] is not None:
                data["ytd"] = SelfSimulationActivities.Period(**data["ytd"])
            if "total" in data and data["total"] is not None:
                data["total"] = SelfSimulationActivities.Period(**data["total"])
            if "records" in data and data["records"] is not None:
                data["records"] = SelfSimulationActivities.Records.from_json(
                    data["records"]
                )
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")
