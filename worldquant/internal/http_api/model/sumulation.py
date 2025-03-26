import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SimulationSettings:
    nanHandling: Optional[str] = None
    instrumentType: Optional[str] = None
    delay: Optional[int] = None
    universe: Optional[str] = None
    truncation: Optional[float] = None
    unitHandling: Optional[str] = None
    testPeriod: Optional[str] = None
    pasteurization: Optional[str] = None
    region: Optional[str] = None
    language: Optional[str] = None
    decay: Optional[int] = None
    neutralization: Optional[str] = None
    visualization: Optional[bool] = None
    maxTrade: Optional[str] = None  # 无权限字段，默认值为 None


@dataclass
class SimulationProgress:
    progress: float

    @classmethod
    def from_json(cls, json_data: str) -> "SimulationProgress":
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
    regular: Optional[str] = None
    alpha: Optional[str] = None
    parent: Optional[str] = None

    @dataclass
    class ErrorLocation:
        line: Optional[int]
        start: Optional[int]
        end: Optional[int]
        property: Optional[str]

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
class MultiSimulationResult:
    children: List[str]
    status: str
    type: str
    settings: SimulationSettings

    @classmethod
    def from_json(cls, json_data: str) -> "MultiSimulationResult":
        try:
            data = json.loads(json_data)
            data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class SingleSimulationRequest:
    type: str
    settings: SimulationSettings
    regular: Dict[str, Any]

    @classmethod
    def from_json(cls, json_data: str) -> "SingleSimulationRequest":
        try:
            data = json.loads(json_data)
            if "settings" in data and data["settings"] is not None:
                data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")

    def to_params(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "settings": self.settings.__dict__,
            "regular": self.regular,
        }


class MultiSimulationRequest(List[SingleSimulationRequest]):
    def to_params(self) -> List[Any]:
        return [s.to_params() for s in self]

    @classmethod
    def from_json(cls, json_data: str) -> "MultiSimulationRequest":
        try:
            data = json.loads(json_data)
            return cls(SingleSimulationRequest(**d) for d in data)
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
