"""
此模块定义了与模拟相关的数据模型，包括设置、进度、结果和请求等。
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SimulationSettings:
    """
    表示模拟的设置。
    """

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
    """
    表示模拟的进度。
    """

    progress: float

    @classmethod
    def from_json(cls, json_data: str) -> "SimulationProgress":
        """
        从 JSON 字符串创建一个 SimulationProgress 实例。

        参数:
            json_data (str): 表示模拟进度的 JSON 字符串。

        返回:
            SimulationProgress: SimulationProgress 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
        try:
            data = json.loads(json_data)
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}") from e


@dataclass
class SingleSimulationResult:
    """
    表示单次模拟的结果。
    """

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
        """
        表示模拟中错误的位置。
        """

        line: Optional[int]
        start: Optional[int]
        end: Optional[int]
        property: Optional[str]

    @classmethod
    def from_json(cls, json_data: str) -> "SingleSimulationResult":
        """
        从 JSON 字符串创建一个 SingleSimulationResult 实例。

        参数:
            json_data (str): 表示模拟结果的 JSON 字符串。

        返回:
            SingleSimulationResult: SingleSimulationResult 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
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
            raise ValueError(f"Invalid JSON data: {e}") from e


@dataclass
class MultiSimulationResult:
    """
    表示多次模拟的结果。
    """

    children: List[str]
    status: str
    type: str
    settings: SimulationSettings

    @classmethod
    def from_json(cls, json_data: str) -> "MultiSimulationResult":
        """
        从 JSON 字符串创建一个 MultiSimulationResult 实例。

        参数:
            json_data (str): 表示模拟结果的 JSON 字符串。

        返回:
            MultiSimulationResult: MultiSimulationResult 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
        try:
            data = json.loads(json_data)
            data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}") from e


@dataclass
class SingleSimulationRequest:
    """
    表示单次模拟的请求。
    """

    type: str
    settings: SimulationSettings
    regular: str

    @classmethod
    def from_json(cls, json_data: str) -> "SingleSimulationRequest":
        """
        从 JSON 字符串创建一个 SingleSimulationRequest 实例。

        参数:
            json_data (str): 表示模拟请求的 JSON 字符串。

        返回:
            SingleSimulationRequest: SingleSimulationRequest 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
        try:
            data = json.loads(json_data)
            if "settings" in data and data["settings"] is not None:
                data["settings"] = SimulationSettings(**data["settings"])
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}") from e

    def to_params(self) -> Dict[str, Any]:
        """
        将模拟请求转换为参数字典。

        返回:
            Dict[str, Any]: 参数字典。
        """
        return {
            "type": self.type,
            "settings": self.settings.__dict__,
            "regular": self.regular,
        }


class MultiSimulationRequest(List[SingleSimulationRequest]):
    """
    表示多次模拟的请求。
    """

    def to_params(self) -> List[Any]:
        """
        将模拟请求转换为参数字典的列表。

        返回:
            List[Any]: 参数字典的列表。
        """
        return [s.to_params() for s in self]

    @classmethod
    def from_json(cls, json_data: str) -> "MultiSimulationRequest":
        """
        从 JSON 字符串创建一个 MultiSimulationRequest 实例。

        参数:
            json_data (str): 表示模拟请求的 JSON 字符串。

        返回:
            MultiSimulationRequest: MultiSimulationRequest 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
        try:
            data = json.loads(json_data)
            return cls(SingleSimulationRequest(**d) for d in data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}") from e


@dataclass
class SelfSimulationActivities:
    """
    表示自模拟活动。
    """

    yesterday: "SelfSimulationActivities.Period"
    current: "SelfSimulationActivities.Period"
    previous: "SelfSimulationActivities.Period"
    ytd: "SelfSimulationActivities.Period"
    total: "SelfSimulationActivities.Period"
    records: "SelfSimulationActivities.Records"
    type: str

    @dataclass
    class Period:
        """
        表示模拟活动中的一个时间段。
        """

        start: str
        end: str
        value: float

    @dataclass
    class Records:
        """
        表示模拟活动的记录。
        """

        schema: "SelfSimulationActivities.Records.Schema"
        records: List[Dict[str, Any]]

        @dataclass
        class Schema:
            """
            表示记录的模式。
            """

            name: str
            title: str
            properties: List["SelfSimulationActivities.Records.Schema.Property"]

            @dataclass
            class Property:
                """
                表示模式中的一个属性。
                """

                name: str
                title: str
                type: str

            @classmethod
            def from_json(
                cls, json_data: str
            ) -> "SelfSimulationActivities.Records.Schema":
                """
                从 JSON 字符串创建一个 Schema 实例。

                参数:
                    json_data (str): 表示模式的 JSON 字符串。

                返回:
                    Schema: Schema 的实例。

                异常:
                    ValueError: 如果 JSON 数据无效。
                """
                try:
                    data = json.loads(json_data)
                    if "properties" in data and data["properties"] is not None:
                        data["properties"] = [
                            SelfSimulationActivities.Records.Schema.Property(**d)
                            for d in data["properties"]
                        ]
                    return cls(**data)
                except (json.JSONDecodeError, TypeError) as e:
                    raise ValueError(f"Invalid JSON data: {e}") from e

        @classmethod
        def from_json(cls, json_data: str) -> "SelfSimulationActivities.Records":
            """
            从 JSON 字符串创建一个 Records 实例。

            参数:
                json_data (str): 表示记录的 JSON 字符串。

            返回:
                Records: Records 的实例。

            异常:
                ValueError: 如果 JSON 数据无效。
            """
            try:
                data = json.loads(json_data)
                if "schema" in data and data["schema"] is not None:
                    data["schema"] = SelfSimulationActivities.Records.Schema.from_json(
                        data["schema"]
                    )
                return cls(**data)
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError(f"Invalid JSON data: {e}") from e

    @classmethod
    def from_json(cls, json_data: str) -> "SelfSimulationActivities":
        """
        从 JSON 字符串创建一个 SelfSimulationActivities 实例。

        参数:
            json_data (str): 表示模拟活动的 JSON 字符串。

        返回:
            SelfSimulationActivities: SelfSimulationActivities 的实例。

        异常:
            ValueError: 如果 JSON 数据无效。
        """
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
            raise ValueError(f"Invalid JSON data: {e}") from e
