"""
此模块定义了与模拟相关的数据模型，包括设置、进度、结果和请求等。
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, RootModel

from .base import TableSchema


class SimulationSettings(BaseModel):
    """
    表示模拟的设置。
    """

    nan_handling: Optional[str] = Field(None, alias="nanHandling")
    instrument_type: Optional[str] = Field(None, alias="instrumentType")
    delay: Optional[int] = None
    universe: Optional[str] = None
    truncation: Optional[float] = None
    unit_handling: Optional[str] = Field(None, alias="unitHandling")
    test_period: Optional[str] = Field(None, alias="testPeriod")
    pasteurization: Optional[str] = None
    region: Optional[str] = None
    language: Optional[str] = None
    decay: Optional[int] = None
    neutralization: Optional[str] = None
    visualization: Optional[bool] = None
    max_trade: Optional[str] = Field(
        None, alias="maxTrade"
    )  # 无权限字段，默认值为 None


class SimulationProgress(BaseModel):
    """
    表示模拟的进度。
    """

    progress: float


class SingleSimulationResult(BaseModel):
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

    class ErrorLocation(BaseModel):
        """
        表示模拟中错误的位置。
        """

        line: Optional[int] = None
        start: Optional[int] = None
        end: Optional[int] = None
        property: Optional[str] = None


class MultiSimulationResult(BaseModel):
    """
    表示多次模拟的结果。
    """

    children: List[str]
    status: str
    type: str
    settings: Optional[SimulationSettings] = None


class SingleSimulationRequest(BaseModel):
    """
    表示单次模拟的请求。
    """

    type: str
    settings: SimulationSettings
    regular: str

    def to_params(self) -> Dict[str, Any]:
        """
        将模拟请求转换为参数字典。

        返回:
            Dict[str, Any]: 参数字典。
        """
        return {
            "type": self.type,
            "settings": self.settings.dict(by_alias=True),
            "regular": self.regular,
        }


class MultiSimulationRequest(RootModel):
    """
    表示多次模拟的请求。
    """

    root: List[SingleSimulationRequest]

    def to_params(self) -> List[Any]:
        """
        将模拟请求转换为参数字典的列表。

        返回:
            List[Any]: 参数字典的列表。
        """
        return [s.to_params() for s in self.root]


class SelfSimulationActivities(BaseModel):
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

    class Period(BaseModel):
        """
        表示模拟活动中的一个时间段。
        """

        start: str
        end: str
        value: float

    class Records(BaseModel):
        """
        表示模拟活动的记录。
        """

        table_schema: TableSchema = Field(alias="schema")
        records: List[Dict[str, Any]]
