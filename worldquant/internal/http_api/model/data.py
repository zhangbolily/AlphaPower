import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DataCategoriesChild:
    id: str
    name: str
    datasetCount: int
    fieldCount: int
    alphaCount: int
    userCount: int
    valueScore: float
    region: str
    children: List["DataCategoriesChild"] = field(default_factory=list)

    def __post_init__(self):
        self.children = [DataCategoriesChild(**child) for child in self.children]


@dataclass
class DataCategoriesParent(DataCategoriesChild):
    @classmethod
    def from_json(cls, json_data: str) -> List["DataCategoriesParent"]:
        try:
            data = json.loads(json_data)
            return [cls(**item) for item in data] if data else []
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class DataSetsQueryParams:
    category: Optional[str] = None
    delay: Optional[int] = None
    instrumentType: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    region: Optional[str] = None
    universe: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:
        return {k: v for k, v in vars(self).items() if v is not None}


@dataclass
class DataSets_ItemCategory:
    id: str
    name: str


@dataclass
class DataSets_ItemSubcategory:
    id: str
    name: str


@dataclass
class DataSets_Item:
    id: str
    name: str
    description: str
    category: DataSets_ItemCategory
    subcategory: DataSets_ItemSubcategory
    region: str
    delay: int
    universe: str
    coverage: str
    valueScore: float
    userCount: int
    alphaCount: int
    fieldCount: int
    themes: List[str]
    researchPapers: List[str]
    pyramidMultiplier: Optional[float] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "DataSets_Item":
        data["category"] = (
            DataSets_ItemCategory(**data["category"]) if data.get("category") else None
        )
        data["subcategory"] = (
            DataSets_ItemSubcategory(**data["subcategory"])
            if data.get("subcategory")
            else None
        )
        return cls(**data)


@dataclass
class DataSets:
    count: int
    results: List[DataSets_Item] = field(default_factory=list)

    @classmethod
    def from_json(cls, json_data: str) -> "DataSets":
        try:
            data = json.loads(json_data)
            data["results"] = (
                [DataSets_Item.from_json(item) for item in data["results"]]
                if data.get("results")
                else []
            )
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class DatasetDetail_Category:
    id: str
    name: str


@dataclass
class DatasetDetail_Subcategory:
    id: str
    name: str


@dataclass
class DatasetDetail_DataItem:
    region: str
    delay: int
    universe: str
    coverage: str
    valueScore: float
    userCount: int
    alphaCount: int
    fieldCount: int
    themes: List[str]
    pyramidMultiplier: Optional[float] = None


@dataclass
class DatasetDetail_ResearchPaper:
    type: str
    title: str
    url: str


@dataclass
class DatasetDetail:
    name: str
    description: str
    category: DatasetDetail_Category
    subcategory: DatasetDetail_Subcategory
    data: List[DatasetDetail_DataItem]
    researchPapers: List[DatasetDetail_ResearchPaper]

    @classmethod
    def from_json(cls, json_data: str) -> "DatasetDetail":
        try:
            data = json.loads(json_data)
            data["category"] = (
                DatasetDetail_Category(**data["category"])
                if data.get("category")
                else None
            )
            data["subcategory"] = (
                DatasetDetail_Subcategory(**data["subcategory"])
                if data.get("subcategory")
                else None
            )
            data["data"] = (
                [DatasetDetail_DataItem(**item) for item in data["data"]]
                if data.get("data")
                else []
            )
            data["researchPapers"] = (
                [
                    DatasetDetail_ResearchPaper(**paper)
                    for paper in data["researchPapers"]
                ]
                if data.get("researchPapers")
                else []
            )
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class DataFieldDetail_Category:
    id: str
    name: str


@dataclass
class DataFieldDetail_Subcategory:
    id: str
    name: str


@dataclass
class DataFieldDetail_DataItem:
    region: str
    delay: int
    universe: str
    coverage: str
    userCount: int
    alphaCount: int
    themes: List[str]


@dataclass
class DataFieldDetail_Dataset:
    id: str
    name: str


@dataclass
class DataFieldDetail:
    dataset: DataFieldDetail_Dataset
    category: DataFieldDetail_Category
    subcategory: DataFieldDetail_Subcategory
    description: str
    type: str
    data: List[DataFieldDetail_DataItem]

    @classmethod
    def from_json(cls, json_data: str) -> "DataFieldDetail":
        try:
            data = json.loads(json_data)
            data["dataset"] = (
                DataFieldDetail_Dataset(**data["dataset"])
                if data.get("dataset")
                else None
            )
            data["category"] = (
                DataFieldDetail_Category(**data["category"])
                if data.get("category")
                else None
            )
            data["subcategory"] = (
                DataFieldDetail_Subcategory(**data["subcategory"])
                if data.get("subcategory")
                else None
            )
            data["data"] = (
                [DataFieldDetail_DataItem(**item) for item in data["data"]]
                if data.get("data")
                else []
            )
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class DataField_Dataset:
    id: str
    name: str


@dataclass
class DataField_Category:
    id: str
    name: str


@dataclass
class DataField_Subcategory:
    id: str
    name: str


@dataclass
class DataField:
    id: str
    description: str
    dataset: DataField_Dataset
    category: DataField_Category
    subcategory: DataField_Subcategory
    region: str
    delay: int
    universe: str
    type: str
    coverage: str
    userCount: int
    alphaCount: int
    themes: List[str]
    pyramidMultiplier: Optional[float] = None

    def __post_init__(self):
        self.dataset = DataField_Dataset(**self.dataset) if self.dataset else None
        self.category = DataField_Category(**self.category) if self.category else None
        self.subcategory = (
            DataField_Subcategory(**self.subcategory) if self.subcategory else None
        )


@dataclass
class DatasetDataFields:
    count: int
    results: List[DataField] = field(default_factory=list)

    @classmethod
    def from_json(cls, json_data: str) -> "DatasetDataFields":
        try:
            data = json.loads(json_data)
            data["results"] = (
                [DataField(**item) for item in data["results"]]
                if data.get("results")
                else []
            )
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON data: {e}")


@dataclass
class GetDataFieldsQueryParams:
    dataset_id: str
    delay: Optional[int] = None
    instrumentType: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    region: Optional[str] = None
    universe: Optional[str] = None

    def to_params(self) -> Dict[str, Any]:
        # 将类属性转换为请求参数字典
        params = {k: v for k, v in vars(self).items() if v is not None}
        params["dataset.id"] = params.pop(
            "dataset_id", None
        )  # 将 dataset_id 重命名为 dataset.id
        return params
