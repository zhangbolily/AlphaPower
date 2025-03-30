"""
数据模型模块
用于定义数据结构和处理 JSON 数据
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, RootModel


class DataCategoriesView(BaseModel):
    """
    数据类别视图模型
    表示数据类别的详细信息，包括子类别和统计信息
    """

    id: str  # 类别的唯一标识符
    name: str  # 类别名称
    dataset_count: int = Field(alias="datasetCount")  # 数据集数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    user_count: int = Field(alias="userCount")  # 用户数量
    value_score: float = Field(alias="valueScore")  # 价值评分
    region: str  # 所属区域
    children: List["DataCategoriesView"] = []  # 子类别列表


class DataCategoriesListView(RootModel):
    """
    数据类别列表视图模型
    表示数据类别的集合
    """

    root: Optional[List[DataCategoriesView]] = None  # 数据类别列表


class DataSetsQueryParams(BaseModel):
    """
    数据集查询参数模型
    用于定义查询数据集时的参数
    """

    category: Optional[str] = None  # 数据类别
    delay: Optional[int] = None  # 延迟时间
    instrumentType: Optional[str] = None  # 仪器类型
    limit: Optional[int] = None  # 查询结果限制数量
    offset: Optional[int] = None  # 查询结果偏移量
    region: Optional[str] = None  # 区域
    universe: Optional[str] = None  # 宇宙（范围）

    def to_params(self) -> Dict[str, Any]:
        """
        转换为查询参数字典
        :return: 查询参数字典
        """
        params = self.model_dump(mode="python")
        return params


class DataCategoryView(BaseModel):
    """
    数据类别视图模型
    表示单个数据类别的基本信息
    """

    id: str  # 类别的唯一标识符
    name: str  # 类别名称


class DatasetView(BaseModel):
    """
    数据集视图模型
    表示数据集的详细信息
    """

    id: str  # 数据集的唯一标识符
    name: str  # 数据集名称
    description: str  # 数据集描述
    category: DataCategoryView  # 数据集所属类别
    subcategory: DataCategoryView  # 数据集所属子类别
    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    value_score: float = Field(alias="valueScore")  # 价值评分
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    themes: List[str]  # 主题列表
    research_papers: List["ResearchPaperView"] = Field(
        alias="researchPapers"
    )  # 相关研究论文
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class DatasetListView(BaseModel):
    """
    数据集列表视图模型
    表示数据集的集合
    """

    count: int  # 数据集总数
    results: List[DatasetView] = []  # 数据集列表


class DatasetDataView(BaseModel):
    """
    数据集详细视图模型
    表示数据集的详细统计信息
    """

    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    value_score: float = Field(alias="valueScore")  # 价值评分
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    field_count: int = Field(alias="fieldCount")  # 字段数量
    themes: List[str]  # 主题列表
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class ResearchPaperView(BaseModel):
    """
    研究论文视图模型
    表示研究论文的基本信息
    """

    type: str  # 论文类型
    title: str  # 论文标题
    url: str  # 论文链接


class DatasetDetailView(BaseModel):
    """
    数据集详细信息模型
    包含数据集的基本信息和详细数据
    """

    name: str  # 数据集名称
    description: str  # 数据集描述
    category: DataCategoryView  # 数据集所属类别
    subcategory: DataCategoryView  # 数据集所属子类别
    data: List[DatasetDataView]  # 数据集详细数据
    research_papers: List[ResearchPaperView] = Field(
        alias="researchPapers"
    )  # 相关研究论文


class DataFieldItemView(BaseModel):
    """
    数据字段项视图模型
    表示单个数据字段的详细信息
    """

    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    coverage: str  # 覆盖范围
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    themes: List[str]  # 主题列表


class DataFieldDatasetView(BaseModel):
    """
    数据字段数据集视图模型
    表示数据字段所属的数据集的基本信息
    """

    id: str  # 数据集的唯一标识符
    name: str  # 数据集名称


class DatasetDataFieldsView(BaseModel):
    """
    数据集字段视图模型
    表示数据集的字段信息
    """

    dataset: DataFieldDatasetView  # 数据字段所属数据集
    category: DataCategoryView  # 数据字段所属类别
    subcategory: DataCategoryView  # 数据字段所属子类别
    description: str  # 数据字段描述
    type: str  # 数据字段类型
    data: List[DataFieldItemView]  # 数据字段详细信息


class DataFieldView(BaseModel):
    """
    数据字段模型
    表示单个数据字段的详细信息
    """

    id: str  # 数据字段的唯一标识符
    description: str  # 数据字段描述
    dataset: DataFieldDatasetView  # 数据字段所属数据集
    category: DataCategoryView  # 数据字段所属类别
    subcategory: DataCategoryView  # 数据字段所属子类别
    region: str  # 所属区域
    delay: int  # 延迟时间
    universe: str  # 宇宙（范围）
    type: str  # 数据字段类型
    coverage: str  # 覆盖范围
    user_count: int = Field(alias="userCount")  # 用户数量
    alpha_count: int = Field(alias="alphaCount")  # Alpha 数量
    themes: List[str]  # 主题列表
    pyramid_multiplier: Optional[float] = Field(alias="pyramidMultiplier")  # 金字塔乘数


class DataFieldListView(BaseModel):
    """
    数据字段列表视图模型
    表示数据字段的集合
    """

    count: int  # 数据字段总数
    results: List[DataFieldView] = []  # 数据字段列表


class GetDataFieldsQueryParams(BaseModel):
    """
    获取数据字段查询参数模型
    用于定义查询数据字段时的参数
    """

    dataset_id: str = Field(serialization_alias="dataset.id")  # 数据集 ID
    delay: Optional[int] = None  # 延迟时间
    instrument_type: Optional[str] = Field(
        serialization_alias="instrumentType"
    )  # 仪器类型
    limit: Optional[int] = None  # 查询结果限制数量
    offset: Optional[int] = None  # 查询结果偏移量
    region: Optional[str] = None  # 区域
    universe: Optional[str] = None  # 宇宙（范围）

    def to_params(self) -> Dict[str, Any]:
        """
        转换为查询参数字典
        :return: 查询参数字典
        """
        params = self.model_dump(mode="python")
        return params
