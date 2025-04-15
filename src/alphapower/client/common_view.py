"""common_view 模块。

此模块定义了通用的视图模型，用于描述表格数据的结构和属性。
主要包含 TableSchemaView 类及其嵌套的 Property 类，用于表示表格模式及其属性。

Classes:
    TableSchemaView: 表格模式类，描述表格数据的结构。
    TableView: 表格视图类，包含表格模式和数据记录。
"""

from typing import Any, List, Optional

from pydantic import AliasChoices, BaseModel, Field


class TableSchemaView(BaseModel):
    """表格模式（Table Schema）。

    描述表格数据的结构，包括名称、标题和属性列表。

    Attributes:
        name (str): 模式的名称，用于唯一标识表格模式。
        title (str): 模式的标题，通常用于显示给用户。
        properties (List[Property]): 模式包含的属性列表，每个属性描述表格的一列。
    """

    class Property(BaseModel):
        """模式属性（Schema Property）。

        描述模式中的单个属性，包括名称、标题和数据类型。

        Attributes:
            name (str): 属性的名称，用于唯一标识该属性。
            title (str): 属性的标题，通常用于显示给用户。
            data_type (str): 属性的数据类型（Data Type），例如 "string"、"integer" 等。
        """

        name: str
        title: str
        data_type: str = Field(
            validation_alias=AliasChoices("type", "data_type"),
            serialization_alias="type",
        )  # 兼容 'type' 字段输入

    name: str
    title: str
    properties: List[Property]

    def index_of(self, name: str) -> int:
        """获取属性在模式中的索引。

        Args:
            name (str): 属性的名称。

        Returns:
            int: 属性在模式中的索引，如果不存在则返回 -1。
        """
        for i, prop in enumerate(self.properties):
            if prop.name == name:
                return i
        return -1


class TableView(BaseModel):
    """表格视图（Table View）。

    包含表格模式和实际的数据记录。

    Attributes:
        table_schema (TableSchemaView): 表格的模式定义。
        records (Optional[List[List[Any]]]): 表格的数据记录，是一个二维列表。
        min (Optional[float]): 可选的最小值，可能用于数据可视化。
        max (Optional[float]): 可选的最大值，可能用于数据可视化。
    """

    table_schema: TableSchemaView = Field(
        validation_alias=AliasChoices("schema", "table_schema"),
        serialization_alias="schema",
    )
    records: Optional[List[List[Any]]] = None
    min: Optional[float] = 0.0
    max: Optional[float] = 0.0
