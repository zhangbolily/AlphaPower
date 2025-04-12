"""common_view 模块。

此模块定义了通用的视图模型，用于描述表格数据的结构和属性。
主要包含 TableSchemaView 类及其嵌套的 Property 类，用于表示表格模式及其属性。

Classes:
    TableSchemaView: 表格模式类，描述表格数据的结构。
"""

from typing import List

from pydantic import BaseModel


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

        描述模式中的单个属性，包括名称、标题和类型。

        Attributes:
            name (str): 属性的名称，用于唯一标识该属性。
            title (str): 属性的标题，通常用于显示给用户。
            type (str): 属性的数据类型（Data Type），例如 "string"、"integer" 等。
        """

        name: str
        title: str
        type: str

    name: str
    title: str
    properties: List[Property]
