"""
@file: other.py
"""

from typing import List

from pydantic import BaseModel


class Operator(BaseModel):
    """
    操作符类，表示单个操作符的详细信息。

    属性:
        name: 操作符名称
        category: 操作符类别
        scope: 操作符作用范围
        definition: 操作符定义
        description: 操作符描述
        documentation: 操作符文档链接
        level: 操作符级别
    """

    name: str
    category: str
    scope: str
    definition: str
    description: str
    documentation: str
    level: str


class Operators(BaseModel):
    """
    操作符集合类，包含多个操作符。

    属性:
        operators: 操作符列表
    """

    operators: List[Operator]
