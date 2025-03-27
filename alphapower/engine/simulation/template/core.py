from enum import Enum
from itertools import product
from typing import Any, Dict, Iterable, List, Set, Union


class DataFieldType(Enum):
    MATRIX = "Matrix"
    VECTOR = "Vector"
    GROUP = "Group"
    UNIVERSE = "Universe"


class DataField:
    def __init__(
        self,
        field_id: str,
        description: str,
        field_type: DataFieldType = DataFieldType.VECTOR,
    ) -> None:
        self.field_id: str = field_id
        self.description: str = description
        self.field_type: DataFieldType = field_type

    def __add__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("add", [self, other])

    def __sub__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("subtract", [self, other])

    def __mul__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("multiply", [self, other])

    def __truediv__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("divide", [self, other])

    def __repr__(self) -> str:
        return self.field_id

    def compile(self) -> Iterable[str]:
        return list([self.field_id])


class DataFieldSet:
    def __init__(self, fields: Iterable[DataField], fields_type: DataFieldType) -> None:
        self.fields: Set[DataField] = set(fields)
        self.fields_type: DataFieldType = fields_type

    def compile(self) -> Iterable[str]:
        for field in self.fields:
            yield from field.compile()


class Expression:
    def __init__(
        self,
        operator: str,
        operands: List[Union["Expression", DataField, DataFieldSet, Any]],
        parameters: Dict[str, Any] = {},
    ) -> None:
        self.operator: str = operator
        self.operands: List[Union["Expression", DataField, DataFieldSet, Any]] = (
            operands
        )
        self.parameters: Dict[str, Any] = parameters or {}

    def __add__(
        self, other: Union["Expression", DataField, DataFieldSet, Any]
    ) -> "Expression":
        return Expression("add", [self, other])

    def __sub__(
        self, other: Union["Expression", DataField, DataFieldSet, Any]
    ) -> "Expression":
        return Expression("subtract", [self, other])

    def __mul__(
        self, other: Union["Expression", DataField, DataFieldSet, Any]
    ) -> "Expression":
        return Expression("multiply", [self, other])

    def __truediv__(
        self, other: Union["Expression", DataField, DataFieldSet, Any]
    ) -> "Expression":
        return Expression("divide", [self, other])

    def compile(self) -> Iterable[str]:
        operand_combinations = (
            combination
            for combination in product(
                *(
                    (
                        operand.compile()
                        if isinstance(operand, (Expression, DataField, DataFieldSet))
                        else [str(operand)]
                    )
                    for operand in self.operands
                )
            )
        )

        for combination in operand_combinations:
            yield f"{self.operator}({', '.join(list(combination) + [f'{k}={v}' for k, v in self.parameters.items()])})"
