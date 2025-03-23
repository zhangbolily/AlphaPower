from enum import Enum


class ValueType(Enum):
    MATRIX = "Matrix"
    VECTOR = "Vector"
    GROUP = "Group"
    UNIVERSE = "Universe"
    SCALAR = "Scalar"
    EXPRESSION = "Expression"
    SET = "Set"


class Value:
    def __init__(self, val: str, type: ValueType = ValueType.SCALAR):
        self.val: str = val
        self.type: ValueType = type

    def __repr__(self):
        return f"Value(val={self.val}, type={self.type.name})"


class DatafieldSet(Value):
    def __init__(self, val: set[str]):
        super().__init__(val, type=ValueType.SET)

    def __repr__(self):
        return f"DatafieldSet(val={self.val})"
