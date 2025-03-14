from typing import Final, Dict
from enum import Enum

class DataFieldType(Enum):
    MATRIX = "Matrix"
    VECTOR = "Vector"
    GROUP = "Group"
    UNIVERSE = "Universe"
    NUMERIC = "Numeric"

class Value:
    def __init__(self, val, type: DataFieldType=DataFieldType.NUMERIC):
        self.val = val
        self.type = type
    
    def __repr__(self):
        return f"{self.val}"