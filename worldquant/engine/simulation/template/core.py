from enum import Enum
from typing import Any, Dict, Iterable, List, Union


class DataFieldType(Enum):
    MATRIX = "Matrix"
    VECTOR = "Vector"
    GROUP = "Group"
    UNIVERSE = "Universe"


class DataField:
    def __init__(
        self, id: str, description: str, type: DataFieldType = DataFieldType.VECTOR
    ) -> None:
        self.id: str = id
        self.description: str = description
        self.type: DataFieldType = type

    def __add__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("add", [self, other])

    def __sub__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("subtract", [self, other])

    def __mul__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("multiply", [self, other])

    def __truediv__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("divide", [self, other])

    def __eq__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("equal", [self, other])

    def __gt__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("greater", [self, other])

    def __lt__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("less", [self, other])

    def __ge__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("greater_equal", [self, other])

    def __le__(self, other: Union["Expression", "DataField", Any]) -> "Expression":
        return Expression("less_equal", [self, other])

    def __repr__(self) -> str:
        return self.id


class Expression:
    def __init__(
        self,
        op: str,
        args: List[Union["Expression", DataField, Any]],
        params: Dict[str, Any] = None,
    ) -> None:
        self.op: str = op
        self.args: List[Union["Expression", DataField, Any]] = args
        self.params: Dict[str, Any] = params or {}

    def __add__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("add", [self, other])

    def __sub__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("subtract", [self, other])

    def __mul__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("multiply", [self, other])

    def __truediv__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("divide", [self, other])

    def __eq__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("equal", [self, other])

    def __gt__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("greater", [self, other])

    def __lt__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("less", [self, other])

    def __ge__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("greater_equal", [self, other])

    def __le__(self, other: Union["Expression", DataField, Any]) -> "Expression":
        return Expression("less_equal", [self, other])

    def __repr__(self) -> str:
        return self.to_alpha()

    def to_alpha(self) -> str:
        # Convert to platform-specific expression
        return f"{self.op}({', '.join(map(str, self.args))})"

    def to_template(self) -> str:
        # Convert to template expression
        return f"{self.op}({', '.join(map(str, self.args))})"


class BatchGenerator:
    def __init__(self, template: str, param_ranges: Dict[str, Iterable]) -> None:
        self.template: str = template
        self.param_ranges: Dict[str, Iterable] = param_ranges

    def generate(self) -> List[str]:
        # Generate batch expressions
        results: List[str] = []
        for param_set in self._param_combinations():
            expr: str = self.template.format(**param_set)
            results.append(expr)
        return results

    def _param_combinations(self) -> Iterable[Dict[str, Any]]:
        # Generate all combinations of parameters
        from itertools import product

        keys: Iterable[str]
        values: Iterable[Iterable[Any]]
        keys, values = zip(*self.param_ranges.items())
        for combination in product(*values):
            yield dict(zip(keys, combination))

    def export(self, format: str = "csv") -> str:
        # Export generated expressions
        if format == "csv":
            return "\n".join(self.generate())
