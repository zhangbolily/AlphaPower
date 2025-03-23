from itertools import product
from typing import Iterable, List, Set, Union

from .ops import Value, ValueType


class Expression:
    def __init__(
        self,
        alias: str,
        content: str,
        used_fields: Set[str],
        used_operators: Set[str],
        return_type: ValueType,
    ):
        self.alias: str = alias
        self.content: str = content
        self.used_fields: Set[str] = used_fields
        self.used_operators: Set[str] = used_operators
        self.return_type: ValueType = return_type


class ExpressionNode:
    def __init__(
        self,
        operator: Union[str, Set[str]],
        operands: List[Iterable[Union[Value, "ExpressionNode"]]],
        return_type: ValueType,
    ):
        self.operator: Union[str, Set[str]] = operator
        self.operands: List[Iterable[Union[Value, "ExpressionNode"]]] = operands
        self.return_type: ValueType = return_type

    def _generate_combinations(self):
        """Generate all combinations of operators and operands."""
        operators = self.operator if isinstance(self.operator, set) else {self.operator}
        operand_combinations = product(*self.operands)
        return product(operators, operand_combinations)

    def _dfs(
        self,
        node: Union[Value, "ExpressionNode"],
        used_fields: Set[str],
        used_operators: Set[str],
    ) -> str:
        """Depth-first search to generate expression content."""
        expr: str = ""
        if isinstance(node, ExpressionNode):
            if not isinstance(node.operator, str):
                raise ValueError("Operator must be a string in Expression Generation.")

            used_operators.add(node.operator)
            for operand in node.operands:
                expr = f"{expr}{self._dfs(operand, used_fields, used_operators)},"

            if len(expr) > 1:
                return f"{node.operator}({expr[:-1]})"
            else:
                raise ValueError("Invalid expression generation.")
        elif isinstance(node, Value):
            if node.type in {
                ValueType.GROUP,
                ValueType.UNIVERSE,
                ValueType.MATRIX,
                ValueType.VECTOR,
            }:
                used_fields.add(node.val)
            return node.val

    def end(self) -> List[Expression]:
        """Generate all possible expressions based on combinations."""
        expressions = []

        for operator, operand_combination in self._generate_combinations():
            used_fields = set()
            used_operators = set()
            content = self._dfs(
                # TODO: return type 由 operator 决定
                ExpressionNode(operator, list(operand_combination), self.return_type),
                used_fields,
                used_operators,
            )
            expressions.append(
                Expression(
                    alias="",
                    content=content,
                    used_fields=used_fields,
                    used_operators=used_operators,
                    return_type=self.return_type,
                )
            )

        return expressions
