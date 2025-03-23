from itertools import product
from typing import Iterable, List, Set, Union

from .base import Value, ValueType


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
        operator: Union[str, Set[str], List[str]],
        operands: List[Iterable[Union[Value, "ExpressionNode"]]],
        return_type: ValueType,
    ):
        # 提前检查 operator 的合法性
        if isinstance(operator, list):
            for op in operator:
                if op.count("{}") != len(operands):
                    raise ValueError(
                        f"Operator '{op}' has mismatched placeholders for operands."
                    )
        elif not isinstance(operator, (str, set)):
            raise TypeError("Operator must be a string, set, or list.")

        self.operator: Union[str, Set[str], List[str]] = operator
        self.operands: List[Iterable[Union[Value, "ExpressionNode"]]] = operands
        self.return_type: ValueType = return_type

    def _generate_combinations(self):
        """Generate all combinations of operators and operands."""
        if isinstance(self.operator, set):
            operators = self.operator
        elif isinstance(self.operator, str):
            operators = {self.operator}
        elif isinstance(self.operator, list):
            operators = set(self.operator)
        else:
            raise TypeError("Operator must be a string, set, or list.")

        operand_combinations = product(*self.operands)
        return product(operators, operand_combinations)

    def _dfs(
        self,
        node: Union[Value, "ExpressionNode"],
        used_fields: Set[str],
        used_operators: Set[str],
    ) -> List[Expression]:
        """Depth-first search to generate expression content."""
        exprs: List[Expression] = []

        if isinstance(node, ExpressionNode):
            if isinstance(node.operator, set) or isinstance(node.operator, list):
                operators = node.operator
                for operator in operators:
                    exprs += ExpressionNode(
                        operator, node.operands, node.return_type
                    ).end()
                return exprs
            elif not isinstance(node.operator, str):
                raise TypeError("Operator must be a string, set, or list.")

            _operands = []
            for operand in node.operands:
                if isinstance(operand, Value):
                    used_fields.add(operand.val)
                    _operands.append([operand])  # 保留 Value 类型
                elif isinstance(operand, ExpressionNode):
                    sub_exprs = self._dfs(operand, set(), set())  # 递归调用，独立路径
                    _operands.append(sub_exprs)
                else:
                    raise TypeError("Operand must be a Value or ExpressionNode.")

            _operands_combinations = product(*_operands)
            for operands_combination in _operands_combinations:
                str_operands = []
                local_used_fields = used_fields.copy()  # 当前路径的字段集合
                local_used_operators = used_operators.copy()  # 当前路径的运算符集合

                for operand in operands_combination:
                    if isinstance(operand, Expression):
                        local_used_fields.update(operand.used_fields)
                        local_used_operators.update(operand.used_operators)
                        str_operands.append(operand.content)
                    elif isinstance(operand, Value):
                        local_used_fields.add(operand.val)
                        str_operands.append(operand.val)

                # 检查占位符数量是否匹配
                if node.operator.count("{}") != len(str_operands):
                    raise ValueError(
                        f"Operator '{node.operator}' has mismatched placeholders for operands."
                    )

                content = node.operator.format(*str_operands)
                local_used_operators.add(node.operator)  # 当前路径记录运算符
                exprs.append(
                    Expression(
                        alias="",
                        content=content,
                        used_fields=local_used_fields,
                        used_operators=local_used_operators,
                        return_type=node.return_type,
                    )
                )
        elif isinstance(node, Value):
            used_fields.add(node.val)
            exprs.append(
                Expression(
                    alias="",
                    content=node.val,
                    used_fields=used_fields.copy(),
                    used_operators=used_operators.copy(),
                    return_type=node.type,
                )
            )
        else:
            raise TypeError("Node must be a Value or ExpressionNode.")

        return exprs

    def end(self) -> List[Expression]:
        """Generate all possible expressions based on combinations."""
        exprs = []

        for operator, operand_combination in self._generate_combinations():
            _dfs_exprs = self._dfs(
                ExpressionNode(operator, list(operand_combination), self.return_type),
                set(),  # 初始化路径上的 used_fields
                set(),  # 初始化路径上的 used_operators
            )
            exprs.extend(_dfs_exprs)  # 展平结果

        return exprs
