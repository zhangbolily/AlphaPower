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
        # 检查 operator 的合法性
        if isinstance(operator, list):
            if any(op.count("{}") != len(operands) for op in operator):
                raise ValueError("列表中的操作符占位符数量与操作数不匹配。")
        elif isinstance(operator, (str, set)):
            pass
        else:
            raise TypeError("Operator 必须是字符串、集合或列表。")

        self.operator: Union[str, Set[str], List[str]] = operator
        self.operands: List[Iterable[Union[Value, "ExpressionNode"]]] = operands
        self.return_type: ValueType = return_type

    def _generate_operator_operand_combinations(
        self,
    ) -> Iterable[tuple[str, tuple[Union[Value, "ExpressionNode"], ...]]]:
        """生成所有操作符和操作数的组合。"""
        if isinstance(self.operator, str):
            operator_set: Set[str] = {self.operator}
        elif isinstance(self.operator, (set, list)):
            operator_set: Set[str] = set(self.operator)
        else:
            raise TypeError("Operator 必须是字符串、集合或列表。")

        return product(operator_set, product(*self.operands))

    def _process_operand(
        self,
        operand: Union[Value, "Expression"],
        used_fields: Set[str],
        used_operators: Set[str],
    ) -> str:
        """处理单个操作数，返回其字符串表示并更新字段和操作符集合。"""
        if isinstance(operand, Expression):
            used_fields.update(operand.used_fields)
            used_operators.update(operand.used_operators)
            return operand.content
        elif isinstance(operand, Value):
            used_fields.add(operand.val)
            return operand.val
        else:
            raise TypeError(f"操作数必须是 Value 或 Expression 而非 {type(operand)}。")

    def _perform_depth_first_search(
        self,
        node: Union[Value, "ExpressionNode"],
        used_fields: Set[str],
        used_operators: Set[str],
    ) -> List[Expression]:
        """使用深度优先搜索生成表达式内容。"""
        if isinstance(node, Value):
            used_fields.add(node.val)
            return [
                Expression(
                    alias="",
                    content=node.val,
                    used_fields=used_fields.copy(),
                    used_operators=used_operators.copy(),
                    return_type=node.type,
                )
            ]

        expressions: List[Expression] = []
        operand_groups: List[List[Union[Value, Expression]]] = []

        for operand in node.operands:
            if isinstance(operand, Value):
                operand_groups.append([operand])
            elif isinstance(operand, ExpressionNode):
                operand_groups.append(
                    self._perform_depth_first_search(operand, set(), set())
                )
            elif isinstance(operand, Iterable):
                # 如果 operand 是可迭代对象，递归处理其内部元素
                nested_group: List[Union[Value, Expression]] = []
                for sub_operand in operand:
                    if isinstance(sub_operand, Value):
                        nested_group.append(sub_operand)
                    elif isinstance(sub_operand, ExpressionNode):
                        nested_group.extend(
                            self._perform_depth_first_search(sub_operand, set(), set())
                        )
                    else:
                        raise TypeError(
                            f"操作数必须是 Value 或 ExpressionNode 而非 {type(sub_operand)}。"
                        )
                operand_groups.append(nested_group)
            else:
                raise TypeError(
                    f"操作数必须是 Value、ExpressionNode 或可迭代对象，而非 {type(operand)}。"
                )

        for operand_combination in product(*operand_groups):
            local_used_fields: Set[str] = used_fields.copy()
            local_used_operators: Set[str] = used_operators.copy()
            operand_strings: List[str] = [
                self._process_operand(op, local_used_fields, local_used_operators)
                for op in operand_combination
            ]

            # 根据 node.operator 的类型处理占位符匹配逻辑
            if isinstance(node.operator, str):
                if node.operator.count("{}") != len(operand_strings):
                    raise ValueError(
                        f"Operator '{node.operator}' 的占位符数量与操作数不匹配。"
                    )
                content: str = node.operator.format(*operand_strings)
                local_used_operators.add(node.operator)
            elif isinstance(node.operator, (set, list)):
                for operator in node.operator:
                    if operator.count("{}") != len(operand_strings):
                        raise ValueError(
                            f"Operator '{operator}' 的占位符数量与操作数不匹配。"
                        )
                    content: str = operator.format(*operand_strings)
                    local_used_operators.add(operator)
                    expressions.append(
                        Expression(
                            alias="",
                            content=content,
                            used_fields=local_used_fields.copy(),
                            used_operators=local_used_operators.copy(),
                            return_type=node.return_type,
                        )
                    )
                continue
            else:
                raise TypeError("Operator 必须是字符串、集合或列表。")

            expressions.append(
                Expression(
                    alias="",
                    content=content,
                    used_fields=local_used_fields,
                    used_operators=local_used_operators,
                    return_type=node.return_type,
                )
            )

        return expressions

    def end(self) -> List[Expression]:
        """基于操作符和操作数的组合生成所有可能的表达式。"""
        all_expressions: List[Expression] = []

        for (
            operator,
            operand_combination,
        ) in self._generate_operator_operand_combinations():
            dfs_expressions: List[Expression] = self._perform_depth_first_search(
                ExpressionNode(operator, list(operand_combination), self.return_type),
                set(),
                set(),
            )
            all_expressions.extend(dfs_expressions)

        return all_expressions
