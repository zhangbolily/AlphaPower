import re
import unittest
from unittest.mock import MagicMock

from worldquant.engine.simulation.template import Value, ValueType
from worldquant.engine.simulation.template.expression import ExpressionNode


class TestExpressionNode(unittest.TestCase):
    def setUp(self):
        self.value1 = Value("field1", ValueType.VECTOR)
        self.value2 = Value("field2", ValueType.VECTOR)
        self.value3 = Value("field3", ValueType.VECTOR)
        self.operator = {
            "add({}, {})",
            "subtract({}, {})",
            "multiply({}, {})",
            "divide({}, {})",
        }
        self.return_type = ValueType.VECTOR
        self.node = ExpressionNode(
            operator=self.operator,
            operands=[
                [self.value1, self.value2, self.value3],
                [self.value1, self.value2, self.value3],
            ],
            return_type=self.return_type,
        )

    def _validate_expression(self, expression, operator, operands, return_type):
        """Helper method to validate an expression."""
        expected_fields = {operand.val for operand in operands}
        self.assertEqual(expression.used_fields, expected_fields)
        self.assertEqual(expression.used_operators, {operator})
        self.assertEqual(expression.return_type, return_type)

    def _test_expression_node(self, node, expected_operators, expected_operands):
        """Helper method to test an ExpressionNode."""
        expressions = node.end()
        # 直接计算表达式数量
        expected_expression_count = (
            len(expected_operators) * len(node.operands[0]) * len(node.operands[1])
        )
        self.assertEqual(len(expressions), expected_expression_count)  # 验证表达式数量
        for expression in expressions:
            # 从表达式内容中提取操作符
            operator = next(
                (
                    op
                    for op in expected_operators
                    if op.split("(")[0] in expression.content
                ),
                None,
            )
            self.assertIsNotNone(
                operator,
                f"No matching operator found in expression: {expression.content}",
            )
            # 使用正则表达式精确匹配操作数
            operand_vals = [operand.val for operand in expected_operands]
            operand_pattern = (
                r"\b(" + "|".join(re.escape(val) for val in operand_vals) + r")\b"
            )
            matched_operands = re.findall(operand_pattern, expression.content)
            operands = [
                operand
                for operand in expected_operands
                for matched_operand in matched_operands
                if operand.val in matched_operand
            ]
            self.assertEqual(
                len(operands),
                2,
                f"Expected 2 operands, but found {len(operands)} in expression: {expression.content}",
            )
            self._validate_expression(expression, operator, operands, node.return_type)

    def test_generate_combinations(self):
        combinations = list(self.node._generate_operator_operand_combinations())
        expected_combinations_count = (
            len(self.operator) * len(self.node.operands[0]) * len(self.node.operands[1])
        )
        self.assertEqual(len(combinations), expected_combinations_count)  # 验证组合数量
        for op, operands in combinations:
            self.assertIn(op, self.operator)  # 验证操作符在集合中
            self.assertEqual(len(operands), 2)  # 验证操作数数量

    def test_end(self):
        # 测试多操作符
        self._test_expression_node(
            self.node, self.operator, [self.value1, self.value2, self.value3]
        )

        # 测试单一操作符
        single_operator = "add({}, {})"
        node = ExpressionNode(
            operator=single_operator,
            operands=[[self.value1], [self.value2]],
            return_type=self.return_type,
        )
        self._test_expression_node(node, {single_operator}, [self.value1, self.value2])

        # 测试多个操作符
        multiple_operators = {"add({}, {})", "subtract({}, {})"}
        node = ExpressionNode(
            operator=multiple_operators,
            operands=[[self.value1], [self.value2]],
            return_type=self.return_type,
        )
        self._test_expression_node(node, multiple_operators, [self.value1, self.value2])

        # 测试嵌套 ExpressionNode
        nested_node = ExpressionNode(
            operator="multiply({}, {})",
            operands=[[node], [self.value1]],
            return_type=self.return_type,
        )
        nested_expressions = nested_node.end()
        self.assertEqual(
            len(nested_expressions), len(multiple_operators)
        )  # 验证嵌套表达式数量
        for nested_expression in nested_expressions:
            self.assertIn("multiply", nested_expression.content)
            self.assertIn(self.value1.val, nested_expression.content)
            self.assertEqual(
                nested_expression.used_fields, {self.value1.val, self.value2.val}
            )
            self.assertIn("multiply({}, {})", nested_expression.used_operators)
            self.assertEqual(nested_expression.return_type, self.return_type)


if __name__ == "__main__":
    unittest.main()
