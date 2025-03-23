import unittest
from unittest.mock import MagicMock

from worldquant.engine.simulation.template import Value, ValueType

from worldquant.engine.simulation.template.expression import ExpressionNode


class TestExpressionNode(unittest.TestCase):
    def setUp(self):
        self.value1 = MagicMock(spec=Value, val="field1", type=ValueType.VECTOR)
        self.value2 = MagicMock(spec=Value, val="field2", type=ValueType.VECTOR)
        self.operator = set(
            ["add({}, {})", "subtract({}, {})", "multiply({}, {})", "divide({}, {})"]
        )
        self.expr_node1 = MagicMock(
            spec=ExpressionNode,
            return_type=ValueType.VECTOR,
            operator=self.operator,
            operands=[[self.value1], [self.value2]],
        )
        self.expr_node2 = MagicMock(
            spec=ExpressionNode,
            return_type=ValueType.VECTOR,
            operator=self.operator,
            operands=[[self.value1], [self.value2]],
        )

        self.return_type = ValueType.VECTOR
        self.node = ExpressionNode(
            operator=self.operator,
            operands=[[self.value1], [self.value2]],
            return_type=self.return_type,
        )

    def test_generate_combinations(self):
        combinations = list(self.node._generate_combinations())
        self.assertEqual(len(combinations), len(self.operator))  # 验证组合数量
        for op, operands in combinations:
            self.assertIn(op, self.operator)  # 验证操作符在集合中
            self.assertEqual(operands, (self.value1, self.value2))  # 验证操作数

    def test_end(self):
        expressions = self.node.end()
        self.assertEqual(len(expressions), len(self.operator))  # 验证表达式数量
        for expression, op in zip(expressions, self.operator):
            self.assertIn(op, self.operator)
            self.assertEqual(
                expression.content, op.format(self.value1.val, self.value2.val)
            )
            self.assertEqual(expression.used_fields, {self.value1.val, self.value2.val})
            self.assertEqual(expression.used_operators, {op})
            self.assertEqual(expression.return_type, self.return_type)

        # 测试单一操作符
        single_operator = "add({}, {})"
        node = ExpressionNode(
            operator=single_operator,
            operands=[[self.value1], [self.value2]],
            return_type=self.return_type,
        )
        expressions = node.end()
        self.assertEqual(len(expressions), 1)  # 只有一个表达式
        expression = expressions[0]
        self.assertEqual(
            expression.content, single_operator.format(self.value1.val, self.value2.val)
        )
        self.assertEqual(expression.used_fields, {self.value1.val, self.value2.val})
        self.assertEqual(expression.used_operators, {single_operator})
        self.assertEqual(expression.return_type, self.return_type)

        # 测试多个操作符
        multiple_operators = {"add({}, {})", "subtract({}, {})"}
        node = ExpressionNode(
            operator=multiple_operators,
            operands=[[self.value1], [self.value2]],
            return_type=self.return_type,
        )
        expressions = node.end()
        self.assertEqual(
            len(expressions), len(multiple_operators)
        )  # 表达式数量等于操作符数量
        for expression, op in zip(expressions, multiple_operators):
            self.assertIn(op, multiple_operators)
            self.assertEqual(
                expression.content, op.format(self.value1.val, self.value2.val)
            )
            self.assertEqual(expression.used_fields, {self.value1.val, self.value2.val})
            self.assertEqual(expression.used_operators, {op})
            self.assertEqual(expression.return_type, self.return_type)

        # 测试嵌套 ExpressionNode
        nested_node = ExpressionNode(
            operator="multiply({}, {})",
            operands=[[node], [self.value1]],
            return_type=self.return_type,
        )
        nested_expressions = nested_node.end()
        self.assertEqual(
            len(nested_expressions), len(multiple_operators)
        )  # 嵌套表达式数量等于内层操作符数量
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
