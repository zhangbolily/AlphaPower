import unittest
from unittest.mock import MagicMock

from worldquant.engine.simulation.template.expression import ExpressionNode, Value
from worldquant.engine.simulation.template.ops import ValueType


class TestExpressionNode(unittest.TestCase):
    def setUp(self):
        self.value1 = MagicMock(spec=Value, val="field1", type=ValueType.VECTOR)
        self.value2 = MagicMock(spec=Value, val="field2", type=ValueType.VECTOR)
        self.operator = set(["add", "subtract", "multiply", "divide"])
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
            self.assertEqual(
                expression.content, f"{op}({self.value1.val},{self.value2.val})"
            )  # 验证表达式内容
            self.assertIn("field1", expression.used_fields)
            self.assertIn("field2", expression.used_fields)
            self.assertIn(op, expression.used_operators)  # 验证操作符
            self.assertEqual(expression.return_type, self.return_type)


if __name__ == "__main__":
    unittest.main()
