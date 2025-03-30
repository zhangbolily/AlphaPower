import unittest
from typing import List

from alphapower.engine.simulation.template.core import (
    DataField,
    DataFieldSet,
    DataFieldType,
    Expression,
)


class TestDataField(unittest.TestCase):
    def test_datafield_initialization(self) -> None:
        field: DataField = DataField("field1", "Test Field", DataFieldType.MATRIX)
        self.assertEqual(field.field_id, "field1")
        self.assertEqual(field.description, "Test Field")
        self.assertEqual(field.field_type, DataFieldType.MATRIX)

    def test_datafield_operations(self) -> None:
        field1: DataField = DataField("field1", "Field 1")
        field2: DataField = DataField("field2", "Field 2")
        expr_add: Expression = field1 + field2
        expr_sub: Expression = field1 - field2
        expr_mul: Expression = field1 * field2
        expr_div: Expression = field1 / field2

        self.assertIsInstance(expr_add, Expression)
        self.assertEqual(expr_add.operator, "add")
        self.assertEqual(expr_add.operands[0], field1)
        self.assertEqual(expr_add.operands[1], field2)

        self.assertIsInstance(expr_sub, Expression)
        self.assertEqual(expr_sub.operator, "subtract")
        self.assertEqual(expr_sub.operands[0], field1)
        self.assertEqual(expr_sub.operands[1], field2)

        self.assertIsInstance(expr_mul, Expression)
        self.assertEqual(expr_mul.operator, "multiply")
        self.assertEqual(expr_mul.operands[0], field1)
        self.assertEqual(expr_mul.operands[1], field2)

        self.assertIsInstance(expr_div, Expression)
        self.assertEqual(expr_div.operator, "divide")
        self.assertEqual(expr_div.operands[0], field1)
        self.assertEqual(expr_div.operands[1], field2)

        # 测试嵌套表达式
        nested_expr: Expression = (field1 + field2) * (field1 - field2)
        self.assertIsInstance(nested_expr, Expression)
        self.assertEqual(nested_expr.operator, "multiply")

        left_operand_raw = nested_expr.operands[0]
        self.assertTrue(isinstance(left_operand_raw, Expression))
        if isinstance(left_operand_raw, Expression):
            left_operand: Expression = left_operand_raw  # 类型检查后转换
            self.assertEqual(left_operand.operator, "add")

        right_operand_raw = nested_expr.operands[1]
        self.assertTrue(isinstance(right_operand_raw, Expression))
        if isinstance(right_operand_raw, Expression):
            right_operand: Expression = right_operand_raw  # 类型检查后转换
            self.assertEqual(right_operand.operator, "subtract")

    def test_datafield_compile(self) -> None:
        field: DataField = DataField("field1", "Field 1")
        compiled: List[str] = list(field.compile())
        self.assertEqual(compiled, ["field1"])


class TestExpression(unittest.TestCase):
    def test_expression_initialization(self) -> None:
        field1: DataField = DataField("field1", "Field 1")
        field2: DataField = DataField("field2", "Field 2")
        expr: Expression = Expression("add", [field1, field2])
        self.assertEqual(expr.operator, "add")
        self.assertEqual(len(expr.operands), 2)
        self.assertEqual(expr.operands[0], field1)
        self.assertEqual(expr.operands[1], field2)

    def test_expression_to_alpha(self) -> None:
        field1: DataField = DataField("field1", "Field 1")
        field2: DataField = DataField("field2", "Field 2")
        expr: Expression = Expression("add", [field1, field2])
        compiled = expr.compile()
        self.assertEqual(list(compiled), ["add(field1, field2)"])

    def test_expression_with_parameters(self) -> None:
        field1: DataField = DataField("field1", "Field 1")
        field2: DataField = DataField("field2", "Field 2")
        expr: Expression = Expression("add", [field1, field2], {"scale": 2})
        compiled: List[str] = list(expr.compile())
        self.assertEqual(compiled, ["add(field1, field2, scale=2)"])

    def test_expression_with_datafield_set(self) -> None:
        field1: DataField = DataField("field1", "Field 1", DataFieldType.MATRIX)
        field2: DataField = DataField("field2", "Field 2", DataFieldType.MATRIX)
        field_set: DataFieldSet = DataFieldSet([field1, field2], DataFieldType.MATRIX)
        expr: Expression = Expression("add", [field_set, field_set], {"filter": True})
        compiled: List[str] = list(expr.compile())
        self.assertIn("add(field1, field1, filter=True)", compiled)
        self.assertIn("add(field2, field2, filter=True)", compiled)
        self.assertIn("add(field1, field2, filter=True)", compiled)
        self.assertIn("add(field2, field1, filter=True)", compiled)

    def test_expression_with_sub_expression(self) -> None:
        field1: DataField = DataField("field1", "Field 1")
        field2: DataField = DataField("field2", "Field 2")
        expr1: Expression = Expression("add", [field1, field2], {"filter": True})
        expr2: Expression = Expression("subtract", [field1, field2])
        expr: Expression = Expression("multiply", [expr1, expr2])
        compiled: List[str] = list(expr.compile())

        self.assertIn(
            "multiply(add(field1, field2, filter=True), subtract(field1, field2))",
            compiled,
        )


if __name__ == "__main__":
    unittest.main()
