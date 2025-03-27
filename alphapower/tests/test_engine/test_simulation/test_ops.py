import unittest

from alphapower.engine.simulation.template.core import DataField, Expression
from alphapower.engine.simulation.template.ops import (
    abs,
    add,
    densify,
    divide,
    inverse,
    log,
    max,
    min,
    multiply,
    power,
    reverse,
    sign,
    signed_power,
    subtract,
)


class TestOps(unittest.TestCase):
    def test_abs(self):
        # 测试 abs 函数
        input_value = DataField("test_field", "Test Field")
        result = abs(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "abs")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "abs(test_field)")

    def test_add(self):
        # 测试 add 函数
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        filter_param = True
        result = add(input_a, input_b, filter_param)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "add")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(
            result.parameters, {"filter": "true" if filter_param else "false"}
        )
        self.assertEqual(next(result.compile()), "add(field_a, field_b, filter=true)")
        self.assertEqual(next(result.compile()), "add(field_a, field_b, filter=true)")

    def test_densify(self):
        input_value = DataField("test_field", "Test Field")
        result = densify(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "densify")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "densify(test_field)")

    def test_divide(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        result = divide(input_a, input_b)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "divide")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(next(result.compile()), "divide(field_a, field_b)")

    def test_inverse(self):
        input_value = DataField("test_field", "Test Field")
        result = inverse(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "inverse")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "inverse(test_field)")

    def test_log(self):
        input_value = DataField("test_field", "Test Field")
        result = log(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "log")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "log(test_field)")

    def test_max(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        result = max(input_a, input_b)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "max")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(next(result.compile()), "max(field_a, field_b)")

    def test_min(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        result = min(input_a, input_b)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "min")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(next(result.compile()), "min(field_a, field_b)")

    def test_multiply(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        filter_param = True
        result = multiply(input_a, input_b, filter_param)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "multiply")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(
            result.parameters, {"filter": "true" if filter_param else "false"}
        )
        self.assertEqual(
            next(result.compile()), "multiply(field_a, field_b, filter=true)"
        )

    def test_power(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        result = power(input_a, input_b)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "power")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(next(result.compile()), "power(field_a, field_b)")

    def test_reverse(self):
        input_value = DataField("test_field", "Test Field")
        result = reverse(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "reverse")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "reverse(test_field)")

    def test_sign(self):
        input_value = DataField("test_field", "Test Field")
        result = sign(input_value)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "sign")
        self.assertEqual(result.operands, [input_value])
        self.assertEqual(next(result.compile()), "sign(test_field)")

    def test_signed_power(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        result = signed_power(input_a, input_b)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "signed_power")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(next(result.compile()), "signed_power(field_a, field_b)")

    def test_subtract(self):
        input_a = DataField("field_a", "Field A")
        input_b = DataField("field_b", "Field B")
        filter_param = True
        result = subtract(input_a, input_b, filter_param)
        self.assertIsInstance(result, Expression)
        self.assertEqual(result.operator, "subtract")
        self.assertEqual(result.operands, [input_a, input_b])
        self.assertEqual(
            result.parameters, {"filter": "true" if filter_param else "false"}
        )
        self.assertEqual(
            next(result.compile()), "subtract(field_a, field_b, filter=true)"
        )


if __name__ == "__main__":
    unittest.main()
