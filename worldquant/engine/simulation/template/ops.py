from typing import Union

from .base import Value
from .expression import ExpressionNode


def abs(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    Absolute value of x
    Parameters:
    a (Union[Value, ExpressionNode]): The value for which to compute the absolute value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the absolute value of the input.
    """
    operator: str = "abs({})"
    return ExpressionNode(operator=operator, operands=[a], return_type=a.type)


def add(
    a: Union[Value, ExpressionNode],
    b: Union[Value, ExpressionNode],
    filter: Value = Value(val="False"),
) -> ExpressionNode:
    """
    Add all inputs (at least 2 inputs required). If filter = true, filter all input NaN to 0 before adding.
    Parameters:
    a (Union[Value, ExpressionNode]): The first value to add.
    b (Union[Value, ExpressionNode]): The second value to add.
    filter (bool): If true, filter all input NaN to 0 before adding.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the sum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot add {a.type} with {b.type}")

    operator: str = "add({}, {}, filter={})"
    return ExpressionNode(
        operator=operator, operands=[a, b, filter], return_type=a.type
    )


def densify(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    Converts a grouping field of many buckets into lesser number of only available buckets so as to make working with grouping fields computationally efficient.
    Parameters:
    a (Union[Value, ExpressionNode]): The value to densify.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the densified input.
    """
    val: str = f"densify({a})"
    return ExpressionNode(val, type=a.type)


def divide(
    a: Union[Value, ExpressionNode], b: Union[Value, ExpressionNode]
) -> ExpressionNode:
    """
    x / y.
    Parameters:
    a (Union[Value, ExpressionNode]): The numerator value.
    b (Union[Value, ExpressionNode]): The denominator value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the division of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot divide {a.type} with {b.type}")

    val: str = f"divide({a}, {b})"
    return ExpressionNode(val, type=a.type)


def inverse(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    1 / x.
    Parameters:
    a (Union[Value, ExpressionNode]): The value to invert.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the inverse of the input.
    """
    val: str = f"inverse({a})"
    return ExpressionNode(val, type=a.type)


def log(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    Natural logarithm.
    Parameters:
    a (Union[Value, ExpressionNode]): The value for which to compute the natural logarithm.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the natural logarithm of the input.
    """
    val: str = f"log({a})"
    return ExpressionNode(val, type=a.type)


def max(
    a: Union[Value, ExpressionNode], b: Union[Value, ExpressionNode]
) -> ExpressionNode:
    """
    Maximum value of all inputs. At least 2 inputs are required.
    Parameters:
    a (Union[Value, ExpressionNode]): The first value.
    b (Union[Value, ExpressionNode]): The second value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the maximum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot find max of {a.type} with {b.type}")

    val: str = f"max({a}, {b})"
    return ExpressionNode(val, type=a.type)


def min(
    a: Union[Value, ExpressionNode], b: Union[Value, ExpressionNode]
) -> ExpressionNode:
    """
    Minimum value of all inputs. At least 2 inputs are required.
    Parameters:
    a (Union[Value, ExpressionNode]): The first value.
    b (Union[Value, ExpressionNode]): The second value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the minimum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot find min of {a.type} with {b.type}")

    val: str = f"min({a}, {b})"
    return ExpressionNode(val, type=a.type)


def multiply(
    a: Union[Value, ExpressionNode],
    b: Union[Value, ExpressionNode],
    filter: bool = False,
) -> ExpressionNode:
    """
    Multiply all inputs. At least 2 inputs are required. Filter sets the NaN values to 1.
    Parameters:
    a (Union[Value, ExpressionNode]): The first value to multiply.
    b (Union[Value, ExpressionNode]): The second value to multiply.
    filter (bool): If true, filter all input NaN to 1 before multiplying.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the product of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot multiply {a.type} with {b.type}")

    val: str = f"multiply({a}, {b}, filter={filter})"
    return ExpressionNode(val, type=a.type)


def power(
    a: Union[Value, ExpressionNode], b: Union[Value, ExpressionNode]
) -> ExpressionNode:
    """
    x ^ y.
    Parameters:
    a (Union[Value, ExpressionNode]): The base value.
    b (Union[Value, ExpressionNode]): The exponent value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the power of the input values.
    """
    val: str = f"power({a}, {b})"
    return ExpressionNode(val, type=a.type)


def reverse(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    - x.
    Parameters:
    a (Union[Value, ExpressionNode]): The value to reverse.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the reversed input.
    """
    val: str = f"reverse({a})"
    return ExpressionNode(val, type=a.type)


def sign(a: Union[Value, ExpressionNode]) -> ExpressionNode:
    """
    if input = NaN; return NaN.
    Parameters:
    a (Union[Value, ExpressionNode]): The value for which to compute the sign.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the sign of the input.
    """
    val: str = f"sign({a})"
    return ExpressionNode(val, type=a.type)


def signed_power(
    a: Union[Value, ExpressionNode], b: Union[Value, ExpressionNode]
) -> ExpressionNode:
    """
    x raised to the power of y such that final result preserves sign of x.
    Parameters:
    a (Union[Value, ExpressionNode]): The base value.
    b (Union[Value, ExpressionNode]): The exponent value.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the signed power of the input values.
    """
    val: str = f"signed_power({a}, {b})"
    return ExpressionNode(val, type=a.type)


def subtract(
    a: Union[Value, ExpressionNode],
    b: Union[Value, ExpressionNode],
    filter: bool = False,
) -> ExpressionNode:
    """
    x - y. If filter = true, filter all input NaN to 0 before subtracting.
    Parameters:
    a (Union[Value, ExpressionNode]): The first value.
    b (Union[Value, ExpressionNode]): The second value.
    filter (bool): If true, filter all input NaN to 0 before subtracting.
    Returns:
    ExpressionNode: A new ExpressionNode instance representing the difference of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot subtract {a.type} with {b.type}")

    val: str = f"subtract({a}, {b}, filter={filter})"
    return ExpressionNode(val, type=a.type)
