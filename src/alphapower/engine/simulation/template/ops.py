from typing import Any, Dict, Union

from .core import DataField, Expression


def abs(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Computes the absolute value of the input.
    Parameters:
    a: The value for which to compute the absolute value.
    Returns:
    None
    """
    operator = "abs"
    return Expression(operator, [a])


def add(
    a: Union[DataField, Expression, Any],
    b: Union[DataField, Expression, Any],
    filter: bool,
) -> Expression:
    """
    Adds two inputs. If filter is true, NaN values are treated as 0 before addition.
    Parameters:
    a: The first value to add.
    b: The second value to add.
    filter: If true, NaN values are treated as 0 before addition.
    Returns:
    None
    """
    operator = "add"
    return Expression(
        operator, [a, b], parameters={"filter": "true" if filter else "false"}
    )


def densify(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Reduces the number of buckets in a grouping field to only those that are available.
    Parameters:
    a: The value to densify.
    Returns:
    Expression
    """
    operator = "densify"
    return Expression(operator, [a])


def divide(
    a: Union[DataField, Expression, Any], b: Union[DataField, Expression, Any]
) -> Expression:
    """
    Divides the first input by the second input.
    Parameters:
    a: The numerator.
    b: The denominator.
    Returns:
    Expression
    """
    operator = "divide"
    return Expression(operator, [a, b])


def inverse(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Computes the inverse (1/x) of the input.
    Parameters:
    a: The value to invert.
    Returns:
    Expression
    """
    operator = "inverse"
    return Expression(operator, [a])


def log(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Computes the natural logarithm of the input.
    Parameters:
    a: The value for which to compute the natural logarithm.
    Returns:
    Expression
    """
    operator = "log"
    return Expression(operator, [a])


def max(
    a: Union[DataField, Expression, Any], b: Union[DataField, Expression, Any]
) -> Expression:
    """
    Computes the maximum of two inputs.
    Parameters:
    a: The first value.
    b: The second value.
    Returns:
    Expression
    """
    operator = "max"
    return Expression(operator, [a, b])


def min(
    a: Union[DataField, Expression, Any], b: Union[DataField, Expression, Any]
) -> Expression:
    """
    Computes the minimum of two inputs.
    Parameters:
    a: The first value.
    b: The second value.
    Returns:
    Expression
    """
    operator = "min"
    return Expression(operator, [a, b])


def multiply(
    a: Union[DataField, Expression, Any],
    b: Union[DataField, Expression, Any],
    filter: bool,
) -> Expression:
    """
    Multiplies two inputs. If filter is true, NaN values are treated as 1 before multiplication.
    Parameters:
    a: The first value to multiply.
    b: The second value to multiply.
    filter: If true, NaN values are treated as 1 before multiplication.
    Returns:
    Expression
    """
    operator = "multiply"
    return Expression(
        operator, [a, b], parameters={"filter": "true" if filter else "false"}
    )


def power(
    a: Union[DataField, Expression, Any], b: Union[DataField, Expression, Any]
) -> Expression:
    """
    Raises the first input to the power of the second input.
    Parameters:
    a: The base value.
    b: The exponent value.
    Returns:
    Expression
    """
    operator = "power"
    return Expression(operator, [a, b])


def reverse(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Negates the input.
    Parameters:
    a: The value to negate.
    Returns:
    Expression
    """
    operator = "reverse"
    return Expression(operator, [a])


def sign(a: Union[DataField, Expression, Any]) -> Expression:
    """
    Computes the sign of the input. If the input is NaN, returns NaN.
    Parameters:
    a: The value for which to compute the sign.
    Returns:
    Expression
    """
    operator = "sign"
    return Expression(operator, [a])


def signed_power(
    a: Union[DataField, Expression, Any], b: Union[DataField, Expression, Any]
) -> Expression:
    """
    Raises the first input to the power of the second input, preserving the sign of the first input.
    Parameters:
    a: The base value.
    b: The exponent value.
    Returns:
    Expression
    """
    operator = "signed_power"
    return Expression(operator, [a, b])


def subtract(
    a: Union[DataField, Expression, Any],
    b: Union[DataField, Expression, Any],
    filter: bool,
) -> Expression:
    """
    Subtracts the second input from the first input. If filter is true, NaN values are treated as 0 before subtraction.
    Parameters:
    a: The first value.
    b: The second value.
    filter: If true, NaN values are treated as 0 before subtraction.
    Returns:
    Expression
    """
    operator = "subtract"
    return Expression(
        operator, [a, b], parameters={"filter": "true" if filter else "false"}
    )
