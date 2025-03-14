from _base_operator import Value

def abs(a: Value):
    """
    Absolute value of x
    Parameters:
    a (Value): The value for which to compute the absolute value.
    Returns:
    Value: A new Value instance representing the absolute value of the input.
    """
    val = f"abs({a})"
    return Value(val, type=a.type)


def add(a: Value, b: Value, filter=False):
    """
    Add all inputs (at least 2 inputs required). If filter = true, filter all input NaN to 0 before adding.
    Parameters:
    a (Value): The first value to add.
    b (Value): The second value to add.
    filter (bool): If true, filter all input NaN to 0 before adding.
    Returns:
    Value: A new Value instance representing the sum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot add {a.type} with {b.type}")

    val = f"add({a}, {b}, filter={filter})"
    return Value(val, type=a.type)


def densify(a: Value):
    """
    Converts a grouping field of many buckets into lesser number of only available buckets so as to make working with grouping fields computationally efficient.
    Parameters:
    a (Value): The value to densify.
    Returns:
    Value: A new Value instance representing the densified input.
    """
    val = f"densify({a})"
    return Value(val, type=a.type)


def divide(a: Value, b: Value):
    """
    x / y.
    Parameters:
    a (Value): The numerator value.
    b (Value): The denominator value.
    Returns:
    Value: A new Value instance representing the division of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot divide {a.type} with {b.type}")

    val = f"divide({a}, {b})"
    return Value(val, type=a.type)


def inverse(a: Value):
    """
    1 / x.
    Parameters:
    a (Value): The value to invert.
    Returns:
    Value: A new Value instance representing the inverse of the input.
    """
    val = f"inverse({a})"
    return Value(val, type=a.type)


def log(a: Value):
    """
    Natural logarithm.
    Parameters:
    a (Value): The value for which to compute the natural logarithm.
    Returns:
    Value: A new Value instance representing the natural logarithm of the input.
    """
    val = f"log({a})"
    return Value(val, type=a.type)


def max(a: Value, b: Value):
    """
    Maximum value of all inputs. At least 2 inputs are required.
    Parameters:
    a (Value): The first value.
    b (Value): The second value.
    Returns:
    Value: A new Value instance representing the maximum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot find max of {a.type} with {b.type}")

    val = f"max({a}, {b})"
    return Value(val, type=a.type)


def min(a: Value, b: Value):
    """
    Minimum value of all inputs. At least 2 inputs are required.
    Parameters:
    a (Value): The first value.
    b (Value): The second value.
    Returns:
    Value: A new Value instance representing the minimum of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot find min of {a.type} with {b.type}")

    val = f"min({a}, {b})"
    return Value(val, type=a.type)


def multiply(a: Value, b: Value, filter=False):
    """
    Multiply all inputs. At least 2 inputs are required. Filter sets the NaN values to 1.
    Parameters:
    a (Value): The first value to multiply.
    b (Value): The second value to multiply.
    filter (bool): If true, filter all input NaN to 1 before multiplying.
    Returns:
    Value: A new Value instance representing the product of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot multiply {a.type} with {b.type}")

    val = f"multiply({a}, {b}, filter={filter})"
    return Value(val, type=a.type)


def power(a: Value, b: Value):
    """
    x ^ y.
    Parameters:
    a (Value): The base value.
    b (Value): The exponent value.
    Returns:
    Value: A new Value instance representing the power of the input values.
    """
    val = f"power({a}, {b})"
    return Value(val, type=a.type)


def reverse(a: Value):
    """
    - x.
    Parameters:
    a (Value): The value to reverse.
    Returns:
    Value: A new Value instance representing the reversed input.
    """
    val = f"reverse({a})"
    return Value(val, type=a.type)


def sign(a: Value):
    """
    if input = NaN; return NaN.
    Parameters:
    a (Value): The value for which to compute the sign.
    Returns:
    Value: A new Value instance representing the sign of the input.
    """
    val = f"sign({a})"
    return Value(val, type=a.type)


def signed_power(a: Value, b: Value):
    """
    x raised to the power of y such that final result preserves sign of x.
    Parameters:
    a (Value): The base value.
    b (Value): The exponent value.
    Returns:
    Value: A new Value instance representing the signed power of the input values.
    """
    val = f"signed_power({a}, {b})"
    return Value(val, type=a.type)


def subtract(a: Value, b: Value, filter=False):
    """
    x - y. If filter = true, filter all input NaN to 0 before subtracting.
    Parameters:
    a (Value): The first value.
    b (Value): The second value.
    filter (bool): If true, filter all input NaN to 0 before subtracting.
    Returns:
    Value: A new Value instance representing the difference of the input values.
    """
    if a.type != b.type:
        raise TypeError(f"Cannot subtract {a.type} with {b.type}")

    val = f"subtract({a}, {b}, filter={filter})"
    return Value(val, type=a.type)