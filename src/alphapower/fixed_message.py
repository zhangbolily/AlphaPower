"""
此模块不要生成 docstring 文档
"""

from typing import Final

MESSAGE_RUNNING_TOO_LONG: Final[str] = (
    "Your simulation has been running too long. "
    "If you are running simulations in batch, "
    "consider to reduce number of concurrent simulations or input data."
)
# 示例: Attempted to use unknown variable "mdl165_forddl8nuggetv2_51v"
MESSAGE_USE_UNKNOWN_VARIABLE_REGEX: Final[str] = (
    'Attempted to use unknown variable "{name}"'
)
MESSAGE_TOO_MUCH_RESOURCE: Final[str] = (
    "Your simulation probably took too much resource"
)
# 示例: Incompatible unit for input of "ts_product" at index 0, expected "Unit[]", found "Unit[CSPrice:-1]"
MESSAGE_INCOMPATIBLE_UNIT_REGEX: Final[str] = (
    'Incompatible unit for input of "{name}" at index {index}, expected "{expected}", found "{found}"'
)
