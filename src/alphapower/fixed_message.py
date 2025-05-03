"""
此模块不要生成 docstring 文档
"""

import re
from typing import Final

MESSAGE_RUNNING_TOO_LONG: Final[str] = (
    "Your simulation has been running too long. "
    "If you are running simulations in batch, "
    "consider to reduce number of concurrent simulations or input data."
)
# 示例: Attempted to use unknown variable "mdl165_forddl8nuggetv2_51v"
# 用于匹配未知变量使用的正则表达式，捕获变量名
# 专业术语：正则表达式（Regular Expression, regex）
MESSAGE_USE_UNKNOWN_VARIABLE_REGEX: re.Pattern[str] = re.compile(
    r'Attempted to use unknown variable "(?P<name>[\w\d_]+)"'
)
MESSAGE_TOO_MUCH_RESOURCE: Final[str] = (
    "Your simulation probably took too much resource"
)
# 示例: Incompatible unit for input of "ts_product" at index 0, expected "Unit[]", found "Unit[CSPrice:-1]"
MESSAGE_INCOMPATIBLE_UNIT_REGEX: Final[str] = (
    'Incompatible unit for input of "{name}" at index {index}, expected "{expected}", found "{found}"'
)
