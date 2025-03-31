"""
Constants for the AlphaPower package.
"""

from typing import Callable, Dict, Final, Tuple

DB_ALPHAS: Final[str] = "alphas"
DB_DATA: Final[str] = "data"
DB_SIMULATION: Final[str] = "simulation"

"""
API 路由
"""
BASE_URL = "https://api.worldquantbrain.com"
DEFAULT_SIMULATION_RESPONSE: Tuple[bool, str, float] = (False, "", 0.0)
ENDPOINT_AUTHENTICATION = "authentication"
ENDPOINT_ALPHAS = "alphas"
ENDPOINT_SELF_ALPHA_LIST = "users/self/alphas"
ENDPOINT_ALPHA_YEARLY_STATS: Callable[[str], str] = (
    lambda alpha_id: f"alphas/{alpha_id}/recordsets/yearly-stats"
)
ENDPOINT_ALPHA_PNL: Callable[[str], str] = (
    lambda alpha_id: f"alphas/{alpha_id}/recordsets/pnl"
)
ENDPOINT_ALPHA_SELF_CORRELATIONS: Callable[[str, str], str] = (
    lambda alpha_id, correlation_type: f"alphas/{alpha_id}/correlations/{correlation_type}"
)
ENDPOINT_SIMULATION = "simulations"
ENDPOINT_ACTIVITIES_SIMULATION = "users/self/activities/simulations"
ENDPOINT_DATA_CATEGORIES = "data-categories"
ENDPOINT_DATA_SETS = "data-sets"
ENDPOINT_DATA_FIELDS = "data-fields"
ENDPOINT_OPERATORS = "operators"


# HTTP 错误代码映射关系
HttpCodeMessage: Final[Dict] = {
    100: "Continue",
    101: "Switching Protocols",
    102: "Processing",
    200: "OK",
    201: "Created",
    202: "Accepted",
    203: "Non-Authoritative Information",
    204: "No Content",
    205: "Reset Content",
    206: "Partial Content",
    207: "Multi-Status",
    208: "Already Reported",
    226: "IM Used",
    300: "Multiple Choices",
    301: "Moved Permanently",
    302: "Found",
    303: "See Other",
    304: "Not Modified",
    305: "Use Proxy",
    307: "Temporary Redirect",
    308: "Permanent Redirect",
    400: "Bad Request",
    401: "Unauthorized",
    402: "Payment Required",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    406: "Not Acceptable",
    407: "Proxy Authentication Required",
    408: "Request Timeout",
    409: "Conflict",
    410: "Gone",
    411: "Length Required",
    412: "Precondition Failed",
    413: "Payload Too Large",
    414: "URI Too Long",
    415: "Unsupported Media Type",
    416: "Range Not Satisfiable",
    417: "Expectation Failed",
    418: "I'm a teapot",
    421: "Misdirected Request",
    422: "Unprocessable Entity",
    423: "Locked",
    424: "Failed Dependency",
    425: "Unordered Collection",
    426: "Upgrade Required",
    428: "Precondition Required",
    429: "Too Many Requests",
    431: "Request Header Fields Too Large",
    451: "Unavailable For Legal Reasons",
    500: "Internal Server Error",
    501: "Not Implemented",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
    505: "HTTP Version Not Supported",
    506: "Variant Also Negotiates",
    507: "Insufficient Storage",
    508: "Loop Detected",
    509: "Bandwidth Limit Exceeded",
    510: "Not Extended",
    511: "Network Authentication Required",
}

# 常见枚举值
TOP3000: Final[str] = "TOP3000"
TOP2000: Final[str] = "TOP2000"
TOP1000: Final[str] = "TOP1000"
TOP500: Final[str] = "TOP500"
TOP200: Final[str] = "TOP200"

UNIVERSE: Final[list] = [TOP3000, TOP2000, TOP1000, TOP500, TOP200]

PYTHON: Final[str] = "PYTHON"
EXPRESSION: Final[str] = "EXPRESSION"
FASTEXPR: Final[str] = "FASTEXPR"

ALPHAS_LANGUAGE: Final[list] = [PYTHON, EXPRESSION, FASTEXPR]

NONE: Final[str] = "NONE"
MARKET: Final[str] = "MARKET"
INDUSTRY: Final[str] = "INDUSTRY"
SUBINDUSTRY: Final[str] = "SUBINDUSTRY"
SECTOR: Final[str] = "SECTOR"

NEUTRALIZATION: Final[list] = [NONE, MARKET, INDUSTRY, SUBINDUSTRY, SECTOR]
