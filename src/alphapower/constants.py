from __future__ import annotations  # 添加此行解决前向引用问题

from enum import Enum
from functools import lru_cache  # 添加缓存支持
from typing import Callable, Dict, Final, List, Optional, Tuple
from urllib.parse import urljoin

# 类型别名定义，提高代码可读性
# 注意：使用字符串作为类型注解，解决类型前向引用问题
UniverseMap = Dict["Region", List["Universe"]]
NeutralizationMap = Dict["Region", List["Neutralization"]]

# -----------------------------------------------------------------------------
# 基础环境和系统配置
# -----------------------------------------------------------------------------


class Environment(Enum):

    PROD = "prod"  # 生产环境
    DEV = "dev"  # 开发环境
    TEST = "test"  # 测试环境


# 环境设置常量
# 注意：建议使用Environment枚举而非这些字符串常量
ENV_PROD: Final[str] = "prod"  # 生产环境标识符
ENV_DEV: Final[str] = "dev"  # 开发环境标识符
ENV_TEST: Final[str] = "test"  # 测试环境标识符

ALPHA_ID_LENGTH: Final[int] = 7  # Alpha ID的长度
MAX_COUNT_IN_SINGLE_ALPHA_LIST_QUERY: Final[int] = 10000  # 单个Alpha列表查询的最大数量
CORRELATION_CALCULATION_YEARS: Final[int] = 4  # 相关性计算的年份范围
MIN_FORMULATED_PYRAMID_ALPHAS: Final[int] = 3  # 点亮金字塔Alpha的最小数量


class Database(Enum):

    ALPHAS = "alphas"  # Alpha因子数据库
    DATA = "data"  # 市场数据库
    SIMULATION = "simulation"  # 模拟回测数据库
    EVALUATE = "evaluate"  # 检查数据库


# 数据库名称常量 (兼容性保留，建议使用Database枚举)
DB_ALPHAS: Final[str] = "alphas"  # Alpha因子数据库名称
DB_DATA: Final[str] = "data"  # 市场数据库名称
DB_SIMULATION: Final[str] = "simulation"  # 模拟回测数据库名称


# -----------------------------------------------------------------------------
# API 路由相关常量
# -----------------------------------------------------------------------------
# API基础URL
BASE_URL: Final[str] = "https://api.worldquantbrain.com"  # API服务器基础URL

# 默认模拟响应格式：(成功标志, 消息, 分数)
DEFAULT_SIMULATION_RESPONSE: Final[Tuple[bool, str, float]] = (False, "", 0.0)

# 认证和用户端点
ENDPOINT_AUTHENTICATION: Final[str] = "authentication"  # 用户认证端点
ENDPOINT_ALPHAS: Final[str] = "alphas"  # Alpha因子端点
ENDPOINT_SELF_ALPHA_LIST: Final[str] = "users/self/alphas"  # 获取用户自己的Alpha列表

# Alpha相关端点
ENDPOINT_ALPHA_YEARLY_STATS: Final[Callable[[str], str]] = (
    lambda alpha_id: f"alphas/{alpha_id}/recordsets/yearly-stats"
)
ENDPOINT_ALPHA_PNL: Final[Callable[[str], str]] = (
    lambda alpha_id: f"alphas/{alpha_id}/recordsets/pnl"
)
ENDPOINT_ALPHA_SELF_CORRELATIONS: Final[Callable[[str, str], str]] = (
    lambda alpha_id, correlation_type: f"alphas/{alpha_id}/correlations/{correlation_type}"
)
ENDPOINT_COMPETITIONS: Final[str] = "competitions"  # 竞赛端点
ENDPOINT_BEFORE_AND_AFTER_PERFORMANCE: Final[Callable[[Optional[str], str], str]] = (
    lambda competition_id, alpha_id: (
        f"competitions/{competition_id}/alphas/{alpha_id}/before-and-after-performance"
        if competition_id and alpha_id
        else f"users/self/alphas/{alpha_id}/before-and-after-performance"
    )
)

# 模拟端点
ENDPOINT_SIMULATION: Final[str] = "simulations"

# 用户活动端点
ENDPOINT_ACTIVITIES: Final[str] = "/users/self/activities/"
ENDPOINT_ACTIVITIES_SIMULATIONS: Final[str] = urljoin(ENDPOINT_ACTIVITIES, "simulations")
ENDPOINT_ACTIVITIES_PYRAMID_ALPHAS: Final[str] = urljoin(ENDPOINT_ACTIVITIES, "pyramid-alphas")
ENDPOINT_ACTIVITIES_DIVERSITY: Final[str] = urljoin(ENDPOINT_ACTIVITIES, "diversity")


# 数据相关端点
ENDPOINT_DATA_CATEGORIES: Final[str] = "data-categories"
ENDPOINT_DATA_SETS: Final[str] = "data-sets"
ENDPOINT_DATA_FIELDS: Final[str] = "data-fields"
ENDPOINT_OPERATORS: Final[str] = "operators"

# 前后性能对比默认路径
PATH_SELF_PERFORMANCE_COMPARE: Final[str] = "/users/self/"

# -----------------------------------------------------------------------------
# 用户角色相关常量
# -----------------------------------------------------------------------------


class UserRole(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CONSULTANT = "ROLE_CONSULTANT"  # 顾问角色
    USER = "ROLE_USER"  # 普通用户角色


# 用户角色常量 (兼容性保留，建议使用UserRole枚举)
ROLE_CONSULTANT: Final[str] = "CONSULTANT"  # 顾问角色标识符
ROLE_USER: Final[str] = "USER"  # 用户角色标识符


class Color(Enum):

    DEFAULT = "DEFAULT"
    NONE = "NONE"
    RED = "RED"
    GREEN = "GREEN"
    BLUE = "BLUE"
    YELLOW = "YELLOW"
    PURPLE = "PURPLE"


# -----------------------------------------------------------------------------
# 模拟回测相关常量
# -----------------------------------------------------------------------------

# 基于用户角色的最大模拟槽位数
MAX_CONSULTANT_SIMULATION_SLOTS: Final[int] = (
    10  # 顾问角色 (Consultant) 的最大并发模拟槽位数
)
MAX_USER_SIMULATION_SLOTS: Final[int] = 3  # 普通用户角色 (User) 的最大并发模拟槽位数

# 根据用户角色获取最大模拟槽位数的函数
# 输入角色字符串 (例如 ROLE_CONSULTANT)，返回对应的槽位数
MAX_SIMULATION_SLOTS: Final[Callable[[str], int]] = lambda role: (
    MAX_CONSULTANT_SIMULATION_SLOTS
    if role == ROLE_CONSULTANT
    else MAX_USER_SIMULATION_SLOTS
)

# 根据用户角色获取每个模拟槽位允许的最大作业数 (jobs) 的函数
# 输入角色字符串 (例如 ROLE_CONSULTANT)，返回对应的最大作业数
MAX_SIMULATION_JOBS_PER_SLOT: Final[Callable[[str], int]] = lambda role: (
    10 if role == ROLE_CONSULTANT else 1
)

# -----------------------------------------------------------------------------
# 顾问因子过滤基本要求常量 (Consultant Alpha Filtering Requirements)
# -----------------------------------------------------------------------------

# --- 通用要求 (适用于除中国外的区域) ---

# 适应度 (Fitness) 阈值
CONSULTANT_FITNESS_THRESHOLD_DELAY_0: Final[float] = 1.5  # 延迟 0 时的最低适应度
CONSULTANT_FITNESS_THRESHOLD_DELAY_1: Final[float] = 1.0  # 延迟 1 时的最低适应度

# 夏普比率 (Sharpe Ratio) 阈值
CONSULTANT_SHARPE_THRESHOLD_DELAY_0: Final[float] = 2.69  # 延迟 0 时的最低夏普比率
CONSULTANT_SHARPE_THRESHOLD_DELAY_1: Final[float] = 1.58  # 延迟 1 时的最低夏普比率

# 换手率 (Turnover) 范围 (%)
CONSULTANT_TURNOVER_MIN_PERCENT: Final[float] = 1.0  # 最低换手率 (百分比)
CONSULTANT_TURNOVER_MAX_PERCENT: Final[float] = 70.0  # 最高换手率 (百分比)

# 权重 (Weight) 限制 (%)
CONSULTANT_MAX_SINGLE_STOCK_WEIGHT_PERCENT: Final[float] = (
    10.0  # 单一股票最大权重限制 (百分比)
)
# 注意: 子宇宙测试 (Sub-universe Test) 的具体阈值依赖于子宇宙规模，难以用单一常量表示

# 相关性 (Correlation) 阈值
CONSULTANT_MAX_SELF_CORRELATION: Final[float] = (
    0.7  # 与用户其他 Alpha 的最大 PNL 相关性
)
CONSULTANT_MAX_PROD_CORRELATION: Final[float] = (
    0.7  # 与平台所有 Alpha 的最大 PNL 相关性
)
CONSULTANT_CORRELATION_SHARPE_IMPROVEMENT_FACTOR: Final[float] = (
    0.10  # 相关性高时，夏普比率需提高的比例 (10%)
)

# 样本内夏普比率/阶梯测试 (IS-Sharpe/Ladder Test)
# 注意: 此测试涉及多个年份和 D0/D1 阈值，难以用简单常量表示，通常在检查逻辑中实现

# 偏差测试 (Bias Test)
# 注意: 这是一个布尔型检查 (不应失败)，没有数值阈值常量

# --- 中国 (CHN) 区域特定要求 ---

# 延迟 1 (D1) 提交标准
CONSULTANT_CHN_SHARPE_THRESHOLD_DELAY_1: Final[float] = (
    2.08  # 中国区延迟 1 最低夏普比率
)
CONSULTANT_CHN_RETURNS_MIN_PERCENT_DELAY_1: Final[float] = (
    8.0  # 中国区延迟 1 最低收益率 (百分比)
)
CONSULTANT_CHN_FITNESS_THRESHOLD_DELAY_1: Final[float] = 1.0  # 中国区延迟 1 最低适应度

# 延迟 0 (D0) 提交标准
CONSULTANT_CHN_SHARPE_THRESHOLD_DELAY_0: Final[float] = 3.5  # 中国区延迟 0 最低夏普比率
CONSULTANT_CHN_RETURNS_MIN_PERCENT_DELAY_0: Final[float] = (
    12.0  # 中国区延迟 0 最低收益率 (百分比)
)
CONSULTANT_CHN_FITNESS_THRESHOLD_DELAY_0: Final[float] = 1.5  # 中国区延迟 0 最低适应度

# 稳健宇宙检验性能 (Robust Universe Test Performance) 阈值 (%)
CONSULTANT_CHN_ROBUST_UNIVERSE_MIN_RETENTION_PERCENT: Final[float] = (
    40.0  # 稳健宇宙成分保留的原收益和夏普值的最低比例
)

# --- 超级 Alpha (Superalphas) 特定要求 ---

# 超级 Alpha 换手率 (Turnover) 范围 (%)
CONSULTANT_SUPERALPHA_TURNOVER_MIN_PERCENT: Final[float] = (
    2.0  # 超级 Alpha 最低换手率 (百分比)
)
CONSULTANT_SUPERALPHA_TURNOVER_MAX_PERCENT: Final[float] = (
    40.0  # 超级 Alpha 最高换手率 (百分比)
)


# -----------------------------------------------------------------------------
# HTTP 错误代码映射关系
# -----------------------------------------------------------------------------
# 将HTTP状态码映射到其标准描述
HTTP_CODE_MESSAGE_MAP: Final[Dict[int, str]] = {
    # 1xx: 信息性响应
    100: "Continue",
    101: "Switching Protocols",
    102: "Processing",
    # 2xx: 成功
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
    # 3xx: 重定向
    300: "Multiple Choices",
    301: "Moved Permanently",
    302: "Found",
    303: "See Other",
    304: "Not Modified",
    305: "Use Proxy",
    307: "Temporary Redirect",
    308: "Permanent Redirect",
    # 4xx: 客户端错误
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
    # 5xx: 服务器错误
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

# -----------------------------------------------------------------------------
# 业务枚举类型定义
# -----------------------------------------------------------------------------


class Neutralization(Enum):

    DEFAULT = "DEFAULT"
    NONE = "NONE"
    MARKET = "MARKET"
    INDUSTRY = "INDUSTRY"
    SUBINDUSTRY = "SUBINDUSTRY"
    SECTOR = "SECTOR"
    COUNTRY = "COUNTRY"
    STATISTICAL = "STATISTICAL"
    CROWDING = "CROWDING"
    FAST = "FAST"
    SLOW = "SLOW"
    SLOW_AND_FAST = "SLOW_AND_FAST"


# 基础中性化策略
NEUTRALIZATION_BASIC: Final[List[Neutralization]] = [
    Neutralization.NONE,
    Neutralization.MARKET,
    Neutralization.INDUSTRY,
    Neutralization.SUBINDUSTRY,
    Neutralization.SECTOR,
]

# 扩展中性化策略
NEUTRALIZATION_EXTENDED: Final[List[Neutralization]] = [
    Neutralization.NONE,
    Neutralization.MARKET,
    Neutralization.INDUSTRY,
    Neutralization.SUBINDUSTRY,
    Neutralization.SECTOR,
    Neutralization.COUNTRY,
    Neutralization.STATISTICAL,
    Neutralization.CROWDING,
    Neutralization.FAST,
    Neutralization.SLOW,
    Neutralization.SLOW_AND_FAST,
]


class AlphaType(Enum):

    DEFAULT = "DEFAULT"
    REGULAR = "REGULAR"
    SUPER = "SUPER"


class RegularLanguage(Enum):

    DEFAULT = "DEFAULT"
    PYTHON = "PYTHON"
    EXPRESSION = "EXPRESSION"
    FASTEXPR = "FASTEXPR"


class Region(Enum):

    DEFAULT = "DEFAULT"
    AMR = "AMR"
    ASI = "ASI"
    CHN = "CHN"
    EUR = "EUR"
    GLB = "GLB"
    HKG = "HKG"
    JPN = "JPN"
    KOR = "KOR"
    TWN = "TWN"
    USA = "USA"


class InstrumentType(Enum):

    DEFAULT = "DEFAULT"
    EQUITY = "EQUITY"
    CRYPTO = "CRYPTO"


class Universe(Enum):

    DEFAULT = "DEFAULT"
    ILLIQUID_MINVOL1M = "ILLIQUID_MINVOL1M"  # 修正拼写错误，原为 "ILLQUID_MINVOL1M"
    MINVOL1M = "MINVOL1M"
    TOP5 = "TOP5"
    TOP10 = "TOP10"
    TOP20 = "TOP20"
    TOP50 = "TOP50"
    TOP100 = "TOP100"
    TOP200 = "TOP200"
    TOP400 = "TOP400"
    TOP500 = "TOP500"
    TOP600 = "TOP600"
    TOP800 = "TOP800"
    TOP1000 = "TOP1000"
    TOP1200 = "TOP1200"
    TOP1600 = "TOP1600"
    TOP2000 = "TOP2000"
    TOP2000U = "TOP2000U"
    TOP2500 = "TOP2500"
    TOP3000 = "TOP3000"
    TOPSP500 = "TOPSP500"


class Switch(Enum):

    DEFAULT = "DEFAULT"
    ON = "ON"
    OFF = "OFF"


class Delay(Enum):

    DEFAULT = -1
    ZERO = 0
    ONE = 1


class Decay(Enum):

    DEFAULT = -1
    MIN = 0
    MAX = 512


class Truncation(Enum):

    DEFAULT = -1
    MIN = 0
    MAX = 1


class LookbackDays(Enum):

    DEFAULT = 0
    DAYS_25 = 25
    DAYS_50 = 50
    DAYS_128 = 128
    DAYS_256 = 256
    DAYS_384 = 384
    DAYS_512 = 512


class UnitHandling(Enum):

    DEFAULT = "DEFAULT"
    VERIFY = "VERIFY"


class SelectionHandling(Enum):

    DEFAULT = "DEFAULT"
    POSITIVE = "POSITIVE"
    NON_ZERO = "NON_ZERO"
    NON_NAN = "NON_NAN"


class Category(Enum):

    DEFAULT = "DEFAULT"
    PRICE_REVERSION = "PRICE_REVERSION"
    PRICE_MOMENTUM = "PRICE_MOMENTUM"
    VOLUME = "VOLUME"
    FUNDAMENTAL = "FUNDAMENTAL"
    ANALYST = "ANALYST"
    PRICE_VOLUME = "PRICE_VOLUME"
    RELATION = "RELATION"
    SENTIMENT = "SENTIMENT"


class Grade(Enum):

    DEFAULT = "DEFAULT"
    SPECTACULAR = "SPECTACULAR"
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    AVERAGE = "AVERAGE"
    INFERIOR = "INFERIOR"


class Stage(Enum):

    DEFAULT = "DEFAULT"
    IS = "IS"  # 样本内
    OS = "OS"  # 样本外
    PROD = "PROD"  # 生产环境


class Status(Enum):

    DEFAULT = "DEFAULT"
    UNSUBMITTED = "UNSUBMITTED"
    ACTIVE = "ACTIVE"
    DECOMMISSIONED = "DECOMMISSIONED"


class DataFieldType(Enum):

    DEFAULT = "DEFAULT"
    MATRIX = "MATRIX"
    VECTOR = "VECTOR"
    GROUP = "GROUP"
    UNIVERSE = "UNIVERSE"


class CheckRecordType(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CORRELATION_SELF = "CORRELATION_SELF"  # 自相关性检查
    CORRELATION_PROD = "CORRELATION_PROD"  # 生产环境相关性检查
    BEFORE_AND_AFTER_PERFORMANCE = (
        "BEFORE_AND_AFTER_PERFORMANCE"  # 提交前后性能对比检查
    )
    SUBMISSION = "SUBMISSION"  # 提交检查


class SubmissionCheckType(Enum):

    DEFAULT = "DEFAULT"
    CONCENTRATED_WEIGHT = "CONCENTRATED_WEIGHT"
    D0_SUBMISSION = "D0_SUBMISSION"
    DATA_DIVERSITY = "DATA_DIVERSITY"
    DRAWDOWN = "DRAWDOWN"
    HIGH_TURNOVER = "HIGH_TURNOVER"
    IS_LADDER_SHARPE = "IS_LADDER_SHARPE"
    IS_SHARPE = "IS_SHARPE"
    LONG_DURATION = "LONG_DURATION"
    LOW_2Y_SHARPE = "LOW_2Y_SHARPE"
    LOW_AFTER_COST_ILLIQUID_UNIVERSE_SHARPE = "LOW_AFTER_COST_ILLIQUID_UNIVERSE_SHARPE"
    LOW_FITNESS = "LOW_FITNESS"
    LOW_RETURNS = "LOW_RETURNS"
    LOW_ROBUST_UNIVERSE_RETURNS = "LOW_ROBUST_UNIVERSE_RETURNS"
    LOW_ROBUST_UNIVERSE_SHARPE = "LOW_ROBUST_UNIVERSE_SHARPE"
    LOW_SHARPE = "LOW_SHARPE"
    LOW_SUB_UNIVERSE_SHARPE = "LOW_SUB_UNIVERSE_SHARPE"
    LOW_TURNOVER = "LOW_TURNOVER"
    MATCHES_COMPETITION = "MATCHES_COMPETITION"
    MATCHES_PYRAMID = "MATCHES_PYRAMID"
    MATCHES_THEMES = "MATCHES_THEMES"
    MEMORY_USAGE = "MEMORY_USAGE"
    NEW_HIGH = "NEW_HIGH"
    POWER_POOL_CORRELATION = "POWER_POOL_CORRELATION"
    PROD_CORRELATION = "PROD_CORRELATION"
    RANK_SHARPE = "RANK_SHARPE"
    REGULAR_SUBMISSION = "REGULAR_SUBMISSION"
    SELF_CORRELATION = "SELF_CORRELATION"
    SHARPE = "SHARPE"
    SUB_UNIVERSE_SHARPE = "SUB_UNIVERSE_SHARPE"
    SUPER_SUBMISSION = "SUPER_SUBMISSION"
    SUPER_UNIVERSE_SHARPE = "SUPER_UNIVERSE_SHARPE"
    UNITS = "UNITS"


class RecordSetType(Enum):
    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    PNL = "PNL"  # 收益率
    YEARLY_STATS = "YEARLY_STATS"  # 年度统计


class SubmissionCheckResult(Enum):
    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    PASS = "PASS"  # 检查通过
    PENDING = "PENDING"  # 检查待定
    WARNING = "WARNING"  # 检查警告
    ERROR = "ERROR"  # 检查错误
    FAIL = "FAIL"  # 检查失败


class CorrelationType(Enum):

    DEFAULT = "DEFAULT"
    SELF = "self"  # 自相关
    PROD = "prod"  # 生产环境相关性


class CorrelationCalcType(Enum):

    DEFAULT = "DEFAULT"
    LOCAL = "LOCAL"
    PLATFORM = "PLATFORM"


class CompetitionStatus(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    EXCLUDED = "EXCLUDED"  # 排除
    ACCEPTED = "ACCEPTED"  # 接受


class CompetitionScoring(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CHALLENGE = "CHALLENGE"  # 挑战赛
    PERFORMANCE = "PERFORMANCE"  # 性能赛


# -----------------------------------------------------------------------------
# 评估相关枚举
# -----------------------------------------------------------------------------


class RefreshPolicy(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    USE_EXISTING = "USE_EXISTING"
    FORCE_REFRESH = "FORCE_REFRESH"
    SKIP_IF_MISSING = "SKIP_IF_MISSING"
    REFRESH_ASYNC_IF_MISSING = "REFRESH_ASYNC_IF_MISSING"


class CorrelationSource(Enum):

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    API = "API"
    LOCAL = "LOCAL"


# -----------------------------------------------------------------------------
# 对象关系映射
# -----------------------------------------------------------------------------

# 证券类型到支持地区的映射
INSTRUMENT_TYPE_REGION_MAP: Final[Dict[InstrumentType, List[Region]]] = {
    InstrumentType.EQUITY: [
        Region.GLB,
        Region.EUR,
        Region.ASI,
        Region.CHN,
        Region.KOR,
        Region.TWN,
        Region.HKG,
        Region.JPN,
        Region.AMR,
        Region.USA,
    ],
    InstrumentType.CRYPTO: [
        Region.GLB,
    ],
}

# 地区到支持证券类型的反向映射
REGION_INSTRUMENT_TYPE_MAP: Final[Dict[Region, List[InstrumentType]]] = {
    region: [
        inst_type
        for inst_type, regions in INSTRUMENT_TYPE_REGION_MAP.items()
        if region in regions
    ]
    for region in Region
    if region != Region.DEFAULT
}

# 选股范围映射
EQUITY_REGION_UNIVERSE_MAP: Final[Dict[Region, List[Universe]]] = {
    Region.USA: [
        Universe.TOP3000,
        Universe.TOP1000,
        Universe.TOP500,
        Universe.TOP200,
        Universe.ILLIQUID_MINVOL1M,
        Universe.TOPSP500,
    ],
    Region.ASI: [
        Universe.MINVOL1M,
        Universe.ILLIQUID_MINVOL1M,
    ],
    Region.CHN: [
        Universe.TOP2000U,
    ],
    Region.KOR: [
        Universe.TOP600,
    ],
    Region.TWN: [
        Universe.TOP500,
        Universe.TOP100,
    ],
    Region.HKG: [
        Universe.TOP800,
        Universe.TOP500,
    ],
    Region.JPN: [
        Universe.TOP1600,
        Universe.TOP1200,
    ],
    Region.AMR: [
        Universe.TOP600,
    ],
    Region.EUR: [
        Universe.TOP2500,
        Universe.TOP1200,
        Universe.TOP800,
        Universe.TOP400,
        Universe.ILLIQUID_MINVOL1M,
    ],
    Region.GLB: [
        Universe.TOP3000,
        Universe.MINVOL1M,
    ],
}

# 加密货币的选股范围映射
CRYPTO_REGION_UNIVERSE_MAP: Final[Dict[Region, List[Universe]]] = {
    Region.GLB: [
        Universe.TOP50,
        Universe.TOP20,
        Universe.TOP10,
        Universe.TOP5,
    ],
}

# 证券类型到Universe映射关系的整合
INSTRUMENT_TYPE_UNIVERSE_MAP: Final[
    Dict[InstrumentType, Dict[Region, List[Universe]]]
] = {
    InstrumentType.EQUITY: EQUITY_REGION_UNIVERSE_MAP,
    InstrumentType.CRYPTO: CRYPTO_REGION_UNIVERSE_MAP,
}

# 区域支持的延迟配置
REGION_DELAY_MAP: Final[Dict[Region, List[Delay]]] = {
    Region.USA: [Delay.ZERO, Delay.ONE],
    Region.GLB: [Delay.ONE],
    Region.EUR: [Delay.ZERO, Delay.ONE],
    Region.ASI: [Delay.ONE],
    Region.CHN: [Delay.ZERO, Delay.ONE],
    Region.KOR: [Delay.ONE],
    Region.TWN: [Delay.ONE],
    Region.HKG: [Delay.ONE],
    Region.JPN: [Delay.ONE],
    Region.AMR: [Delay.ZERO, Delay.ONE],
}

# 证券类型支持的中性化策略
INSTRUMENT_TYPE_NEUTRALIZATION_MAP: Final[
    Dict[InstrumentType, List[Neutralization]]
] = {
    InstrumentType.EQUITY: NEUTRALIZATION_EXTENDED,
    InstrumentType.CRYPTO: [Neutralization.NONE, Neutralization.MARKET],
}

# 特定区域支持的中性化策略
REGION_NEUTRALIZATION_MAP: Final[Dict[Region, List[Neutralization]]] = {
    Region.USA: [
        Neutralization.NONE,
        Neutralization.STATISTICAL,
        Neutralization.CROWDING,
        Neutralization.FAST,
        Neutralization.SLOW,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.SLOW_AND_FAST,
    ],
    Region.GLB: [
        Neutralization.NONE,
        Neutralization.STATISTICAL,
        Neutralization.CROWDING,
        Neutralization.FAST,
        Neutralization.SLOW,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.COUNTRY,
        Neutralization.SLOW_AND_FAST,
    ],
    Region.EUR: [
        Neutralization.NONE,
        Neutralization.STATISTICAL,
        Neutralization.CROWDING,
        Neutralization.FAST,
        Neutralization.SLOW,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.COUNTRY,
        Neutralization.SLOW_AND_FAST,
    ],
    Region.ASI: [
        Neutralization.NONE,
        Neutralization.CROWDING,
        Neutralization.FAST,
        Neutralization.SLOW,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.COUNTRY,
        Neutralization.SLOW_AND_FAST,
    ],
    Region.CHN: [
        Neutralization.NONE,
        Neutralization.CROWDING,
        Neutralization.FAST,
        Neutralization.SLOW,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.SLOW_AND_FAST,
    ],
    Region.KOR: NEUTRALIZATION_BASIC,
    Region.TWN: NEUTRALIZATION_BASIC,
    Region.HKG: NEUTRALIZATION_BASIC,
    Region.JPN: NEUTRALIZATION_BASIC,
    Region.AMR: [
        Neutralization.NONE,
        Neutralization.MARKET,
        Neutralization.SECTOR,
        Neutralization.INDUSTRY,
        Neutralization.SUBINDUSTRY,
        Neutralization.COUNTRY,
    ],
}

# -----------------------------------------------------------------------------
# 辅助函数
# -----------------------------------------------------------------------------


@lru_cache(maxsize=128)
def get_regions_for_instrument_type(instrument_type: InstrumentType) -> List[Region]:

    return INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


@lru_cache(maxsize=128)
def get_instrument_types_for_region(region: Region) -> List[InstrumentType]:

    return REGION_INSTRUMENT_TYPE_MAP.get(region, [])


def is_region_supported_for_instrument_type(
    region: Region, instrument_type: InstrumentType
) -> bool:

    if region == Region.DEFAULT or instrument_type == InstrumentType.DEFAULT:
        raise ValueError("不能使用DEFAULT枚举值进行支持检查")

    return region in INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


@lru_cache(maxsize=128)
def get_universe_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Universe]:

    return INSTRUMENT_TYPE_UNIVERSE_MAP.get(instrument_type, {}).get(region, [])


@lru_cache(maxsize=128)
def get_neutralization_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Neutralization]:

    if instrument_type is None or instrument_type == InstrumentType.DEFAULT:
        raise ValueError("instrument_type不能为None或DEFAULT")

    if region is None or region == Region.DEFAULT:
        raise ValueError("region不能为None或DEFAULT")

    if instrument_type == InstrumentType.CRYPTO:
        return INSTRUMENT_TYPE_NEUTRALIZATION_MAP[InstrumentType.CRYPTO]

    return REGION_NEUTRALIZATION_MAP.get(region, NEUTRALIZATION_BASIC)


@lru_cache(maxsize=128)
def get_delay_for_region(region: Region) -> List[Delay]:

    return REGION_DELAY_MAP.get(region, [Delay.ONE])


# 在文件结尾添加自检代码，确保常量值与枚举定义一致
if __name__ == "__main__":
    # 验证枚举和常量定义的一致性
    assert Environment.PROD.value == ENV_PROD
    assert Environment.DEV.value == ENV_DEV
    assert Environment.TEST.value == ENV_TEST

    # 验证数据库枚举和常量的一致性
    assert Database.ALPHAS.value == DB_ALPHAS
    assert Database.DATA.value == DB_DATA
    assert Database.SIMULATION.value == DB_SIMULATION

    # 验证所有映射的完整性
    for inst_type in InstrumentType:
        if inst_type != InstrumentType.DEFAULT:
            regions = get_regions_for_instrument_type(inst_type)
            assert len(regions) > 0, f"证券类型 {inst_type} 没有关联的地区"

    # 验证新增枚举的默认值
    assert RefreshPolicy.DEFAULT.value == "DEFAULT"
    assert CorrelationSource.DEFAULT.value == "DEFAULT"

    print("所有常量检查通过!")
