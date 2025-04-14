"""AlphaPower包的常量定义。

此模块定义了AlphaPower包中使用的各种常量，包括环境设置、数据库名称、
API端点、用户角色、模拟设置和HTTP状态码映射。

常量按照功能分组：
- 环境常量：用于指定运行环境（生产、开发、测试）
- 数据库常量：定义了系统使用的不同数据库名称
- API常量：定义了与API交互所需的各种URL和端点
- 用户角色常量：定义了系统中用户可能具有的权限级别
- 枚举类型：定义了系统中使用的各种枚举类型，如地区、证券类型等
- 映射关系：定义了不同枚举类型之间的关系和约束

典型用法:
  from alphapower.constants import Environment, BASE_URL, InstrumentType, Region

  # 使用环境枚举
  current_env = Environment.DEV

  # 使用API端点构建请求URL
  auth_url = f"{BASE_URL}/{ENDPOINT_AUTHENTICATION}"

  # 检查特定组合是否有效
  is_valid = is_region_supported_for_instrument_type(Region.CHINA, InstrumentType.EQUITY)

  # 获取特定地区支持的证券类型
  instrument_types = get_instrument_types_for_region(Region.GLOBAL)
"""

from __future__ import annotations  # 添加此行解决前向引用问题

from enum import Enum
from functools import lru_cache  # 添加缓存支持
from typing import Callable, Dict, Final, List, Tuple

# 类型别名定义，提高代码可读性
# 注意：使用字符串作为类型注解，解决类型前向引用问题
UniverseMap = Dict["Region", List["Universe"]]
NeutralizationMap = Dict["Region", List["Neutralization"]]

# -----------------------------------------------------------------------------
# 基础环境和系统配置
# -----------------------------------------------------------------------------


class Environment(Enum):
    """环境设置枚举。

    定义了系统可能运行的不同环境类型。

    Attributes:
        PROD: 生产环境，用于实际部署的系统
        DEV: 开发环境，用于开发和测试新功能
        TEST: 测试环境，用于系统测试和集成测试
    """

    PROD = "prod"  # 生产环境
    DEV = "dev"  # 开发环境
    TEST = "test"  # 测试环境


# 环境设置常量
# 注意：建议使用Environment枚举而非这些字符串常量
ENV_PROD: Final[str] = "prod"  # 生产环境标识符
ENV_DEV: Final[str] = "dev"  # 开发环境标识符
ENV_TEST: Final[str] = "test"  # 测试环境标识符

ALPHA_ID_LENGTH: Final[int] = 7  # Alpha ID的长度


class Database(Enum):
    """数据库名称枚举。

    定义了系统中使用的不同数据库名称。

    Attributes:
        ALPHAS: Alpha因子数据库，存储算法因子数据
        DATA: 市场数据库，存储市场相关数据
        SIMULATION: 模拟回测数据库，存储模拟结果
    """

    ALPHAS = "alphas"  # Alpha因子数据库
    DATA = "data"  # 市场数据库
    SIMULATION = "simulation"  # 模拟回测数据库
    CHECKS = "checks"  # 检查数据库


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
ENDPOINT_BEFORE_AND_AFTER_PERFORMANCE: Final[Callable[[str, str], str]] = (
    lambda competition_id, alpha_id: f"/{competition_id}/alphas/{alpha_id}/before-and-after-performance"
)

# 模拟端点
ENDPOINT_SIMULATION: Final[str] = "simulations"
ENDPOINT_ACTIVITIES_SIMULATION: Final[str] = "users/self/activities/simulations"

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
    """用户角色枚举。

    定义了系统中可能的用户角色。

    Attributes:
        CONSULTANT: 顾问角色，通常具有更高权限
        USER: 普通用户角色，具有基本操作权限
    """

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CONSULTANT = "ROLE_CONSULTANT"  # 顾问角色
    USER = "ROLE_USER"  # 普通用户角色


# 用户角色常量 (兼容性保留，建议使用UserRole枚举)
ROLE_CONSULTANT: Final[str] = "CONSULTANT"  # 顾问角色标识符
ROLE_USER: Final[str] = "USER"  # 用户角色标识符


class Color(Enum):
    """颜色枚举。

    定义了系统中可能使用的颜色标记。

    Attributes:
        DEFAULT: 默认值，无意义
        NONE: 无颜色
        RED: 红色
        GREEN: 绿色
        BLUE: 蓝色
        YELLOW: 黄色
        PURPLE: 紫色
    """

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
MAX_CONSULTANT_SIMULATION_SLOTS: Final[int] = 10
MAX_USER_SIMULATION_SLOTS: Final[int] = 3
MAX_SIMULATION_SLOTS: Final[Callable[[str], int]] = lambda role: (
    MAX_CONSULTANT_SIMULATION_SLOTS
    if role == ROLE_CONSULTANT
    else MAX_USER_SIMULATION_SLOTS
)

MAX_SIMULATION_JOBS_PER_SLOT: Final[Callable[[str], int]] = lambda role: (
    10 if role == ROLE_CONSULTANT else 1
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
    """中性化策略枚举。

    定义了系统中可能使用的中性化策略。

    Attributes:
        DEFAULT: 默认值，无实际意义
        NONE: 不进行中性化
        MARKET: 市场中性
        INDUSTRY: 产业中性
        SUBINDUSTRY: 子产业中性
        SECTOR: 行业中性
        COUNTRY: 国家/地区中性
        STATISTICAL: 统计中性
        CROWDING: 拥挤因子
        FAST: 快速因子
        SLOW: 慢速因子
        SLOW_AND_FAST: 慢速+快速因子
    """

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
    """Alpha类型枚举。

    定义了系统中可能的Alpha类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        REGULAR: 常规Alpha
        SUPER: 超级Alpha
    """

    DEFAULT = "DEFAULT"
    REGULAR = "REGULAR"
    SUPER = "SUPER"


class RegularLanguage(Enum):
    """语言枚举。

    定义了系统中可能使用的编程语言或表达式类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        PYTHON: Python语言
        EXPRESSION: 表达式语言
        FASTEXPR: 快速表达式语言
    """

    DEFAULT = "DEFAULT"
    PYTHON = "PYTHON"
    EXPRESSION = "EXPRESSION"
    FASTEXPR = "FASTEXPR"


class Region(Enum):
    """地区枚举。

    定义了系统中可能涉及的地理区域。
    每个枚举值关联RegionInfo对象提供额外元数据。

    Attributes:
        DEFAULT: 默认值，无实际意义
        AMERICA: 美洲
        ASIA: 亚洲
        CHINA: 中国
        EUROPE: 欧洲
        GLOBAL: 全球
        HONGKONG: 香港
        JAPAN: 日本
        KOREA: 韩国
        TAIWAN: 台湾
        USA: 美国
    """

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
    """证券类型枚举。

    定义了系统中可能涉及的证券类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        EQUITY: 股票
        CRYPTO: 数字货币
    """

    DEFAULT = "DEFAULT"
    EQUITY = "EQUITY"
    CRYPTO = "CRYPTO"


class Universe(Enum):
    """选股范围枚举。

    定义了系统中可能使用的选股范围。

    Attributes:
        DEFAULT: 默认值，无实际意义
        ILLIQUID_MINVOL1M: 低流动性股票池
        MINVOL1M: 低波动性股票池
        TOP5: 前5只股票
        TOP10: 前10只股票
        TOP20: 前20只股票
        TOP50: 前50只股票
        TOP100: 前100只股票
        TOP200: 前200只股票
        TOP400: 前400只股票
        TOP500: 前500只股票
        TOP600: 前600只股票
        TOP800: 前800只股票
        TOP1000: 前1000只股票
        TOP1200: 前1200只股票
        TOP1600: 前1600只股票
        TOP2000: 前2000只股票
        TOP2000U: 前2000只中国股票
        TOP2500: 前2500只股票
        TOP3000: 前3000只股票
        TOPSP500: 前500只股票(S&P500)
    """

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
    """开关枚举。

    定义了系统中可能使用的开关类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        ON: 开启
        OFF: 关闭
    """

    DEFAULT = "DEFAULT"
    ON = "ON"
    OFF = "OFF"


class Delay(Enum):
    """延迟枚举。

    定义了Alpha因子的延迟天数。

    Attributes:
        DEFAULT: 默认值，无实际意义
        ZERO: 0天延迟
        ONE: 1天延迟
    """

    DEFAULT = -1
    ZERO = 0
    ONE = 1


class Decay(Enum):
    """衰减枚举。
    Int 类型，定义了Alpha因子的衰减天数。
    Attributes:
        DEFAULT: 默认值，无实际意义
        MIN: 最小值
        MAX: 最大值
    """

    DEFAULT = -1
    MIN = 0
    MAX = 512


class Truncation(Enum):
    """截断枚举。
    Float 类型，定义了Alpha因子的截断方式。
    Attributes:
        DEFAULT: 默认值，无实际意义
        MIN: 最小值
        MAX: 最大值
    """

    DEFAULT = -1
    MIN = 0
    MAX = 1


class LookbackDays(Enum):
    """回溯天数枚举。

    定义了系统中可能使用的回溯天数。

    Attributes:
        DEFAULT: 默认值，无实际意义
        DAYS_25: 25天
        DAYS_50: 50天
        DAYS_128: 128天
        DAYS_256: 256天
        DAYS_384: 384天
        DAYS_512: 512天
    """

    DEFAULT = 0
    DAYS_25 = 25
    DAYS_50 = 50
    DAYS_128 = 128
    DAYS_256 = 256
    DAYS_384 = 384
    DAYS_512 = 512


class UnitHandling(Enum):
    """单位处理枚举。

    定义了系统中可能使用的单位处理方式。

    Attributes:
        DEFAULT: 默认值，无实际意义
        VERIFY: 验证
    """

    DEFAULT = "DEFAULT"
    VERIFY = "VERIFY"


class SelectionHandling(Enum):
    """选择处理枚举。

    定义了系统中可能使用的选择处理方式。

    Attributes:
        DEFAULT: 默认值，无实际意义
        POSITIVE: 正值
        NON_ZERO: 非零
        NON_NAN: 非NaN
    """

    DEFAULT = "DEFAULT"
    POSITIVE = "POSITIVE"
    NON_ZERO = "NON_ZERO"
    NON_NAN = "NON_NAN"


class Category(Enum):
    """Alpha分类枚举。

    定义了系统中可能使用的Alpha分类。

    Attributes:
        DEFAULT: 默认值，无实际意义
        PRICE_REVERSION: 价格回归
        PRICE_MOMENTUM: 价格动量
        VOLUME: 成交量
        FUNDAMENTAL: 基本面
        ANALYST: 分析师
        PRICE_VOLUME: 价量关系
        RELATION: 关联关系
        SENTIMENT: 情绪
    """

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
    """评级枚举。

    定义了系统中可能使用的评级。

    Attributes:
        DEFAULT: 默认值，无实际意义
        SPECTACULAR: 卓越
        EXCELLENT: 优秀
        GOOD: 良好
        AVERAGE: 一般
        INFERIOR: 需改进
    """

    DEFAULT = "DEFAULT"
    SPECTACULAR = "SPECTACULAR"
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    AVERAGE = "AVERAGE"
    INFERIOR = "INFERIOR"


class Stage(Enum):
    """阶段枚举。

    定义了系统中可能使用的阶段。

    Attributes:
        DEFAULT: 默认值，无实际意义
        IS: 样本内
        OS: 样本外
        PROD: 生产环境
    """

    DEFAULT = "DEFAULT"
    IS = "IS"  # 样本内
    OS = "OS"  # 样本外
    PROD = "PROD"  # 生产环境


class Status(Enum):
    """状态枚举。

    定义了系统中可能使用的状态。

    Attributes:
        DEFAULT: 默认值，无实际意义
        UNSUBMITTED: 未提交
        ACTIVE: 活跃
        DECOMMISSIONED: 退役
    """

    DEFAULT = "DEFAULT"
    UNSUBMITTED = "UNSUBMITTED"
    ACTIVE = "ACTIVE"
    DECOMMISSIONED = "DECOMMISSIONED"


class DataFieldType(Enum):
    """数据字段类型枚举。

    定义了系统中可能使用的数据字段类型。
    Attributes:
        DEFAULT: 默认值，无实际意义
        MATRIX: 矩阵类型
        VECTOR: 向量类型
        GROUP: 组类型
        UNIVERSE: 股票池类型
    """

    DEFAULT = "DEFAULT"
    MATRIX = "MATRIX"
    VECTOR = "VECTOR"
    GROUP = "GROUP"
    UNIVERSE = "UNIVERSE"


class CheckRecordType(Enum):
    """检查记录类型枚举。
    定义了系统中可能使用的检查记录类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        CORRELATION: 相关性检查
        BEFORE_AND_AFTER_PERFORMANCE: 提交前后性能对比检查
        SUBMISSION: 提交检查
    """

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CORRELATION = "CORRELATION"  # 相关性检查
    BEFORE_AND_AFTER_PERFORMANCE = (
        "BEFORE_AND_AFTER_PERFORMANCE"  # 提交前后性能对比检查
    )
    SUBMISSION = "SUBMISSION"  # 提交检查


class CheckType(Enum):
    """检查类型枚举。
    定义了系统中可能使用的检查类型。
    Attributes:
        DEFAULT: 默认值，无实际意义
        LOW_SHARPE: 低夏普比率
        LOW_FITNESS: 低适应度
        LOW_TURNOVER: 低换手率
        HIGH_TURNOVER: 高换手率
        CONCENTRATED_WEIGHT: 集中持仓
        LOW_SUB_UNIVERSE_SHARPE: 低子股票池夏普比率
        SELF_CORRELATION: 自相关性检查
        DATA_DIVERSITY: 数据多样性检查
        PROD_CORRELATION: 生产环境相关性检查
        REGULAR_SUBMISSION: 定期提交检查
        MATCHES_COMPETITION: 匹配竞赛检查
        LOW_2Y_SHARPE: 低2年夏普比率
        MATCHES_PYRAMID: 匹配金字塔检查
        MATCHES_THEMES: 匹配主题检查
        POWER_POOL_CORRELATION: 力量池相关性检查
    """

    DEFAULT = "DEFAULT"
    LOW_SHARPE = "LOW_SHARPE"
    LOW_FITNESS = "LOW_FITNESS"
    LOW_TURNOVER = "LOW_TURNOVER"
    HIGH_TURNOVER = "HIGH_TURNOVER"
    CONCENTRATED_WEIGHT = "CONCENTRATED_WEIGHT"
    LOW_SUB_UNIVERSE_SHARPE = "LOW_SUB_UNIVERSE_SHARPE"
    SELF_CORRELATION = "SELF_CORRELATION"
    DATA_DIVERSITY = "DATA_DIVERSITY"
    PROD_CORRELATION = "PROD_CORRELATION"
    REGULAR_SUBMISSION = "REGULAR_SUBMISSION"
    MATCHES_COMPETITION = "MATCHES_COMPETITION"
    LOW_2Y_SHARPE = "LOW_2Y_SHARPE"
    MATCHES_PYRAMID = "MATCHES_PYRAMID"
    MATCHES_THEMES = "MATCHES_THEMES"
    POWER_POOL_CORRELATION = "POWER_POOL_CORRELATION"
    UNITS = "UNITS"
    IS_LADDER_SHARPE = "IS_LADDER_SHARPE"


class CorrelationType(Enum):
    """相关性类型枚举。

    定义了系统中可能使用的相关性类型。

    Attributes:
        DEFAULT: 默认值，无实际意义
        SELF: 自相关
        PROD: 生产环境相关性
    """

    DEFAULT = "DEFAULT"
    SELF = "self"  # 自相关
    PROD = "prod"  # 生产环境相关性


class CompetitionStatus(Enum):
    """竞赛状态枚举。
    定义了系统中可能使用的竞赛状态。

    Attributes:
        DEFAULT: 默认值，无实际意义
        EXCLUDED: 排除
        ACCEPTED: 接受
    """

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    EXCLUDED = "EXCLUDED"  # 排除
    ACCEPTED = "ACCEPTED"  # 接受


class CompetitionScoring(Enum):
    """竞赛评分枚举。
    定义了系统中可能使用的竞赛评分方式。

    Attributes:
        DEFAULT: 默认值，无实际意义
        CHALLENGE: 挑战赛
        PERFORMANCE: 性能赛
    """

    DEFAULT = "DEFAULT"  # 默认值，无实际意义
    CHALLENGE = "CHALLENGE"  # 挑战赛
    PERFORMANCE = "PERFORMANCE"  # 性能赛


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
    """获取指定证券类型支持的所有地区。

    Args:
        instrument_type: 要查询的证券类型

    Returns:
        支持该证券类型的地区列表
    """
    return INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


@lru_cache(maxsize=128)
def get_instrument_types_for_region(region: Region) -> List[InstrumentType]:
    """获取支持指定地区的所有证券类型。

    Args:
        region: 要查询的地区

    Returns:
        支持该地区的证券类型列表
    """
    return REGION_INSTRUMENT_TYPE_MAP.get(region, [])


def is_region_supported_for_instrument_type(
    region: Region, instrument_type: InstrumentType
) -> bool:
    """检查指定地区是否支持指定证券类型。

    Args:
        region: 要检查的地区
        instrument_type: 要检查的证券类型

    Returns:
        如果地区支持该证券类型则返回True，否则返回False

    Raises:
        ValueError: 当region是Region.DEFAULT或instrument_type是InstrumentType.DEFAULT时
    """
    if region == Region.DEFAULT or instrument_type == InstrumentType.DEFAULT:
        raise ValueError("不能使用DEFAULT枚举值进行支持检查")

    return region in INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


@lru_cache(maxsize=128)
def get_universe_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Universe]:
    """获取指定证券类型和地区支持的所有Universe。

    Args:
        instrument_type: 要查询的证券类型
        region: 要查询的地区

    Returns:
        支持的Universe列表
    """
    return INSTRUMENT_TYPE_UNIVERSE_MAP.get(instrument_type, {}).get(region, [])


@lru_cache(maxsize=128)
def get_neutralization_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Neutralization]:
    """获取指定证券类型和地区支持的所有中性化策略。

    Args:
        instrument_type: 要查询的证券类型
        region: 要查询的地区

    Returns:
        支持的中性化策略列表

    Raises:
        ValueError: 当instrument_type或region是None或DEFAULT时
    """
    if instrument_type is None or instrument_type == InstrumentType.DEFAULT:
        raise ValueError("instrument_type不能为None或DEFAULT")
    if region is None or region == Region.DEFAULT:
        raise ValueError("region不能为None或DEFAULT")

    if instrument_type == InstrumentType.CRYPTO:
        return INSTRUMENT_TYPE_NEUTRALIZATION_MAP[InstrumentType.CRYPTO]

    return REGION_NEUTRALIZATION_MAP.get(region, NEUTRALIZATION_BASIC)


@lru_cache(maxsize=128)
def get_delay_for_region(region: Region) -> List[Delay]:
    """获取指定地区支持的所有延迟配置。

    Args:
        region: 要查询的地区

    Returns:
        支持的延迟配置列表
    """
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

    print("所有常量检查通过!")
