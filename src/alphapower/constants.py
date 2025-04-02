"""
AlphaPower包的常量定义。

此模块定义了AlphaPower包中使用的各种常量，
包括环境设置、数据库名称、API端点、用户角色、
模拟设置和HTTP状态码映射。
"""

from enum import Enum
from typing import Callable, Dict, Final, List, Tuple

# 环境设置
ENV_PROD: Final[str] = "prod"
ENV_DEV: Final[str] = "dev"
ENV_TEST: Final[str] = "test"

# 数据库名称
DB_ALPHAS: Final[str] = "alphas"
DB_DATA: Final[str] = "data"
DB_SIMULATION: Final[str] = "simulation"

"""
API 路由相关常量
"""
# API基础URL
BASE_URL: Final[str] = "https://api.worldquantbrain.com"
DEFAULT_SIMULATION_RESPONSE: Final[Tuple[bool, str, float]] = (False, "", 0.0)

# 认证和用户端点
ENDPOINT_AUTHENTICATION: Final[str] = "authentication"
ENDPOINT_ALPHAS: Final[str] = "alphas"
ENDPOINT_SELF_ALPHA_LIST: Final[str] = "users/self/alphas"

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

# 模拟端点
ENDPOINT_SIMULATION: Final[str] = "simulations"
ENDPOINT_ACTIVITIES_SIMULATION: Final[str] = "users/self/activities/simulations"

# 数据相关端点
ENDPOINT_DATA_CATEGORIES: Final[str] = "data-categories"
ENDPOINT_DATA_SETS: Final[str] = "data-sets"
ENDPOINT_DATA_FIELDS: Final[str] = "data-fields"
ENDPOINT_OPERATORS: Final[str] = "operators"

"""
用户身份相关常量
"""
ROLE_CONSULTANT: Final[str] = "CONSULTANT"
ROLE_USER: Final[str] = "USER"

"""
模拟回测相关常量
"""
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

# HTTP 错误代码映射关系
# 将HTTP状态码映射到其标准描述
HTTP_CODE_MESSAGE_MAP: Final[Dict[int, str]] = {
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


class Neutralization(Enum):
    """
    中性化策略枚举

    DEFAULT - 默认值，无实际意义
    NONE - 不进行中性化
    MARKET - 市场中性
    INDUSTRY - 产业中性
    SUBINDUSTRY - 子产业中性
    SECTOR - 行业中性
    COUNTRY - 国家/地区中性
    STATISTICAL - 统计中性
    CROWDING - 拥挤因子
    FAST - 快速因子
    SLOW - 慢速因子
    SLOW_AND_FAST - 慢速+快速因子
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
    """
    Alpha类型枚举
    """

    DEFAULT = "DEFAULT"
    REGULAR = "REGULAR"
    SUPER = "SUPER"


class RegularLanguage(Enum):
    """
    语言枚举

    DEFAULT - 默认值，无实际意义
    PYTHON - Python
    EXPRESSION - 表达式
    FASTEXPR - 快速表达式
    """

    DEFAULT = "DEFAULT"
    PYTHON = "PYTHON"
    EXPRESSION = "EXPRESSION"
    FASTEXPR = "FASTEXPR"


class Region(Enum):
    """
    地区枚举

    DEFAULT - 默认值，无实际意义
    AMR - 美洲
    ASI - 亚洲
    CHN - 中国
    EUR - 欧洲
    GLB - 全球
    HKG - 香港
    JPN - 日本
    KOR - 韩国
    TWN - 台湾
    USA - 美国
    """

    DEFAULT = "DEFAULT"
    AMERICA = "AMR"
    ASIA = "ASI"
    CHINA = "CHN"
    EUROPE = "EUR"
    GLOBAL = "GLB"
    HONGKONG = "HKG"
    JAPAN = "JPN"
    KOREA = "KOR"
    TAIWAN = "TWN"
    USA = "USA"


class InstrumentType(Enum):
    """
    证券类型枚举

    DEFAULT - 默认值，无实际意义
    EQUITY - 股票
    CRYPTO - 数字货币
    """

    DEFAULT = "DEFAULT"
    EQUITY = "EQUITY"
    CRYPTO = "CRYPTO"


class Universe(Enum):
    """
    选股范围枚举

    DEFAULT - 默认值，无实际意义
    ILLIQUID_MINVOL1M - 低流动性股票池
    MINVOL1M - 低波动性股票池
    TOP5 - 前5只股票
    TOP10 - 前10只股票
    TOP20 - 前20只股票
    TOP50 - 前50只股票
    TOP100 - 前100只股票
    TOP200 - 前200只股票
    TOP400 - 前400只股票
    TOP500 - 前500只股票
    TOP600 - 前600只股票
    TOP800 - 前800只股票
    TOP1000 - 前1000只股票
    TOP1200 - 前1200只股票
    TOP1600 - 前1600只股票
    TOP2000 - 前2000只股票
    TOP2000U - 前2000只中国股票
    TOP2500 - 前2500只股票
    TOP3000 - 前3000只股票
    TOPSP500 - 前500只股票(S&P500)
    """

    DEFAULT = "DEFAULT"
    ILLIQUID_MINVOL1M = "ILLQUID_MINVOL1M"
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


class Delay(Enum):
    """
    延迟枚举，表示Alpha因子的延迟天数

    DEFAULT - 默认值，无实际意义
    ZERO - 0天延迟
    ONE - 1天延迟
    """

    DEFAULT = -1
    ZERO = 0
    ONE = 1


class LookbackDays(Enum):
    """
    回溯天数枚举

    DEFAULT - 默认值，无实际意义
    DAYS_25 - 25天
    DAYS_50 - 50天
    DAYS_128 - 128天
    DAYS_256 - 256天
    DAYS_384 - 384天
    DAYS_512 - 512天
    """

    DEFAULT = 0
    DAYS_25 = 25
    DAYS_50 = 50
    DAYS_128 = 128
    DAYS_256 = 256
    DAYS_384 = 384
    DAYS_512 = 512


class Pasteurization(Enum):
    """
    巴氏灭菌处理枚举

    DEFAULT - 默认值，无实际意义
    ON - 开启
    OFF - 关闭
    """

    DEFAULT = "DEFAULT"
    ON = "ON"
    OFF = "OFF"


class UnitHandling(Enum):
    """
    单位处理枚举

    DEFAULT - 默认值，无实际意义
    VERIFY - 验证
    """

    DEFAULT = "DEFAULT"
    VERIFY = "VERIFY"


class NanHandling(Enum):
    """
    NaN处理枚举

    DEFAULT - 默认值，无实际意义
    ON - 开启
    OFF - 关闭
    """

    DEFAULT = "DEFAULT"
    ON = "ON"
    OFF = "OFF"


class SelectionHandling(Enum):
    """
    选择处理枚举

    DEFAULT - 默认值，无实际意义
    POSITIVE - 正值
    NON_ZERO - 非零
    NON_NAN - 非NaN
    """

    DEFAULT = "DEFAULT"
    POSITIVE = "POSITIVE"
    NON_ZERO = "NON_ZERO"
    NON_NAN = "NON_NAN"


class Category(Enum):
    """
    Alpha分类枚举

    DEFAULT - 默认值，无实际意义
    PRICE_REVERSION - 价格回归
    PRICE_MOMENTUM - 价格动量
    VOLUME - 成交量
    FUNDAMENTAL - 基本面
    ANALYST - 分析师
    PRICE_VOLUME - 价量关系
    RELATION - 关联关系
    SENTIMENT - 情绪
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
    """
    评级枚举

    DEFAULT - 默认值，无实际意义
    SPECTACULAR - 卓越
    EXCELLENT - 优秀
    GOOD - 良好
    AVERAGE - 一般
    INFERIOR - 需改进
    """

    DEFAULT = "DEFAULT"
    SPECTACULAR = "SPECTACULAR"
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    AVERAGE = "AVERAGE"
    INFERIOR = "INFERIOR"


class Stage(Enum):
    """
    阶段枚举

    DEFAULT - 默认值，无实际意义
    IS - 样本内
    OS - 样本外
    PROD - 生产环境
    """

    DEFAULT = "DEFAULT"
    IS = "IS"  # 样本内
    OS = "OS"  # 样本外
    PROD = "PROD"  # 生产环境


class Status(Enum):
    """
    状态枚举

    DEFAULT - 默认值，无实际意义
    UNSUBMITTED - 未提交
    ACTIVE - 活跃
    DECOMMISSIONED - 退役
    """

    DEFAULT = "DEFAULT"
    UNSUBMITTED = "UNSUBMITTED"
    ACTIVE = "ACTIVE"
    DECOMMISSIONED = "DECOMMISSIONED"


# 证券类型到支持地区的映射
INSTRUMENT_TYPE_REGION_MAP: Final[Dict[InstrumentType, List[Region]]] = {
    InstrumentType.EQUITY: [
        Region.USA,
        Region.GLOBAL,
        Region.EUROPE,
        Region.ASIA,
        Region.CHINA,
        Region.KOREA,
        Region.TAIWAN,
        Region.HONGKONG,
        Region.JAPAN,
        Region.AMERICA,
    ],
    InstrumentType.CRYPTO: [
        Region.GLOBAL,
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


# 辅助函数
def get_regions_for_instrument_type(instrument_type: InstrumentType) -> List[Region]:
    """获取指定证券类型支持的所有地区"""
    return INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


def get_instrument_types_for_region(region: Region) -> List[InstrumentType]:
    """获取支持指定地区的所有证券类型"""
    return REGION_INSTRUMENT_TYPE_MAP.get(region, [])


def is_region_supported_for_instrument_type(
    region: Region, instrument_type: InstrumentType
) -> bool:
    """检查指定地区是否支持指定证券类型"""
    return region in INSTRUMENT_TYPE_REGION_MAP.get(instrument_type, [])


EQUITY_REGION_UNIVERSE_MAP: Final[Dict[Region, List[Universe]]] = {
    Region.USA: [
        Universe.TOP3000,
        Universe.TOP1000,
        Universe.TOP500,
        Universe.TOP200,
        Universe.ILLIQUID_MINVOL1M,
        Universe.TOPSP500,
    ],
    Region.GLOBAL: [
        Universe.TOP3000,
        Universe.MINVOL1M,
    ],
    Region.EUROPE: [
        Universe.TOP2500,
        Universe.TOP1200,
        Universe.TOP800,
        Universe.TOP400,
        Universe.ILLIQUID_MINVOL1M,
    ],
    Region.ASIA: [
        Universe.MINVOL1M,
        Universe.ILLIQUID_MINVOL1M,
    ],
    Region.CHINA: [
        Universe.TOP2000U,
    ],
    Region.KOREA: [
        Universe.TOP600,
    ],
    Region.TAIWAN: [
        Universe.TOP500,
        Universe.TOP100,
    ],
    Region.HONGKONG: [
        Universe.TOP800,
        Universe.TOP500,
    ],
    Region.JAPAN: [
        Universe.TOP1600,
        Universe.TOP1200,
    ],
    Region.AMERICA: [
        Universe.TOP600,
    ],
}

# 加密货币的选股范围映射
CRYPTO_REGION_UNIVERSE_MAP: Final[Dict[Region, List[Universe]]] = {
    Region.GLOBAL: [
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
    Region.GLOBAL: [Delay.ONE],
    Region.EUROPE: [Delay.ZERO, Delay.ONE],
    Region.ASIA: [Delay.ONE],
    Region.CHINA: [Delay.ZERO, Delay.ONE],
    Region.KOREA: [Delay.ONE],
    Region.TAIWAN: [Delay.ONE],
    Region.HONGKONG: [Delay.ONE],
    Region.JAPAN: [Delay.ONE],
    Region.AMERICA: [Delay.ZERO, Delay.ONE],
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
    Region.GLOBAL: NEUTRALIZATION_EXTENDED + [Neutralization.COUNTRY],
    Region.EUROPE: NEUTRALIZATION_EXTENDED + [Neutralization.COUNTRY],
    Region.ASIA: NEUTRALIZATION_EXTENDED + [Neutralization.COUNTRY],
    Region.AMERICA: NEUTRALIZATION_EXTENDED + [Neutralization.COUNTRY],
    Region.USA: NEUTRALIZATION_EXTENDED,
    Region.CHINA: NEUTRALIZATION_BASIC,
    Region.KOREA: NEUTRALIZATION_BASIC,
    Region.TAIWAN: NEUTRALIZATION_BASIC,
    Region.HONGKONG: NEUTRALIZATION_BASIC,
    Region.JAPAN: NEUTRALIZATION_BASIC,
}


# 辅助函数，获取指定证券类型和地区支持的中性化策略
def get_neutralization_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Neutralization]:
    """获取指定证券类型和地区支持的所有中性化策略"""
    if instrument_type == InstrumentType.CRYPTO:
        return INSTRUMENT_TYPE_NEUTRALIZATION_MAP[InstrumentType.CRYPTO]

    return REGION_NEUTRALIZATION_MAP.get(region, NEUTRALIZATION_BASIC)


# 辅助函数，获取指定证券类型和地区支持的Universe
def get_universe_for_instrument_region(
    instrument_type: InstrumentType, region: Region
) -> List[Universe]:
    """获取指定证券类型和地区支持的所有Universe"""
    return INSTRUMENT_TYPE_UNIVERSE_MAP.get(instrument_type, {}).get(region, [])


# 辅助函数，获取指定地区支持的延迟配置
def get_delay_for_region(region: Region) -> List[Delay]:
    """获取指定地区支持的所有延迟配置"""
    return REGION_DELAY_MAP.get(region, [Delay.ONE])
