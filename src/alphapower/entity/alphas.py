"""
alphas.py

定义了与 Alpha 模型相关的数据库实体类、中间表和设置类。
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship

from alphapower.constants import Region


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。

    该类结合了 AsyncAttrs 和 DeclarativeBase，提供异步 ORM 支持和
    SQLAlchemy 声明式映射的基本功能。
    """


# 中间表，用于表示 Alphas 和 Classifications 之间的多对多关系
alphas_classifications = Table(
    "alpha_classification",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "classification_id",
        Integer,
        ForeignKey("classifications.id"),
        primary_key=True,
    ),
)

# 中间表，用于表示 Alphas 和 Competitions 之间的多对多关系
alphas_competitions = Table(
    "alpha_competition",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "competition_id",
        Integer,
        ForeignKey("competitions.id"),
        primary_key=True,
    ),
)


class Setting(Base):
    """Alpha 设置表，存储 Alpha 的各种配置参数。

    该类定义了 Alpha 策略的各种配置选项，包括数据处理、中和方法和
    其他影响 Alpha 性能的参数。

    Attributes:
        id: 主键ID。
        instrument_type: 使用的金融工具类型。
        region: Alpha 应用的市场区域。
        universe: Alpha 选用的股票范围。
        delay: 信号延迟时间（单位：天）。
        decay: 信号衰减参数。
        neutralization: 中性化方法，用于控制风险暴露。
        truncation: 截断阈值，控制异常值影响。
        pasteurization: 巴氏化处理方法。
        unit_handling: 单位处理方式。
        nan_handling: NaN 值处理方式。
        language: 编程语言。
        visualization: 是否启用可视化。
        test_period: 测试周期。
        max_trade: 最大交易量。
    """

    __tablename__ = "settings"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_type = mapped_column(String)  # 使用的金融工具类型
    region = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )  # Alpha 应用的市场区域
    universe = mapped_column(String)  # Alpha 选用的股票范围
    delay = mapped_column(Integer)  # 信号延迟时间（单位：天）
    decay = mapped_column(Integer)  # 信号衰减参数
    neutralization = mapped_column(String)  # 中性化方法，用于控制风险暴露
    truncation = mapped_column(Float)  # 截断阈值，控制异常值影响
    pasteurization = mapped_column(String)  # 巴氏化处理方法
    unit_handling = mapped_column(String)  # 单位处理方式
    nan_handling = mapped_column(String)  # NaN 值处理方式
    language = mapped_column(String)  # 编程语言
    visualization = mapped_column(Boolean)  # 是否启用可视化
    test_period = mapped_column(String, nullable=True)  # 测试周期
    max_trade = mapped_column(Float, nullable=True)  # 最大交易量


class Regular(Base):
    """Alpha 规则表，存储 Alpha 的规则信息。

    该类定义了 Alpha 的规则相关信息，包括规则代码、描述和操作符统计。

    Attributes:
        id: 主键ID。
        code: 规则代码。
        description: 规则描述。
        operator_count: 规则中的操作符数量。
    """

    __tablename__ = "regulars"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    code = mapped_column(String)  # 规则代码
    description = mapped_column(String, nullable=True)  # 描述
    operator_count = mapped_column(Integer)  # 操作符数量


class Classification(Base):
    """Alpha 分类表，存储 Alpha 的分类信息。

    该类定义了 Alpha 的分类系统，用于对 Alpha 进行分类管理。

    Attributes:
        id: 主键ID。
        classification_id: 唯一的分类标识符。
        name: 分类名称。
    """

    __tablename__ = "classifications"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    classification_id = mapped_column(String, nullable=False, unique=True)  # 分类 ID
    name = mapped_column(String)  # 分类名称


class Competition(Base):
    """Alpha 比赛表，存储 Alpha 的比赛信息。"""

    __tablename__ = "competitions"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    competition_id = mapped_column(String, nullable=False, unique=True)  # 比赛 ID
    name = mapped_column(String)  # 比赛名称


class SampleCheck(Base):
    """Alpha 样本检查表，存储样本检查的结果和相关信息。"""

    __tablename__ = "sample_checks"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    name = mapped_column(String)  # 检查名称
    result = mapped_column(String)  # 检查结果
    limit = mapped_column(Float, nullable=True)  # 限制
    value = mapped_column(Float, nullable=True)  # 值
    date = mapped_column(DateTime, nullable=True)  # 日期
    competitions = mapped_column(String, nullable=True)  # 比赛
    message = mapped_column(String, nullable=True)  # 消息


class Sample(Base):
    """Alpha 样本表，存储样本的各种统计信息。"""

    __tablename__ = "samples"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    pnl = mapped_column(Float, nullable=True)  # 盈亏
    book_size = mapped_column(Float, nullable=True)  # 账簿大小
    long_count = mapped_column(Integer, nullable=True)  # 多头数量
    short_count = mapped_column(Integer, nullable=True)  # 空头数量
    turnover = mapped_column(Float, nullable=True)  # 换手率
    returns = mapped_column(Float, nullable=True)  # 收益
    drawdown = mapped_column(Float, nullable=True)  # 回撤
    margin = mapped_column(Float, nullable=True)  # 保证金
    sharpe = mapped_column(Float, nullable=True)  # 夏普比率
    fitness = mapped_column(Float, nullable=True)  # 适应度
    start_date = mapped_column(DateTime)  # 开始日期
    checks_id = mapped_column(
        Integer, ForeignKey("sample_checks.id")
    )  # 添加外键连接到 AlphaSampleCheck 表
    checks = relationship(
        "SampleCheck", backref="alpha_samples"
    )  # 定义 checks 字段的关系
    self_correration = mapped_column(Float, nullable=True)  # 自相关
    prod_correration = mapped_column(Float, nullable=True)  # 生产相关
    os_is_sharpe_ratio = mapped_column(Float, nullable=True)  # OS-IS 夏普比率
    pre_close_sharpe_ratio = mapped_column(Float, nullable=True)  # 收盘前夏普比率


class Alpha(Base):
    """Alpha 主表，存储 Alpha 的基本信息及其关联关系。

    该类是系统的核心实体，定义了 Alpha 策略的所有关键属性和关联关系。

    Attributes:
        id: 主键ID。
        alpha_id: 唯一的 Alpha 标识符。
        type: Alpha 类型。
        author: Alpha 创建者。
        settings_id: Alpha 设置的外键ID。
        regular_id: Alpha 规则的外键ID。
        date_created: Alpha 创建日期。
        date_submitted: Alpha 提交日期。
        date_modified: Alpha 修改日期。
        name: Alpha 名称。
        favorite: 是否收藏。
        hidden: 是否隐藏。
        color: Alpha 颜色。
        category: Alpha 类别。
        tags: Alpha 标签。
        classifications: Alpha 的分类信息。
        grade: Alpha 等级。
        stage: Alpha 阶段。
        status: Alpha 状态。
        in_sample_id: 样本内的外键ID。
        out_sample_id: 样本外的外键ID。
        train_id: 训练样本的外键ID。
        test_id: 测试样本的外键ID。
        prod_id: 生产样本的外键ID。
        competitions: Alpha 的比赛信息。
        themes: Alpha 主题。
        pyramids: Alpha 金字塔。
        team: Alpha 团队。
    """

    __tablename__ = "alphas"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id = mapped_column(String, nullable=False, unique=True)  # Alpha ID
    type = mapped_column(String)  # 类型
    author = mapped_column(String)  # 作者
    settings_id = mapped_column(Integer, ForeignKey("settings.id"))  # 设置 ID
    settings = relationship(
        "Setting", backref="alphas"
    )  # 修正：AlphaSettings 而非 Alpha_Settings
    regular_id = mapped_column(Integer, ForeignKey("regulars.id"))  # 规则 ID
    regular = relationship(
        "Regular", backref="alphas"
    )  # 修正：AlphaRegular 而非 Alpha_Regular
    date_created = mapped_column(DateTime)  # 创建日期
    date_submitted = mapped_column(DateTime, nullable=True)  # 提交日期
    date_modified = mapped_column(DateTime, nullable=True)  # 修改日期
    name = mapped_column(String, nullable=True)  # 名称
    favorite = mapped_column(Boolean)  # 收藏
    hidden = mapped_column(Boolean)  # 隐藏
    color = mapped_column(String, nullable=True)  # 颜色
    category = mapped_column(String, nullable=True)  # 类别
    tags = mapped_column(String)  # 标签
    classifications = relationship(
        "Classification", secondary=alphas_classifications, backref="alphas"
    )  # 修正：Classification 而非 Alphas_Classifications
    grade = mapped_column(String)  # 等级
    stage = mapped_column(String)  # 阶段
    status = mapped_column(String)  # 状态
    in_sample_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 样本内 ID
    in_sample = relationship(
        "Sample", foreign_keys=[in_sample_id], backref="alphas_inSample"
    )  # 修正：AlphaSample 而非 Alpha_Samples
    out_sample_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 样本外 ID
    out_sample = relationship(
        "Sample", foreign_keys=[out_sample_id], backref="alphas_outSample"
    )  # 修正：AlphaSample 而非 Alpha_Samples
    train_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 训练 ID
    train = relationship(
        "Sample", foreign_keys=[train_id], backref="alphas_train"
    )  # 修正：AlphaSample 而非 Alpha_Samples
    test_id = mapped_column(Integer, ForeignKey("samples.id"), nullable=True)  # 测试 ID
    test = relationship(
        "Sample", foreign_keys=[test_id], backref="alphas_test"
    )  # 修正：AlphaSample 而非 Alpha_Samples
    prod_id = mapped_column(Integer, ForeignKey("samples.id"), nullable=True)  # 生产 ID
    prod = relationship(
        "Sample", foreign_keys=[prod_id], backref="alphas_prod"
    )  # 修正：AlphaSample 而非 Alpha_Samples
    competitions = relationship(
        "Competition", secondary=alphas_competitions, backref="alphas"
    )  # 修正：AlphaCompetition 而非 Alphas_Competitions
    themes = mapped_column(String, nullable=True)  # 主题
    pyramids = mapped_column(String, nullable=True)  # 金字塔
    team = mapped_column(String, nullable=True)  # 团队
