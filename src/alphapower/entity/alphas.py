"""
alphas.py
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。"""


# 中间表，用于表示 Alphas 和 Alphas_Classification 之间的多对多关系
alphas_classifications = Table(
    "alphas_classifications_ref",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "classification_id",
        Integer,
        ForeignKey("alpha_classifications.id"),
        primary_key=True,
    ),
)

# 中间表，用于表示 Alphas 和 Competitions 之间的多对多关系
alphas_competitions = Table(
    "alphas_competitions_ref",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "competition_id",
        Integer,
        ForeignKey("alpha_competitions.id"),
        primary_key=True,
    ),
)


class AlphaSettings(Base):
    """Alpha 设置表，存储 Alpha 的各种配置参数。"""

    __tablename__ = "alpha_settings"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_type = mapped_column(String)  # 仪器类型
    region = mapped_column(String)  # 区域
    universe = mapped_column(String)  # 宇宙
    delay = mapped_column(Integer)  # 延迟
    decay = mapped_column(Integer)  # 衰减
    neutralization = mapped_column(String)  # 中性化
    truncation = mapped_column(Float)  # 截断
    pasteurization = mapped_column(String)  # 巴氏化
    unit_handling = mapped_column(String)  # 单位处理
    nan_handling = mapped_column(String)  # NaN 处理
    language = mapped_column(String)  # 语言
    visualization = mapped_column(Boolean)  # 可视化
    test_period = mapped_column(String, nullable=True)  # 测试周期
    max_trade = mapped_column(Float, nullable=True)  # 最大交易量


class AlphaRegular(Base):
    """Alpha 规则表，存储 Alpha 的规则信息。"""

    __tablename__ = "alpha_regulars"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    code = mapped_column(String)  # 规则代码
    description = mapped_column(String, nullable=True)  # 描述
    operator_count = mapped_column(Integer)  # 操作符数量


class AlphaClassification(Base):
    """Alpha 分类表，存储 Alpha 的分类信息。"""

    __tablename__ = "alpha_classifications"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    classification_id = mapped_column(String, nullable=False, unique=True)  # 分类 ID
    name = mapped_column(String)  # 分类名称


class AlphaCompetition(Base):
    """Alpha 比赛表，存储 Alpha 的比赛信息。"""

    __tablename__ = "alpha_competitions"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    competition_id = mapped_column(String, nullable=False, unique=True)  # 比赛 ID
    name = mapped_column(String)  # 比赛名称


class AlphaSampleCheck(Base):
    """Alpha 样本检查表，存储样本检查的结果和相关信息。"""

    __tablename__ = "alpha_sample_checks"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    name = mapped_column(String)  # 检查名称
    result = mapped_column(String)  # 检查结果
    limit = mapped_column(Float, nullable=True)  # 限制
    value = mapped_column(Float, nullable=True)  # 值
    date = mapped_column(DateTime, nullable=True)  # 日期
    competitions = mapped_column(String, nullable=True)  # 比赛
    message = mapped_column(String, nullable=True)  # 消息


class AlphaSample(Base):
    """Alpha 样本表，存储样本的各种统计信息。"""

    __tablename__ = "alpha_samples"

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
        Integer, ForeignKey("alpha_sample_checks.id")
    )  # 添加外键连接到 AlphaSampleCheck 表
    checks = relationship(
        "Alpha_Sample_Checks", backref="alpha_samples"
    )  # 定义 checks 字段的关系
    self_correration = mapped_column(Float, nullable=True)  # 自相关
    prod_correration = mapped_column(Float, nullable=True)  # 生产相关
    os_is_sharpe_ratio = mapped_column(Float, nullable=True)  # OS-IS 夏普比率
    pre_close_sharpe_ratio = mapped_column(Float, nullable=True)  # 收盘前夏普比率


class Alpha(Base):
    """Alpha 主表，存储 Alpha 的基本信息及其关联关系。"""

    __tablename__ = "alphas"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id = mapped_column(String, nullable=False, unique=True)  # Alpha ID
    type = mapped_column(String)  # 类型
    author = mapped_column(String)  # 作者
    settings_id = mapped_column(Integer, ForeignKey("alpha_settings.id"))  # 设置 ID
    settings = relationship("Alpha_Settings", backref="alphas")  # 设置关系
    regular_id = mapped_column(Integer, ForeignKey("alpha_regulars.id"))  # 规则 ID
    regular = relationship("Alpha_Regular", backref="alphas")  # 规则关系
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
        "Alphas_Classifications", secondary=alphas_classifications, backref="alphas"
    )  # 使用多对多关系
    grade = mapped_column(String)  # 等级
    stage = mapped_column(String)  # 阶段
    status = mapped_column(String)  # 状态
    in_sample_id = mapped_column(
        Integer, ForeignKey("alpha_samples.id"), nullable=True
    )  # 样本内 ID
    in_sample = relationship(
        "Alpha_Samples", foreign_keys=[in_sample_id], backref="alphas_inSample"
    )  # 样本内关系
    out_sample_id = mapped_column(
        Integer, ForeignKey("alpha_samples.id"), nullable=True
    )  # 样本外 ID
    out_sample = relationship(
        "Alpha_Samples", foreign_keys=[out_sample_id], backref="alphas_outSample"
    )  # 样本外关系
    train_id = mapped_column(
        Integer, ForeignKey("alpha_samples.id"), nullable=True
    )  # 训练 ID
    train = relationship(
        "Alpha_Samples", foreign_keys=[train_id], backref="alphas_train"
    )  # 训练关系
    test_id = mapped_column(
        Integer, ForeignKey("alpha_samples.id"), nullable=True
    )  # 测试 ID
    test = relationship(
        "Alpha_Samples", foreign_keys=[test_id], backref="alphas_test"
    )  # 测试关系
    prod_id = mapped_column(
        Integer, ForeignKey("alpha_samples.id"), nullable=True
    )  # 生产 ID
    prod = relationship(
        "Alpha_Samples", foreign_keys=[prod_id], backref="alphas_prod"
    )  # 生产关系
    competitions = relationship(
        "Alphas_Competitions", secondary=alphas_competitions, backref="alphas"
    )  # 使用多对多关系
    themes = mapped_column(String, nullable=True)  # 主题
    pyramids = mapped_column(String, nullable=True)  # 金字塔
    team = mapped_column(String, nullable=True)  # 团队
