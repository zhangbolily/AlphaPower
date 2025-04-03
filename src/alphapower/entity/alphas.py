"""
alphas.py

定义了与 Alpha 模型相关的数据库实体类、中间表和设置类。
"""

from typing import Any

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
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship, validates

from alphapower.constants import (
    AlphaType,
    Color,
    Decay,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    Stage,
    Status,
    Switch,
    UnitHandling,
    Universe,
)


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。

    该类结合了 AsyncAttrs 和 DeclarativeBase，提供异步 ORM 支持和
    SQLAlchemy 声明式映射的基本功能。
    """


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
    message = mapped_column(String, nullable=True)  # 消息
    limit = mapped_column(Float, nullable=True)  # 限制
    value = mapped_column(Float, nullable=True)  # 值
    date = mapped_column(DateTime, nullable=True)  # 日期
    competitions = mapped_column(String, nullable=True)  # 比赛


class Sample(Base):
    """Alpha 样本表，存储样本的各种统计信息。"""

    __tablename__ = "samples"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 整型字段
    long_count = mapped_column(Integer, nullable=True)  # 多头数量
    short_count = mapped_column(Integer, nullable=True)  # 空头数量

    # 浮点数字段
    pnl = mapped_column(Float, nullable=True)  # 盈亏
    book_size = mapped_column(Float, nullable=True)  # 账簿大小
    turnover = mapped_column(Float, nullable=True)  # 换手率
    returns = mapped_column(Float, nullable=True)  # 收益
    drawdown = mapped_column(Float, nullable=True)  # 回撤
    margin = mapped_column(Float, nullable=True)  # 保证金
    sharpe = mapped_column(Float, nullable=True)  # 夏普比率
    fitness = mapped_column(Float, nullable=True)  # 适应度
    self_correration = mapped_column(Float, nullable=True)  # 自相关
    prod_correration = mapped_column(Float, nullable=True)  # 生产相关
    os_is_sharpe_ratio = mapped_column(Float, nullable=True)  # OS-IS 夏普比率
    pre_close_sharpe_ratio = mapped_column(Float, nullable=True)  # 收盘前夏普比率

    # 日期时间字段
    start_date = mapped_column(DateTime)  # 开始日期

    # 外键和关系
    checks_id = mapped_column(
        Integer, ForeignKey("sample_checks.id")
    )  # 外键连接到 SampleCheck 表
    checks = relationship(
        "SampleCheck", backref="alpha_samples"
    )  # 定义 checks 字段的关系


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

    # 字符串字段
    language = mapped_column(String)  # 编程语言
    test_period = mapped_column(String, nullable=True)  # 测试周期

    # 数值字段
    decay = mapped_column(Integer)  # 信号衰减参数
    truncation = mapped_column(Float)  # 截断阈值，控制异常值影响

    # 布尔字段
    visualization = mapped_column(Boolean)  # 是否启用可视化

    # 枚举字段
    instrument_type = mapped_column(
        Enum(InstrumentType), nullable=False, default=InstrumentType.DEFAULT
    )  # 使用的金融工具类型
    region = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )  # Alpha 应用的市场区域
    universe = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )  # Alpha 选用的股票范围
    delay = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )  # 信号延迟时间（单位：天）
    neutralization = mapped_column(
        Enum(Neutralization), nullable=False, default=Neutralization.DEFAULT
    )  # 中性化方法
    pasteurization = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )  # 巴氏化处理方法
    unit_handling = mapped_column(
        Enum(UnitHandling), nullable=False, default=UnitHandling.DEFAULT
    )  # 单位处理方式
    nan_handling = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )  # NaN 值处理方式
    max_trade = mapped_column(
        Enum(Switch), nullable=True, default=Switch.DEFAULT
    )  # 最大交易量

    # 验证方法
    @validates("decay")
    def validate_decay(self, key: str, value: int) -> int:
        """验证 decay 字段的值是否在有效范围内"""
        if value is not None and not (Decay.MIN.value <= value <= Decay.MAX.value):
            raise ValueError(f"{key} 必须在 {Decay.MIN} 到 {Decay.MAX} 之间")
        return value


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

    # 主键
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 字符串字段
    alpha_id = mapped_column(String, nullable=False, unique=True)  # Alpha ID
    author = mapped_column(String)  # 作者
    name = mapped_column(String, nullable=True)  # 名称
    category = mapped_column(String, nullable=True)  # 类别
    _tags = mapped_column(String, name="tags")  # 标签，存储为逗号分隔的字符串
    themes = mapped_column(String, nullable=True)  # 主题
    pyramids = mapped_column(String, nullable=True)  # 金字塔
    team = mapped_column(String, nullable=True)  # 团队

    # 布尔字段
    favorite = mapped_column(Boolean)  # 收藏
    hidden = mapped_column(Boolean)  # 隐藏

    # 枚举字段
    type = mapped_column(
        Enum(AlphaType), nullable=False, default=AlphaType.DEFAULT
    )  # Alpha 类型
    color = mapped_column(Enum(Color), nullable=False, default=Color.NONE)  # 颜色
    grade = mapped_column(Enum(Grade), nullable=False, default=Grade.DEFAULT)  # 等级
    stage = mapped_column(Enum(Stage), nullable=False, default=Stage.DEFAULT)  # 阶段
    status = mapped_column(Enum(Status), nullable=False, default=Status.DEFAULT)  # 状态

    # 日期时间字段
    date_created = mapped_column(DateTime)  # 创建日期
    date_submitted = mapped_column(DateTime, nullable=True)  # 提交日期
    date_modified = mapped_column(DateTime, nullable=True)  # 修改日期

    # 外键和关系
    settings_id = mapped_column(Integer, ForeignKey("settings.id"))  # 设置 ID
    settings = relationship("Setting", backref="alphas")
    regular_id = mapped_column(Integer, ForeignKey("regulars.id"))  # 规则 ID
    regular = relationship("Regular", backref="alphas")
    in_sample_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 样本内 ID
    in_sample = relationship(
        "Sample", foreign_keys=[in_sample_id], backref="alphas_inSample"
    )
    out_sample_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 样本外 ID
    out_sample = relationship(
        "Sample", foreign_keys=[out_sample_id], backref="alphas_outSample"
    )
    train_id = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )  # 训练 ID
    train = relationship("Sample", foreign_keys=[train_id], backref="alphas_train")
    test_id = mapped_column(Integer, ForeignKey("samples.id"), nullable=True)  # 测试 ID
    test = relationship("Sample", foreign_keys=[test_id], backref="alphas_test")
    prod_id = mapped_column(Integer, ForeignKey("samples.id"), nullable=True)  # 生产 ID
    prod = relationship("Sample", foreign_keys=[prod_id], backref="alphas_prod")
    classifications = relationship(
        "Classification", secondary="alpha_classification", backref="alphas"
    )
    competitions = relationship(
        "Competition", secondary="alpha_competition", backref="alphas"
    )

    def __init__(self, **kwargs: Any) -> None:
        """初始化模拟任务对象，处理特殊属性。

        Args:
            **kwargs: 包含所有模型属性的关键字参数。
        """
        # 处理 tags 属性 (如果存在)
        tags = kwargs.pop("tags", None)

        # 调用父类的 __init__ 处理其他属性
        super().__init__(**kwargs)

        # 手动设置 _tags 属性
        if tags is not None:
            # 过滤空标签，并使用逗号连接
            self._tags = ",".join(
                filter(
                    None,
                    [tag.strip() if isinstance(tag, str) else str(tag) for tag in tags],
                )
            )

    @hybrid_property
    def tags(self) -> list:
        """获取标签列表"""
        if self._tags is None:
            return []
        return [tag.strip() for tag in self._tags.split(",") if tag.strip()]

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: list) -> None:
        """设置标签列表"""
        if value is None:
            self._tags = None
        else:
            # 过滤空标签，并使用逗号连接
            self._tags = ",".join(
                filter(
                    None,
                    [
                        tag.strip() if isinstance(tag, str) else str(tag)
                        for tag in value
                    ],
                )
            )

    def add_tag(self, tag: str) -> None:
        """添加单个标签到标签列表"""
        if not tag or not tag.strip():
            return

        current_tags = self.tags or []
        if tag.strip() not in current_tags:
            current_tags.append(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]

    def remove_tag(self, tag: str) -> None:
        """从标签列表中移除单个标签"""
        if not tag or not tag.strip() or not self.tags:
            return

        current_tags = self.tags
        if tag.strip() in current_tags:
            current_tags.remove(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]


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
