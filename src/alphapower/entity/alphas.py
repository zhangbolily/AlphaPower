"""
alphas.py

定义了与 Alpha 模型相关的数据库实体类、中间表和设置类。

模块功能：
- 提供 Alpha 策略的核心数据模型，包括分类、比赛、样本、设置等。
- 支持 Alpha 策略的多对多关系映射。
- 提供标签管理的便捷方法。

模块结构：
- Base: 所有 ORM 模型类的基础类。
- Classification: Alpha 分类表。
- Competition: Alpha 比赛表。
- SampleCheck: Alpha 样本检查表。
- Sample: Alpha 样本表。
- Setting: Alpha 设置表。
- Regular: Alpha 规则表。
- Alpha: Alpha 主表。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
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
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedColumn,
    mapped_column,
    relationship,
    validates,
)

from alphapower.constants import (
    ALPHA_ID_LENGTH,
    AlphaType,
    Color,
    CompetitionScoring,
    CompetitionStatus,
    Decay,
    Delay,
    Grade,
    InstrumentType,
    Neutralization,
    Region,
    RegularLanguage,
    Stage,
    Status,
    Switch,
    UnitHandling,
    Universe,
)

# pylint: disable=E1136


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有 ORM 模型类都继承自该类。

    该类结合了 AsyncAttrs 和 DeclarativeBase，提供异步 ORM 支持和
    SQLAlchemy 声明式映射的基本功能。
    """


class Classification(Base):
    """Alpha 分类表，存储 Alpha 的分类信息。

    Attributes:
        id (int): 主键ID。
        classification_id (str): 唯一的分类标识符。
        name (str): 分类名称。
    """

    __tablename__ = "classifications"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    classification_id: MappedColumn[str] = mapped_column(
        String, nullable=False, unique=True
    )
    name: MappedColumn[str] = mapped_column(String)


class Competition(Base):
    """Alpha 比赛表，存储 Alpha 的比赛信息。

    Attributes:
        id (int): 主键ID。
        competition_id (str): 唯一的比赛标识符。
        name (str): 比赛名称。
    """

    __tablename__ = "competitions"

    def __init__(
        self,
        universities: Optional[List[str]] = None,
        countries: Optional[List[str]] = None,
        excluded_countries: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """初始化比赛对象

        Args:
            universities: 大学列表
            countries: 国家列表
        """
        super().__init__(**kwargs)
        self.universities = universities  # type: ignore[method-assign]
        self.countries = countries  # type: ignore[method-assign]
        self.excluded_countries = excluded_countries  # type: ignore[method-assign]

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    competition_id: MappedColumn[str] = mapped_column(
        String, nullable=False, unique=True
    )
    name: MappedColumn[str] = mapped_column(String)
    description: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    _universities: MappedColumn[Optional[str]] = mapped_column(
        String, nullable=True, name="universities"
    )
    _countries: MappedColumn[Optional[str]] = mapped_column(
        String, nullable=True, name="countries"
    )
    _excluded_countries: MappedColumn[Optional[str]] = mapped_column(
        String, nullable=True, name="excluded_countries"
    )
    status: MappedColumn[CompetitionStatus] = mapped_column(
        Enum(CompetitionStatus), nullable=False
    )
    team_based: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    start_date: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    end_date: MappedColumn[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    sign_up_start_date: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    sign_up_end_date: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    sign_up_date: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    team: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    scoring: MappedColumn[CompetitionScoring] = mapped_column(
        Enum(CompetitionScoring), nullable=False
    )
    leaderboard: MappedColumn[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    prize_board: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    university_board: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    submissions: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    faq: MappedColumn[str] = mapped_column(String, nullable=True)
    progress: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)

    @hybrid_property
    def universities(self) -> List[str]:
        """获取大学列表

        Returns:
            List[str]: 大学字符串列表
        """
        if self._universities is None:
            return []
        return [university.strip() for university in self._universities.split(",")]

    @universities.setter  # type: ignore[no-redef]
    def universities(self, value: Optional[List[str]]) -> None:
        """设置大学列表

        Args:
            value: 大学字符串列表
        """
        if value is None:
            self._universities = None
        else:
            self._universities = ",".join(
                filter(
                    None,
                    [
                        (
                            university.strip()
                            if isinstance(university, str)
                            else str(university)
                        )
                        for university in value
                    ],
                )
            )

    @hybrid_property
    def countries(self) -> List[str]:
        """获取国家列表

        Returns:
            List[str]: 国家字符串列表
        """
        if self._countries is None:
            return []
        return [country.strip() for country in self._countries.split(",")]

    @countries.setter  # type: ignore[no-redef]
    def countries(self, value: Optional[List[str]]) -> None:
        """设置国家列表

        Args:
            value: 国家字符串列表
        """
        if value is None:
            self._countries = None
        else:
            self._countries = ",".join(
                filter(
                    None,
                    [
                        country.strip() if isinstance(country, str) else str(country)
                        for country in value
                    ],
                )
            )

    @hybrid_property
    def excluded_countries(self) -> List[str]:
        """获取排除的国家列表

        Returns:
            List[str]: 排除的国家字符串列表
        """
        if self._excluded_countries is None:
            return []
        return [country.strip() for country in self._excluded_countries.split(",")]

    @excluded_countries.setter  # type: ignore[no-redef]
    def excluded_countries(self, value: Optional[List[str]]) -> None:
        """设置排除的国家列表

        Args:
            value: 排除的国家字符串列表
        """
        if value is None:
            self._excluded_countries = None
        else:
            self._excluded_countries = ",".join(
                filter(
                    None,
                    [
                        country.strip() if isinstance(country, str) else str(country)
                        for country in value
                    ],
                )
            )


class Check(Base):
    """Alpha 样本检查表，存储样本检查的结果和相关信息。

    Attributes:
        id (int): 主键ID。
        sample_id (int): 外键连接到 Sample 表。
        name (str): 检查名称。
        result (str): 检查结果。
        message (Optional[str]): 消息。
        limit (Optional[float]): 限制。
        value (Optional[float]): 值。
        date (Optional[datetime]): 日期。
        competitions (Optional[str]): 比赛。
    """

    __tablename__ = "checks"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sample_id: MappedColumn[int] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=False
    )
    name: MappedColumn[str] = mapped_column(String)
    result: MappedColumn[str] = mapped_column(String)
    message: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    limit: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    value: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    date: MappedColumn[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    competitions: MappedColumn[Optional[str]] = mapped_column(JSON, nullable=True)


class Sample(Base):
    """Alpha 样本表，存储样本的各种统计信息。

    Attributes:
        id (int): 主键ID。
        long_count (Optional[int]): 多头数量。
        short_count (Optional[int]): 空头数量。
        pnl (Optional[float]): 盈亏。
        book_size (Optional[float]): 账簿大小。
        turnover (Optional[float]): 换手率。
        returns (Optional[float]): 收益。
        drawdown (Optional[float]): 回撤。
        margin (Optional[float]): 保证金。
        sharpe (Optional[float]): 夏普比率。
        fitness (Optional[float]): 适应度。
        self_correration (Optional[float]): 自相关。
        prod_correration (Optional[float]): 生产相关。
        os_is_sharpe_ratio (Optional[float]): OS-IS 夏普比率。
        pre_close_sharpe_ratio (Optional[float]): 收盘前夏普比率。
        start_date (datetime): 开始日期。
        checks (List[Check]): 样本检查的关系字段。
    """

    __tablename__ = "samples"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    long_count: MappedColumn[Optional[int]] = mapped_column(Integer, nullable=True)
    short_count: MappedColumn[Optional[int]] = mapped_column(Integer, nullable=True)
    pnl: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    book_size: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    turnover: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    returns: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    drawdown: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    margin: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    sharpe: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    fitness: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)
    self_correration: MappedColumn[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    prod_correration: MappedColumn[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    os_is_sharpe_ratio: MappedColumn[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    pre_close_sharpe_ratio: MappedColumn[Optional[float]] = mapped_column(
        Float, nullable=True
    )
    start_date: MappedColumn[datetime] = mapped_column(DateTime)
    checks: Mapped[List[Check]] = relationship(
        "Check",
        backref="sample",  # 定义一对多关系，样本检查属于某个样本
        cascade="all, delete-orphan",
    )


class Setting(Base):
    """Alpha 设置表，存储 Alpha 的各种配置参数。

    该类定义了 Alpha 策略的各种配置选项，包括数据处理、中和方法和
    其他影响 Alpha 性能的参数。

    Attributes:
        id (int): 主键ID。
        instrument_type (InstrumentType): 使用的金融工具类型。
        region (Region): Alpha 应用的市场区域。
        universe (Universe): Alpha 选用的股票范围。
        delay (Delay): 信号延迟时间（单位：天）。
        decay (int): 信号衰减参数。
        neutralization (Neutralization): 中性化方法，用于控制风险暴露。
        truncation (float): 截断阈值，控制异常值影响。
        pasteurization (Switch): 巴氏化处理方法。
        unit_handling (UnitHandling): 单位处理方式。
        nan_handling (Switch): NaN 值处理方式。
        language (RegularLanguage): 编程语言。
        visualization (bool): 是否启用可视化。
        test_period (Optional[str]): 测试周期。
        max_trade (Optional[Switch]): 最大交易量。
    """

    __tablename__ = "settings"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    language: MappedColumn[RegularLanguage] = mapped_column(
        Enum(RegularLanguage), nullable=False, default=RegularLanguage.DEFAULT
    )
    test_period: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    decay: MappedColumn[int] = mapped_column(Integer, nullable=False)
    truncation: MappedColumn[float] = mapped_column(Float, nullable=False)
    visualization: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    instrument_type: MappedColumn[InstrumentType] = mapped_column(
        Enum(InstrumentType), nullable=False, default=InstrumentType.DEFAULT
    )
    region: MappedColumn[Region] = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )
    universe: MappedColumn[Universe] = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )
    delay: MappedColumn[Delay] = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )
    neutralization: MappedColumn[Neutralization] = mapped_column(
        Enum(Neutralization), nullable=False, default=Neutralization.DEFAULT
    )
    pasteurization: MappedColumn[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )
    unit_handling: MappedColumn[UnitHandling] = mapped_column(
        Enum(UnitHandling), nullable=False, default=UnitHandling.DEFAULT
    )
    nan_handling: MappedColumn[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )
    max_trade: MappedColumn[Optional[Switch]] = mapped_column(
        Enum(Switch), nullable=True, default=Switch.DEFAULT
    )

    @validates("decay")
    def validate_decay(self, key: str, value: int) -> int:
        """验证 decay 字段的值是否在有效范围内

        Args:
            key: 字段名称
            value: 要验证的值

        Returns:
            int: 验证通过的值

        Raises:
            ValueError: 当值不在有效范围内时抛出
        """
        if value and not (Decay.MIN.value <= value <= Decay.MAX.value):
            raise ValueError(
                f"{key} 必须在 {Decay.MIN.value} 到 {Decay.MAX.value} 之间"
            )
        return value


class Regular(Base):
    """Alpha 规则表，存储 Alpha 的规则信息。

    该类定义了 Alpha 的规则相关信息，包括规则代码、描述和操作符统计。

    Attributes:
        id (int): 主键ID。
        code (str): 规则代码。
        description (Optional[str]): 规则描述。
        operator_count (int): 操作符数量。
    """

    __tablename__ = "regulars"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: MappedColumn[str] = mapped_column(String)
    description: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    operator_count: MappedColumn[int] = mapped_column(Integer)


class Alpha(Base):
    """Alpha 主表，存储 Alpha 的基本信息及其关联关系。

    该类是系统的核心实体，定义了 Alpha 策略的所有关键属性和关联关系。

    Attributes:
        id (int): 主键ID。
        alpha_id (str): 唯一的 Alpha 标识符。
        type (AlphaType): Alpha 类型。
        author (str): Alpha 创建者。
        settings_id (int): Alpha 设置的外键ID。
        regular_id (int): Alpha 规则的外键ID。
        date_created (datetime): Alpha 创建日期。
        date_submitted (Optional[datetime]): Alpha 提交日期。
        date_modified (Optional[datetime]): Alpha 修改日期。
        name (Optional[str]): Alpha 名称。
        favorite (bool): 是否收藏。
        hidden (bool): 是否隐藏。
        color (Color): Alpha 颜色。
        category (Optional[str]): Alpha 类别。
        tags (List[str]): Alpha 标签。
        classifications (List[Classification]): Alpha 的分类信息。
        grade (Grade): Alpha 等级。
        stage (Stage): Alpha 阶段。
        status (Status): Alpha 状态。
        in_sample_id (Optional[int]): 样本内的外键ID。
        out_sample_id (Optional[int]): 样本外的外键ID。
        train_id (Optional[int]): 训练样本的外键ID。
        test_id (Optional[int]): 测试样本的外键ID。
        prod_id (Optional[int]): 生产样本的外键ID。
        competitions (List[Competition]): Alpha 的比赛信息。
        themes (Optional[str]): Alpha 主题。
        pyramids (Optional[str]): Alpha 金字塔。
        team (Optional[str]): Alpha 团队。
    """

    __tablename__ = "alphas"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH), nullable=False, unique=True
    )
    author: MappedColumn[str] = mapped_column(String, nullable=False)
    name: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    category: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    _tags: MappedColumn[Optional[str]] = mapped_column(String, name="tags")
    themes: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    pyramids: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    team: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    favorite: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    hidden: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    type: MappedColumn[AlphaType] = mapped_column(
        Enum(AlphaType), nullable=False, default=AlphaType.DEFAULT
    )
    color: MappedColumn[Color] = mapped_column(
        Enum(Color), nullable=False, default=Color.NONE
    )
    grade: MappedColumn[Grade] = mapped_column(
        Enum(Grade), nullable=False, default=Grade.DEFAULT
    )
    stage: MappedColumn[Stage] = mapped_column(
        Enum(Stage), nullable=False, default=Stage.DEFAULT
    )
    status: MappedColumn[Status] = mapped_column(
        Enum(Status), nullable=False, default=Status.DEFAULT
    )
    date_created: MappedColumn[datetime] = mapped_column(DateTime, nullable=False)
    date_submitted: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    date_modified: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    settings_id: MappedColumn[int] = mapped_column(
        Integer, ForeignKey("settings.id"), nullable=False
    )
    settings: Mapped[Setting] = relationship("Setting", backref="alphas")
    regular_id: MappedColumn[int] = mapped_column(
        Integer, ForeignKey("regulars.id"), nullable=False
    )
    regular: Mapped[Regular] = relationship("Regular", backref="alphas")
    in_sample_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )
    in_sample: Mapped[Sample] = relationship(
        "Sample", foreign_keys=[in_sample_id], uselist=False, backref="alphas_in_sample"
    )
    out_sample_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )
    out_sample: Mapped[Sample] = relationship(
        "Sample",
        foreign_keys=[out_sample_id],
        uselist=False,
        backref="alphas_out_sample",
    )
    train_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )
    train: Mapped[Sample] = relationship(
        "Sample", foreign_keys=[train_id], uselist=False, backref="alphas_train"
    )
    test_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )
    test: Mapped[Sample] = relationship(
        "Sample", foreign_keys=[test_id], uselist=False, backref="alphas_test"
    )
    prod_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("samples.id"), nullable=True
    )
    prod: Mapped[Sample] = relationship(
        "Sample", foreign_keys=[prod_id], uselist=False, backref="alphas_prod"
    )
    classifications: Mapped[List[Classification]] = relationship(
        "Classification", secondary="alpha_classification", backref="alphas", cascade=""
    )
    competitions: Mapped[List[Competition]] = relationship(
        "Competition", secondary="alpha_competition", backref="alphas", cascade=""
    )

    def __init__(self, **kwargs: Any) -> None:
        """初始化模拟任务对象，处理特殊属性。

        Args:
            **kwargs: 包含所有模型属性的关键字参数。
        """
        tags = kwargs.pop("tags", None)
        super().__init__(**kwargs)
        if tags is not None:
            self._tags = ",".join(
                filter(
                    None,
                    [tag.strip() if isinstance(tag, str) else str(tag) for tag in tags],
                )
            )

    @hybrid_property
    def tags(self) -> List[str]:
        """获取标签列表

        Returns:
            List[str]: 标签字符串列表
        """
        if self._tags is None:
            return []
        return [tag.strip() for tag in self._tags.split(",") if tag.strip()]

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: Optional[List[str]]) -> None:
        """设置标签列表

        Args:
            value: 标签字符串列表
        """
        if value is None:
            self._tags = None
        else:
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
        """添加单个标签到标签列表

        Args:
            tag: 要添加的标签
        """
        if not tag or not tag.strip():
            return

        current_tags = self.tags or []
        if tag.strip() not in current_tags:
            current_tags.append(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]

    def remove_tag(self, tag: str) -> None:
        """从标签列表中移除单个标签

        Args:
            tag: 要移除的标签
        """
        if not tag or not tag.strip() or not self.tags:
            return

        current_tags = self.tags
        if tag.strip() in current_tags:
            current_tags.remove(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]


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
