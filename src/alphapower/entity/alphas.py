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
from alphapower.view.alpha import SubmissionCheckView, SubmissionCheckViewListAdapter

# pylint: disable=E1136


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Classification(Base):

    __tablename__ = "classifications"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    classification_id: MappedColumn[str] = mapped_column(
        String, nullable=False, unique=True
    )
    name: MappedColumn[str] = mapped_column(String)


class Competition(Base):

    __tablename__ = "competitions"

    def __init__(
        self,
        universities: Optional[List[str]] = None,
        countries: Optional[List[str]] = None,
        excluded_countries: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:

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

        if self._universities is None:
            return []
        return [university.strip() for university in self._universities.split(",")]

    @universities.setter  # type: ignore[no-redef]
    def universities(self, value: Optional[List[str]]) -> None:

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

        if self._countries is None:
            return []
        return [country.strip() for country in self._countries.split(",")]

    @countries.setter  # type: ignore[no-redef]
    def countries(self, value: Optional[List[str]]) -> None:

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

        if self._excluded_countries is None:
            return []
        return [country.strip() for country in self._excluded_countries.split(",")]

    @excluded_countries.setter  # type: ignore[no-redef]
    def excluded_countries(self, value: Optional[List[str]]) -> None:

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


class AggregateData(Base):

    __tablename__ = "aggregate_data"

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
    _checks: MappedColumn[JSON] = mapped_column(
        JSON, nullable=True, name="checks"
    )  # 用于存储检查结果的 JSON 字段

    def __init__(self, **kwargs: Any) -> None:
        checks: Optional[List[SubmissionCheckView]] = kwargs.pop("checks", None)
        super().__init__(**kwargs)

        if checks:
            self._checks = SubmissionCheckViewListAdapter.dump_python(
                checks,
                mode="json",
            )
        self._checks_view_cache: Optional[List[SubmissionCheckView]] = checks

    @hybrid_property
    def checks(self) -> List[SubmissionCheckView]:

        if self._checks is None:
            return []

        if self._checks_view_cache is not None:
            return self._checks_view_cache
        checks: List[SubmissionCheckView] = (
            SubmissionCheckViewListAdapter.validate_python(self._checks)
        )
        if checks is None:
            return []

        # 缓存检查结果
        self._checks_view_cache = checks
        return checks

    @checks.setter  # type: ignore[no-redef]
    def checks(self, value: Optional[List[SubmissionCheckView]]) -> None:

        if value is None:
            self._checks = None
            self._checks_view_cache = None
        else:
            self._checks = SubmissionCheckViewListAdapter.dump_python(value)
            self._checks_view_cache = value


class Setting(Base):

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

        if value and not (Decay.MIN.value <= value <= Decay.MAX.value):
            raise ValueError(
                f"{key} 必须在 {Decay.MIN.value} 到 {Decay.MAX.value} 之间"
            )
        return value


class Expression(Base):

    __tablename__ = "expressions"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: MappedColumn[str] = mapped_column(String)
    description: MappedColumn[Optional[str]] = mapped_column(String, nullable=True)
    operator_count: MappedColumn[Optional[int]] = mapped_column(Integer, nullable=True)


class Alpha(Base):

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
    pyramids: MappedColumn[Optional[JSON]] = mapped_column(JSON, nullable=True)
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
    settings: Mapped[Setting] = relationship(
        "Setting", backref="alphas", lazy="joined", cascade="all"
    )
    regular_id: MappedColumn[int] = mapped_column(
        Integer,
        ForeignKey("expressions.id"),
        nullable=True,
    )
    regular: Mapped[Expression] = relationship(
        "Expression",
        foreign_keys=[regular_id],
        uselist=False,
        backref="alphas_regular",
        lazy="joined",
        cascade="all",
    )
    combo_id: MappedColumn[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("expressions.id"),
        nullable=True,
    )
    combo: Mapped[Expression] = relationship(
        "Expression",
        foreign_keys=[combo_id],
        uselist=False,
        backref="alphas_combo",
        lazy="joined",
        cascade="all",
    )
    selection_id: MappedColumn[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("expressions.id"),
        nullable=True,
    )
    selection: Mapped[Expression] = relationship(
        "Expression",
        foreign_keys=[selection_id],
        uselist=False,
        backref="alphas_selection",
        lazy="joined",
        cascade="all",
    )
    in_sample_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("aggregate_data.id"), nullable=True
    )
    in_sample: Mapped[AggregateData] = relationship(
        "AggregateData",
        foreign_keys=[in_sample_id],
        uselist=False,
        backref="alphas_in_sample",
        lazy="joined",
        cascade="all",
    )
    out_sample_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("aggregate_data.id"), nullable=True
    )
    out_sample: Mapped[AggregateData] = relationship(
        "AggregateData",
        foreign_keys=[out_sample_id],
        uselist=False,
        backref="alphas_out_sample",
        lazy="joined",
        cascade="all",
    )
    train_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("aggregate_data.id"), nullable=True
    )
    train: Mapped[AggregateData] = relationship(
        "AggregateData",
        foreign_keys=[train_id],
        uselist=False,
        backref="alphas_train",
        lazy="joined",
        cascade="all",
    )
    test_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("aggregate_data.id"), nullable=True
    )
    test: Mapped[AggregateData] = relationship(
        "AggregateData",
        foreign_keys=[test_id],
        uselist=False,
        backref="alphas_test",
        lazy="joined",
        cascade="all",
    )
    prod_id: MappedColumn[Optional[int]] = mapped_column(
        Integer, ForeignKey("aggregate_data.id"), nullable=True
    )
    prod: Mapped[AggregateData] = relationship(
        "AggregateData",
        foreign_keys=[prod_id],
        uselist=False,
        backref="alphas_prod",
        lazy="joined",
        cascade="all",
    )
    classifications: Mapped[List[Classification]] = relationship(
        "Classification",
        secondary="alpha_classification",
        backref="alphas",
        cascade="",
        lazy="selectin",
    )
    competitions: Mapped[List[Competition]] = relationship(
        "Competition",
        secondary="alpha_competition",
        backref="alphas",
        cascade="",
        lazy="selectin",
    )

    def __init__(self, **kwargs: Any) -> None:

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

        if self._tags is None:
            return []
        return [tag.strip() for tag in self._tags.split(",") if tag.strip()]

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: Optional[List[str]]) -> None:

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

        if not tag or not tag.strip():
            return

        current_tags = self.tags or []
        if tag.strip() not in current_tags:
            current_tags.append(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]

    def remove_tag(self, tag: str) -> None:

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
