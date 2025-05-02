import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    event,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedColumn,
    mapped_column,
    relationship,
)

from alphapower.constants import (
    ALPHA_ID_LENGTH,
    AlphaType,
    Color,
    CompetitionScoring,
    CompetitionStatus,
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
from alphapower.view.alpha import (
    ClassificationRefView,
    ClassificationRefViewListAdapter,
    CompetitionRefViewListAdapter,
    ExpressionView,
    IdNameRefView,
    PyramidRefView,
    PyramidRefViewListAdapter,
    StringListAdapter,
    SubmissionCheckView,
    SubmissionCheckViewListAdapter,
)

# pylint: disable=E1136


class Base(AsyncAttrs, DeclarativeBase):
    pass


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
        String(256), nullable=False, unique=True
    )
    name: MappedColumn[str] = mapped_column(String(256))
    description: MappedColumn[Optional[str]] = mapped_column(Text, nullable=True)
    _universities: MappedColumn[Optional[JSON]] = mapped_column(
        JSON, nullable=True, name="universities"
    )
    _countries: MappedColumn[Optional[JSON]] = mapped_column(
        JSON, nullable=True, name="countries"
    )
    _excluded_countries: MappedColumn[Optional[JSON]] = mapped_column(
        JSON, nullable=True, name="excluded_countries"
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
    team: MappedColumn[Optional[str]] = mapped_column(String(256), nullable=True)
    scoring: MappedColumn[CompetitionScoring] = mapped_column(
        Enum(CompetitionScoring), nullable=False
    )
    leaderboard: MappedColumn[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True
    )
    prize_board: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    university_board: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    submissions: MappedColumn[bool] = mapped_column(Boolean, nullable=False)
    faq: MappedColumn[str] = mapped_column(String(512), nullable=True)
    progress: MappedColumn[Optional[float]] = mapped_column(Float, nullable=True)

    @hybrid_property
    def universities(self) -> List[str]:

        if self._universities is None:
            return []
        universities: List[str] = StringListAdapter.validate_python(self._universities)
        return universities

    @universities.setter  # type: ignore[no-redef]
    def universities(self, value: Optional[List[str]]) -> None:

        if value is None:
            self._universities = None
        else:
            self._universities = StringListAdapter.dump_python(
                value,
                mode="json",
            )

    @hybrid_property
    def countries(self) -> List[str]:

        if self._countries is None:
            return []
        countries: List[str] = StringListAdapter.validate_python(self._countries)
        return countries

    @countries.setter  # type: ignore[no-redef]
    def countries(self, value: Optional[List[str]]) -> None:

        if value is None:
            self._countries = None
        else:
            self._countries = StringListAdapter.dump_python(
                value,
                mode="json",
            )

    @hybrid_property
    def excluded_countries(self) -> List[str]:

        if self._excluded_countries is None:
            return []
        excluded_countries: List[str] = StringListAdapter.validate_python(
            self._excluded_countries
        )
        return excluded_countries

    @excluded_countries.setter  # type: ignore[no-redef]
    def excluded_countries(self, value: Optional[List[str]]) -> None:

        if value is None:
            self._excluded_countries = None
        else:
            self._excluded_countries = StringListAdapter.dump_python(
                value,
                mode="json",
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

        if hasattr(self, "_checks_view_cache") and self._checks_view_cache is not None:
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


class Alpha(Base):

    __tablename__ = "alphas"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH), nullable=False, unique=True
    )
    author: MappedColumn[str] = mapped_column(String(16), nullable=False)
    name: MappedColumn[Optional[str]] = mapped_column(String(128), nullable=True)
    category: MappedColumn[Optional[str]] = mapped_column(String(16), nullable=True)
    _themes: MappedColumn[Optional[JSON]] = mapped_column(
        JSON, nullable=True, name="themes"
    )
    team: MappedColumn[Optional[str]] = mapped_column(String(256), nullable=True)
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

    # 因子模拟配置数据，拍平存储到这张表里
    language: MappedColumn[RegularLanguage] = mapped_column(
        Enum(RegularLanguage), nullable=False, default=RegularLanguage.DEFAULT
    )
    test_period: MappedColumn[Optional[str]] = mapped_column(String(64), nullable=True)
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
    # 配置数据结束

    signature: MappedColumn[str] = mapped_column(String(32), nullable=False, index=True)

    date_created: MappedColumn[datetime] = mapped_column(DateTime, nullable=False)
    date_submitted: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    date_modified: MappedColumn[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
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
    _regular: MappedColumn[JSON] = mapped_column(JSON, nullable=True, name="regular")
    _combo: MappedColumn[JSON] = mapped_column(JSON, nullable=True, name="combo")
    _selection: MappedColumn[JSON] = mapped_column(
        JSON, nullable=True, name="selection"
    )
    _pyramids: MappedColumn[Optional[JSON]] = mapped_column(
        JSON, nullable=True, name="pyramids"
    )
    _classifications: MappedColumn[JSON] = mapped_column(
        JSON, nullable=True, name="classifications"
    )
    _competitions: MappedColumn[JSON] = mapped_column(
        JSON, nullable=True, name="competitions"
    )
    _tags: MappedColumn[Optional[JSON]] = mapped_column(JSON, name="tags")

    def __init__(self, **kwargs: Any) -> None:

        themes: Optional[List[str]] = kwargs.pop("themes", None)
        regular: Optional[ExpressionView] = kwargs.pop("regular", None)
        combo: Optional[ExpressionView] = kwargs.pop("combo", None)
        selection: Optional[ExpressionView] = kwargs.pop("selection", None)
        pyramids: Optional[List[PyramidRefView]] = kwargs.pop("pyramids", None)
        classifications: Optional[List[ClassificationRefView]] = kwargs.pop(
            "classifications", None
        )
        competitions: Optional[List[IdNameRefView]] = kwargs.pop(
            "competitions", None
        )
        tags = kwargs.pop("tags", None)

        super().__init__(**kwargs)
        if tags is not None:
            self._tags = StringListAdapter.dump_python(
                tags,
                mode="json",
            )

        self._regular_view_cache: Optional[ExpressionView] = None
        self._combo_view_cache: Optional[ExpressionView] = None
        self._selection_view_cache: Optional[ExpressionView] = None
        self._pyramids_view_cache: Optional[List[PyramidRefView]] = None
        self._classifications_view_cache: Optional[List[ClassificationRefView]] = None
        self._competitions_view_cache: Optional[List[IdNameRefView]] = None

        if themes is not None:
            self._themes = StringListAdapter.dump_python(
                themes,
                mode="json",
            )

        if regular is not None:
            self._regular = cast(Any, regular.model_dump(mode="json"))
            self._regular_view_cache = regular

        if combo is not None:
            self._combo = cast(Any, combo.model_dump(mode="json"))
            self._combo_view_cache = combo

        if selection is not None:
            self._selection = cast(Any, selection.model_dump(mode="json"))
            self._selection_view_cache = selection

        if pyramids is not None:
            self._pyramids = PyramidRefViewListAdapter.dump_python(
                pyramids,
                mode="json",
            )
            self._pyramids_view_cache = pyramids

        if classifications is not None:
            self._classifications = ClassificationRefViewListAdapter.dump_python(
                classifications,
                mode="json",
            )
            self._classifications_view_cache = classifications

        if competitions is not None:
            self._competitions = CompetitionRefViewListAdapter.dump_python(
                competitions,
                mode="json",
            )
            self._competitions_view_cache = competitions

    @hybrid_property
    def themes(self) -> List[str]:
        if self._themes is None:
            return []
        themes: List[str] = StringListAdapter.validate_python(self._themes)
        return themes

    @themes.setter  # type: ignore[no-redef]
    def themes(self, value: Optional[List[str]]) -> None:
        if value is None:
            self._themes = None
        else:
            self._themes = StringListAdapter.dump_python(
                value,
                mode="json",
            )

    @hybrid_property
    def regular(self) -> Optional[ExpressionView]:

        if self._regular is None:
            return None

        if (
            hasattr(self, "_regular_view_cache")
            and self._regular_view_cache is not None
        ):
            return self._regular_view_cache
        regular: ExpressionView = ExpressionView.model_validate(self._regular)
        if regular is None:
            return None

        self._regular_view_cache = regular
        return regular

    @regular.setter  # type: ignore[no-redef]
    def regular(self, value: Optional[ExpressionView]) -> None:

        if value is None:
            self._regular = None
            self._regular_view_cache = None
        else:
            self._regular = value.model_dump(mode="json")
            self._regular_view_cache = value

    @hybrid_property
    def combo(self) -> Optional[ExpressionView]:
        if self._combo is None:
            return None

        if hasattr(self, "_combo_view_cache") and self._combo_view_cache is not None:
            return self._combo_view_cache
        combo: ExpressionView = ExpressionView.model_validate(self._combo)
        if combo is None:
            return None

        self._combo_view_cache = combo
        return combo

    @combo.setter  # type: ignore[no-redef]
    def combo(self, value: Optional[ExpressionView]) -> None:
        if value is None:
            self._combo = None
            self._combo_view_cache = None
        else:
            self._combo = value.model_dump(mode="json")
            self._combo_view_cache = value

    @hybrid_property
    def selection(self) -> Optional[ExpressionView]:
        if self._selection is None:
            return None

        if (
            hasattr(self, "_selection_view_cache")
            and self._selection_view_cache is not None
        ):
            return self._selection_view_cache
        selection: ExpressionView = ExpressionView.model_validate(self._selection)
        if selection is None:
            return None

        # 缓存选择
        self._selection_view_cache = selection
        return selection

    @selection.setter  # type: ignore[no-redef]
    def selection(self, value: Optional[ExpressionView]) -> None:

        if value is None:
            self._selection = None
            self._selection_view_cache = None
        else:
            self._selection = value.model_dump(mode="json")
            self._selection_view_cache = value

    @hybrid_property
    def pyramids(self) -> List[PyramidRefView]:

        if self._pyramids is None:
            return []

        if (
            hasattr(self, "_pyramids_view_cache")
            and self._pyramids_view_cache is not None
        ):
            return self._pyramids_view_cache
        pyramids: List[PyramidRefView] = PyramidRefViewListAdapter.validate_python(
            self._pyramids
        )
        if pyramids is None:
            return []

        # 缓存金字塔
        self._pyramids_view_cache = pyramids
        return pyramids

    @pyramids.setter  # type: ignore[no-redef]
    def pyramids(self, value: Optional[List[PyramidRefView]]) -> None:

        if value is None:
            self._pyramids = None
            self._pyramids_view_cache = None
        else:
            self._pyramids = PyramidRefViewListAdapter.dump_python(
                value,
                mode="json",
            )
            self._pyramids_view_cache = value

    @hybrid_property
    def classifications(self) -> List[ClassificationRefView]:

        if self._classifications is None:
            return []

        if (
            hasattr(self, "_classifications_view_cache")
            and self._classifications_view_cache is not None
        ):
            return self._classifications_view_cache
        classifications: List[ClassificationRefView] = (
            ClassificationRefViewListAdapter.validate_python(self._classifications)
        )
        if classifications is None:
            return []

        # 缓存分类
        self._classifications_view_cache = classifications
        return classifications

    @classifications.setter  # type: ignore[no-redef]
    def classifications(self, value: Optional[List[ClassificationRefView]]) -> None:

        if value is None:
            self._classifications = None
            self._classifications_view_cache = None
        else:
            self._classifications = ClassificationRefViewListAdapter.dump_python(
                value,
                mode="json",
            )
            self._classifications_view_cache = value

    @hybrid_property
    def competitions(self) -> List[IdNameRefView]:

        if self._competitions is None:
            return []

        if (
            hasattr(self, "_competitions_view_cache")
            and self._competitions_view_cache is not None
        ):
            return self._competitions_view_cache
        competitions: List[IdNameRefView] = (
            CompetitionRefViewListAdapter.validate_python(self._competitions)
        )
        if competitions is None:
            return []

        # 缓存比赛
        self._competitions_view_cache = competitions
        return competitions

    @competitions.setter  # type: ignore[no-redef]
    def competitions(self, value: Optional[List[IdNameRefView]]) -> None:

        if value is None:
            self._competitions = None
            self._competitions_view_cache = None
        else:
            self._competitions = CompetitionRefViewListAdapter.dump_python(
                value,
                mode="json",
            )
            self._competitions_view_cache = value

    @hybrid_property
    def tags(self) -> List[str]:

        if self._tags is None:
            return []
        return StringListAdapter.validate_python(self._tags)

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: Optional[List[str]]) -> None:

        if value is None:
            self._tags = None
        else:
            self._tags = StringListAdapter.dump_python(
                value,
                mode="json",
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


@event.listens_for(Alpha, "before_insert")
@event.listens_for(Alpha, "before_update")
def generate_signature(
    mapper: Any,  # pylint: disable=unused-argument
    connection: Any,  # pylint: disable=unused-argument
    target: "Alpha",
) -> None:
    target.signature = get_alpha_signature(
        target.regular,
        target.combo,
        target.selection,
        target.region,
        target.delay,
        target.language,
        target.instrument_type,
        target.universe,
        target.truncation,
        target.unit_handling,
        target.test_period,
        target.pasteurization,
        target.decay,
        target.neutralization,
        target.visualization,
        target.max_trade,
    )


def get_alpha_signature(
    regular: Optional[ExpressionView],
    combo: Optional[ExpressionView],
    selection: Optional[ExpressionView],
    region: Region,
    delay: Delay,
    language: RegularLanguage,
    instrument_type: InstrumentType,
    universe: Universe,
    truncation: Optional[float],
    unit_handling: UnitHandling,
    test_period: Optional[str],
    pasteurization: Switch,
    decay: int,
    neutralization: Neutralization,
    visualization: bool,
    max_trade: Optional[Switch],
) -> str:
    features_dict: Dict[str, Any] = {
        "regular": regular,
        "combo": combo,
        "selection": selection,
        "region": region,
        "delay": delay,
        "language": language,
        "instrument_type": instrument_type,
        "universe": universe,
        "truncation": truncation,
        "unit_handling": unit_handling,
        "test_period": test_period,
        "pasteurization": pasteurization,
        "decay": decay,
        "neutralization": neutralization,
        "visualization": visualization,
        "max_trade": max_trade,
    }
    # 过滤掉值为 None 的键值对
    filtered_features = {
        k: v
        for k, v in features_dict.items()
        if v is not None and (not isinstance(v, str) or v.strip())
    }
    # 将过滤后的字典转换为字符串
    features_str = json.dumps(
        filtered_features,
        sort_keys=True,
        default=lambda x: (
            x.model_dump(mode="json") if hasattr(x, "model_dump") else str(x)
        ),
    )
    # 生成 MD5 哈希值
    signature: str = hashlib.md5(features_str.encode("utf-8")).hexdigest()
    return signature
