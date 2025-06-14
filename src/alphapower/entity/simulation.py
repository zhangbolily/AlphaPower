# -*- coding: utf-8 -*-


from datetime import datetime
from typing import Any, Dict, List, Optional, cast

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    Text,
    event,
    func,
)
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedColumn, Mapper, mapped_column

from alphapower.constants import (
    ALPHA_ID_LENGTH,
    AlphaType,
    CodeLanguage,
    Decay,
    Delay,
    InstrumentType,
    Neutralization,
    Region,
    SimulationTaskStatus,
    Switch,
    Truncation,
    UnitHandling,
    Universe,
    get_delay_for_region,
    get_neutralization_for_instrument_region,
    get_regions_for_instrument_type,
    get_universe_for_instrument_region,
    is_region_supported_for_instrument_type,
)
from alphapower.view.alpha import StringListAdapter


class Base(AsyncAttrs, DeclarativeBase):
    pass


class SimulationTask(Base):
    __tablename__ = "simulation_tasks"

    # 标识符字段
    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    type: Mapped[AlphaType] = mapped_column(Enum(AlphaType), nullable=False)
    status: Mapped[SimulationTaskStatus] = mapped_column(
        Enum(SimulationTaskStatus), nullable=False, default=SimulationTaskStatus.DEFAULT
    )
    instrument_type: Mapped[InstrumentType] = mapped_column(
        Enum(InstrumentType), nullable=False, default=InstrumentType.DEFAULT
    )
    region: Mapped[Region] = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )
    universe: Mapped[Universe] = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )
    delay: Mapped[Delay] = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )
    neutralization: Mapped[Neutralization] = mapped_column(
        Enum(Neutralization), nullable=False, default=Neutralization.DEFAULT
    )
    pasteurization: Mapped[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )
    unit_handling: Mapped[UnitHandling] = mapped_column(
        Enum(UnitHandling), nullable=False, default=UnitHandling.DEFAULT
    )
    max_trade: Mapped[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )
    nan_handling: Mapped[Switch] = mapped_column(
        Enum(Switch), nullable=False, default=Switch.DEFAULT
    )

    # 基础数据类型字段
    settings_group_key: Mapped[str] = mapped_column(
        String(32), nullable=False, index=True
    )
    regular: Mapped[str] = mapped_column(Text, nullable=False)
    alpha_id: Mapped[Optional[str]] = mapped_column(
        String(ALPHA_ID_LENGTH), nullable=True
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    signature: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    _tags: Mapped[Optional[JSON]] = mapped_column(JSON, nullable=True, name="tags")
    parent_progress_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    child_progress_id: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    language: Mapped[CodeLanguage] = mapped_column(
        Enum(CodeLanguage), nullable=False, default=CodeLanguage.DEFAULT
    )
    test_period: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # 数值类型字段
    decay: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    truncation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 布尔类型字段
    visualization: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # JSON类型字段
    result: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    dependencies: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)

    # 时间字段
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
    )
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        onupdate=func.now(),  # pylint: disable=E1102
        insert_default=func.now(),  # pylint: disable=E1102
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def __init__(self, **kwargs: Any) -> None:
        # 处理 tags 属性 (如果存在)
        tags: Optional[List[str]] = kwargs.pop("tags", None)
        self._initializing: bool = True

        # 调用父类的 __init__ 处理其他属性
        super().__init__(**kwargs)

        # 手动设置 _tags 属性
        if tags is not None:
            self._tags = StringListAdapter.dump_python(tags, mode="json")

        self._initializing = False

        # 生成 settings_group_key
        self._update_settings_group_key()

        # 验证字段间的关系约束
        self.validate_field_relationships()

    def _update_settings_group_key(self) -> None:
        if self.region and self.delay and self.language and self.instrument_type:
            # 存在 None 的情况，都视为初始化未完成的实例，不要执行自动更新操作
            self.settings_group_key = (
                f"{self.region.value}_{self.delay.value}_"
                + f"{self.language.value}_{self.instrument_type.value}"
            )

    def validate_field_relationships(self) -> None:
        if self.region == Region.DEFAULT:
            raise ValueError("region 不能使用 DEFAULT 值")
        if self.instrument_type == InstrumentType.DEFAULT:
            raise ValueError("instrument_type 不能使用 DEFAULT 值")
        if self.universe == Universe.DEFAULT:
            raise ValueError("universe 不能使用 DEFAULT 值")
        if (
            self.decay is not None
            and not Decay.MIN.value <= self.decay <= Decay.MAX.value
        ):
            raise ValueError(
                f"decay 必须在 {Decay.MIN.value} 到 {Decay.MAX.value} 之间"
            )
        if (
            self.truncation is not None
            and not Truncation.MIN.value <= self.truncation <= Truncation.MAX.value
        ):
            raise ValueError(
                f"truncation 必须在 {Truncation.MIN.value} 到 {Truncation.MAX.value} 之间"
            )
        if self.delay == Delay.DEFAULT:
            raise ValueError("delay 不能使用 DEFAULT 值")
        if self.neutralization == Neutralization.DEFAULT:
            raise ValueError("neutralization 不能使用 DEFAULT 值")

        # 验证 region 和 instrument_type 的兼容性
        if not is_region_supported_for_instrument_type(
            self.region, self.instrument_type
        ):
            supported_regions: List[Region] = get_regions_for_instrument_type(
                self.instrument_type
            )
            raise ValueError(
                f"区域 {self.region.value} 不支持证券类型 {self.instrument_type.value}。"
                f"支持的区域有: {[r.value for r in supported_regions]}"
            )

        # 验证 universe 是否与 region 和 instrument_type 兼容
        valid_universes: List[Universe] = get_universe_for_instrument_region(
            self.instrument_type, self.region
        )
        if valid_universes and self.universe not in valid_universes:
            raise ValueError(
                f"选股范围 {self.universe.value} 对证券类型 {self.instrument_type.value} "
                f"和区域 {self.region.value} 无效。"
                f"有效选项: {[u.value for u in valid_universes]}"
            )

        # 验证 delay 是否与 region 兼容
        valid_delays: List[Delay] = get_delay_for_region(self.region)
        if valid_delays and self.delay not in valid_delays:
            raise ValueError(
                f"延迟设置 {self.delay.value} 对区域 {self.region.value} 无效。"
                f"有效选项: {[d.value for d in valid_delays]}"
            )

        # 验证 neutralization 是否与 region 和 instrument_type 兼容
        valid_neutralizations: List[Neutralization] = (
            get_neutralization_for_instrument_region(self.instrument_type, self.region)
        )
        if valid_neutralizations and self.neutralization not in valid_neutralizations:
            raise ValueError(
                f"中性化策略 {self.neutralization.value} 对证券类型 {self.instrument_type.value} "
                f"和区域 {self.region.value} 无效。"
                f"有效选项: {[n.value for n in valid_neutralizations]}"
            )

    @hybrid_property
    def tags(self) -> Optional[List[str]]:
        if self._tags is None:
            return None
        tags: List[str] = StringListAdapter.validate_python(self._tags)
        return tags

    @tags.setter  # type: ignore[no-redef]
    def tags(self, value: Optional[List[str]]) -> None:
        if value is None:
            self._tags = None
        else:
            self._tags = StringListAdapter.dump_python(value, mode="json")

    def add_tag(self, tag: str) -> None:
        if not tag or not tag.strip():
            return

        current_tags: List[str] = self.tags or []
        if tag.strip() not in current_tags:
            current_tags.append(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]

    def remove_tag(self, tag: str) -> None:
        if not tag or not tag.strip() or not self.tags:
            return

        current_tags: List[str] = cast(List[str], self.tags)
        if tag.strip() in current_tags:
            current_tags.remove(tag.strip())
            self.tags = current_tags  # type: ignore[method-assign]


# SQLAlchemy 事件监听器定义
# ==========================


@event.listens_for(SimulationTask, "before_insert")
def validate_before_insert(
    mapper: Mapper,  # pylint: disable=unused-argument
    connection: Connection,  # pylint: disable=unused-argument
    target: SimulationTask,
) -> None:
    target.validate_field_relationships()
    target._update_settings_group_key()  # pylint: disable=W0212


@event.listens_for(SimulationTask, "before_update")
def validate_before_update(
    mapper: Mapper,  # pylint: disable=unused-argument
    connection: Connection,  # pylint: disable=unused-argument
    target: SimulationTask,
) -> None:
    target.validate_field_relationships()
    target._update_settings_group_key()  # pylint: disable=W0212
