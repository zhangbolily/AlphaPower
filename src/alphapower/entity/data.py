"""数据模型类定义模块。

本模块包含用于表示数据集、数据字段、统计数据、研究论文等信息的ORM模型类。
这些模型类用于定义数据的结构和关系，便于在应用中统一管理和操作数据。

典型用法:
    data_set = DataSet(data_set_id="DS001", name="金融数据集")
    category = DataCategory(category_id="CAT001", name="金融数据")
    data_set.category = category
"""

from typing import Any, List, Optional

from sqlalchemy import (
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from alphapower.constants import DataFieldType, Delay, Region, Universe
from alphapower.view.alpha import StringListAdapter
from alphapower.view.common import RegionListAdaptor


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有ORM模型类都继承自该类。

    提供了异步属性访问功能和SQLAlchemy的基本ORM功能。
    """


# 定义所有中间表
# 中间表，用于表示DataSet和ResearchPaper之间的多对多关系
data_set_research_papers = Table(
    "data_set_research_papers",
    Base.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("data_set_id", Integer, ForeignKey("data_sets.id")),
    Column("research_paper_id", Integer, ForeignKey("research_papers.id")),
    UniqueConstraint(
        "data_set_id", "research_paper_id", name="uq_data_set_research_paper"
    ),
)


class Category(Base):
    """数据类别类，用于表示数据的分类信息。

    一个数据类别可以包含多个数据集和数据字段，用于对数据进行分类管理。
    分类之间可以形成父子关系，通过parent_id字段来维护。

    Attributes:
        id: 自增主键ID。
        category_id: 分类唯一标识，不可重复。
        name: 分类名称。
        parent_id: 父分类ID，用于维护分类的层级关系。
        parent: 父分类(关联Category)。
        children: 子分类列表(关联Category)。
        data_sets: 与该分类关联的数据集列表。
    """

    __tablename__ = "categories"

    # 标识符字段
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 基础数据字段
    category_id = mapped_column(String(64), unique=True)  # 分类唯一标识
    name = mapped_column(String(256))  # 分类名称
    _region = mapped_column(
        JSON,
        nullable=False,
        name="region",  # 使用JSON存储Region枚举列表
    )
    data_set_count = mapped_column(Integer, default=0, nullable=False)
    field_count = mapped_column(Integer, default=0, nullable=False)
    alpha_count = mapped_column(Integer, default=0, nullable=False)
    user_count = mapped_column(Integer, default=0, nullable=False)
    value_score = mapped_column(Float, default=0.0, nullable=False)
    parent_id = mapped_column(Integer, ForeignKey("categories.id"), nullable=True)
    parent = relationship("Category", remote_side=[id], backref="children")

    def __init__(self, **kw: Any):
        region: Any = kw.pop("region", None)

        super().__init__(**kw)

        if isinstance(region, list):
            self._region = RegionListAdaptor.dump_python(region, mode="json")

    @hybrid_property
    def region(self) -> List[Region]:
        region: List[Region] = []
        if self._region:
            region = RegionListAdaptor.validate_python(self._region)
        return region

    @region.setter  # type: ignore[no-redef]
    def region(self, value: List[Region]) -> None:
        """设置region属性，确保传入的值是Region枚举类型的列表。"""
        if isinstance(value, list):
            self._region = RegionListAdaptor.dump_python(value, mode="json")
        else:
            raise ValueError("Region must be a list of Region enums.")


class Pyramid(Base):
    """
    金字塔类，用于表示金字塔模型的参数设置。
    金字塔模型通常用于金融数据分析中，用于表示数据的层级结构和延迟。
    Attributes:
        id: 自增主键ID。
        delay: 延迟天数。
        multiplier: 金字塔乘数。
        region: 区域类型。
        category_id: 分类ID，外键关联到Category表。
        category: 分类对象(关联Category)。
    """

    __tablename__ = "pyramids"

    # 标识符字段
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    delay = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )  # 延迟天数
    region = mapped_column(Enum(Region), nullable=False, default=Region.DEFAULT)  # 区域

    # 基础数据字段
    multiplier = mapped_column(Float)  # 金字塔乘数
    category_id = mapped_column(
        Integer, ForeignKey("categories.id"), nullable=False
    )  # 分类ID

    # 关系字段
    category = relationship("Category", backref="pyramids")  # 分类关系


class DataSet(Base):
    """数据集类，用于表示具体的数据集信息。

    数据集包含了完整的元数据信息，如数据来源、覆盖范围、延迟等，
    用于描述一个完整的可用于分析的数据集合。

    Attributes:
        id: 自增主键ID。
        data_set_id: 数据集唯一标识。
        name: 数据集名称。
        description: 数据集详细描述。
        region: 数据集所属地理区域。
        delay: 数据更新延迟(单位:小时)。
        universe: 数据集覆盖的范围。
        coverage: 数据覆盖率(0.0-1.0)。
        value_score: 数据价值评分(0.0-10.0)。
        user_count: 使用该数据集的用户数量。
        alpha_count: Alpha数量。
        field_count: 字段数量。
        themes: 数据集主题。
        categories: 数据集所属的分类列表(多对多关联 Category)。
        subcategories: 数据集所属的子分类列表(多对多关联 Category)。
        data_fields: 与数据集关联的数据字段列表。
        stats_data: 与数据集关联的统计数据列表。
        research_papers: 与数据集关联的研究论文列表。
        pyramid_multiplier: 金字塔乘数。
    """

    __tablename__ = "data_sets"

    # 标识符字段
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    region: Mapped[Region] = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )  # 数据集所属区域
    delay: Mapped[Delay] = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )  # 数据集延迟
    universe: Mapped[Universe] = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )  # 数据集覆盖范围

    # 基础数据字段
    data_set_id: Mapped[str] = mapped_column(String(64))  # 数据集唯一标识
    name: Mapped[str] = mapped_column(String(256))  # 数据集名称
    description: Mapped[str] = mapped_column(Text)  # 数据集描述
    coverage: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )  # 数据覆盖率(0.0-1.0)
    value_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )  # 数据价值评分(0.0-10.0)
    user_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # 用户数量
    alpha_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # Alpha数量
    field_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # 字段数量
    pyramid_multiplier: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )  # 金字塔乘数
    _themes: Mapped[JSON] = mapped_column(
        JSON, nullable=True, name="themes"
    )  # 数据集主题

    # 关系字段
    category_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    subcategory_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    data_fields = relationship("DataField", back_populates="data_set")  # 数据字段关系
    stats_data = relationship("StatsData", back_populates="data_set")  # 统计数据关系
    research_papers = relationship(
        "ResearchPaper",
        secondary=data_set_research_papers,
        back_populates="data_sets",
        cascade="all",
    )  # 研究论文关系

    __table_args__ = (
        UniqueConstraint(
            "data_set_id",
            "region",
            "universe",
            "delay",
            name="_data_set_region_universe_delay_uc",
        ),
    )

    def __init__(self, **kw: Any):
        themes: Any = kw.pop("themes", None)

        super().__init__(**kw)

        if isinstance(themes, list):
            self._themes = StringListAdapter.dump_python(themes, mode="json")

    @hybrid_property
    def themes(self) -> List[str]:
        """获取数据集主题列表。

        返回:
            List[str]: 数据集主题列表。
        """
        themes: List[str] = []
        if self._themes:
            themes = StringListAdapter.validate_python(self._themes)
        return themes

    @themes.setter  # type: ignore[no-redef]
    def themes(self, value: List[str]) -> None:
        """设置数据集主题列表。

        确保传入的值是字符串列表，并将其转换为JSON格式存储。

        Args:
            value (List[str]): 数据集主题列表。
        """
        if isinstance(value, list):
            self._themes = StringListAdapter.dump_python(value, mode="json")
        else:
            raise ValueError("Themes must be a list of strings.")


class DataField(Base):
    """数据字段类，用于表示数据集中的字段信息。

    数据字段是数据集的组成部分，包含了字段的详细信息和统计数据。

    Attributes:
        id: 自增主键ID。
        field_id: 字段唯一标识。
        description: 字段描述。
        data_set_id: 外键，关联到data_sets表。
        data_set: 字段所属的数据集(关联DataSet)。
        categories: 字段所属的分类列表(多对多关联 Category)。
        subcategories: 字段所属的子分类列表(多对多关联 Category)。
        region: 字段所属地理区域。
        delay: 字段更新延迟(单位:小时)。
        universe: 字段覆盖的范围。
        type: 字段类型。
        coverage: 字段覆盖率(0.0-1.0)。
        user_count: 使用该字段的用户数量。
        alpha_count: Alpha数量。
        themes: 字段主题。
        stats_data: 与字段关联的统计数据列表。
        pyramid_multiplier: 金字塔乘数。
    """

    __tablename__ = "data_fields"

    # 标识符字段
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    region = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )  # 字段所属区域
    delay = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )  # 字段延迟
    universe = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )  # 字段覆盖范围
    type = mapped_column(
        Enum(DataFieldType), nullable=False, default=DataFieldType.DEFAULT
    )  # 字段类型

    # 基础数据字段
    field_id = mapped_column(String(256))  # 字段唯一标识
    description = mapped_column(Text)  # 字段描述
    coverage = mapped_column(Float)  # 字段覆盖率
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    pyramid_multiplier = mapped_column(Float, nullable=True)  # 金字塔乘数

    # 关系字段
    data_set_id = mapped_column(
        Integer, ForeignKey("data_sets.id"), nullable=False, index=True
    )  # 数据集 ID
    data_set = relationship(
        "DataSet",
        back_populates="data_fields",
        lazy="joined",  # 使用 joined 方式实现立即加载（eager loading）
    )  # 数据集关系
    category_id = mapped_column(String(64), nullable=False, index=True)
    subcategory_id = mapped_column(String(64), nullable=False, index=True)
    stats_data = relationship("StatsData", back_populates="data_field")  # 统计数据关系


class StatsData(Base):
    """统计数据类，用于表示与数据集或字段相关的统计信息。

    统计数据包含了与数据集或字段相关的统计信息，如覆盖率、价值评分等。

    Attributes:
        id: 自增主键ID。
        data_set_id: 外键，关联到data_sets表。
        data_set: 统计数据所属的数据集(关联DataSet)。
        data_field_id: 外键，关联到data_fields表。
        data_field: 统计数据所属的数据字段(关联DataField)。
        region: 统计数据所属地理区域。
        delay: 统计数据更新延迟(单位:小时)。
        universe: 统计数据覆盖的范围。
        coverage: 统计数据覆盖率(0.0-1.0)。
        value_score: 统计数据价值评分(0.0-10.0)。
        user_count: 使用该统计数据的用户数量。
        alpha_count: Alpha数量。
        field_count: 字段数量。
        themes: 统计数据主题。
    """

    __tablename__ = "stats_data"

    # 标识符字段
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 枚举类型字段
    region = mapped_column(
        Enum(Region), nullable=False, default=Region.DEFAULT
    )  # 统计数据所属区域
    delay = mapped_column(
        Enum(Delay), nullable=False, default=Delay.DEFAULT
    )  # 统计数据延迟
    universe = mapped_column(
        Enum(Universe), nullable=False, default=Universe.DEFAULT
    )  # 统计数据覆盖范围

    # 基础数据字段
    data_set_id = mapped_column(Integer, ForeignKey("data_sets.id"))  # 数据集 ID
    data_field_id = mapped_column(Integer, ForeignKey("data_fields.id"))  # 数据字段 ID
    coverage = mapped_column(Float)  # 统计数据覆盖率
    value_score = mapped_column(Float)  # 统计数据价值评分
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    field_count = mapped_column(Integer)  # 字段数量

    # 关系字段
    data_set = relationship("DataSet", back_populates="stats_data")  # 数据集关系
    data_field = relationship("DataField", back_populates="stats_data")  # 数据字段关系


class ResearchPaper(Base):
    """研究论文类，用于表示与数据集相关的研究论文信息。

    研究论文包含了与数据集相关的研究信息，如论文标题、链接等。

    Attributes:
        id: 自增主键ID。
        type: 论文类型。
        title: 论文标题。
        url: 论文链接。
        data_sets: 与论文关联的数据集列表。
    """

    __tablename__ = "research_papers"

    # 标识符字段
    id = mapped_column(Integer, primary_key=True, autoincrement=True)

    # 基础数据字段
    title = mapped_column(String(256))  # 论文标题
    url = mapped_column(Text)  # 论文链接

    # 关系字段
    data_sets = relationship(
        "DataSet", secondary=data_set_research_papers, back_populates="research_papers"
    )  # 数据集关系
