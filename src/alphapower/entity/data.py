"""数据模型类定义模块。

本模块包含用于表示数据集、数据字段、统计数据、研究论文等信息的ORM模型类。
这些模型类用于定义数据的结构和关系，便于在应用中统一管理和操作数据。

典型用法:
    dataset = Dataset(dataset_id="DS001", name="金融数据集")
    category = DataCategory(category_id="CAT001", name="金融数据")
    dataset.category = category
"""

from sqlalchemy import (
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship

from alphapower.constants import Regoin


class Base(AsyncAttrs, DeclarativeBase):
    """基础类，所有ORM模型类都继承自该类。

    提供了异步属性访问功能和SQLAlchemy的基本ORM功能。
    """


# 中间表，用于表示Dataset和ResearchPaper之间的多对多关系
dataset_research_papers = Table(
    "dataset_research_papers",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id"), primary_key=True),
    Column(
        "research_paper_id", Integer, ForeignKey("research_papers.id"), primary_key=True
    ),
)

# 中间表，用于表示Dataset和Category之间的多对多关系
dataset_categories = Table(
    "dataset_categories",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id"), primary_key=True),
    Column("category_id", Integer, ForeignKey("categories.id"), primary_key=True),
)

# 复用dataset_categories作为subcategory关系表
dataset_subcategories = dataset_categories

# 中间表，用于表示DataField和Category之间的多对多关系
datafield_categories = Table(
    "datafield_categories",
    Base.metadata,
    Column("datafield_id", Integer, ForeignKey("data_fields.id"), primary_key=True),
    Column("category_id", Integer, ForeignKey("categories.id"), primary_key=True),
)

# 复用datafield_categories作为subcategory关系表
datafield_subcategories = datafield_categories


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

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    delay = mapped_column(Integer)  # 延迟 天
    multiplier = mapped_column(Float)  # 金字塔乘数
    region = mapped_column(Enum(Regoin), nullable=False, default=Regoin.DEFAULT)  # 区域
    category_id = mapped_column(
        Integer, ForeignKey("categories.id"), nullable=False
    )  # 分类ID
    category = relationship("Category", backref="pyramids")  # 分类关系


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
        datasets: 与该分类关联的数据集列表。
        data_fields: 与该分类关联的数据字段列表。
    """

    __tablename__ = "categories"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id = mapped_column(String, unique=True)  # 分类唯一标识
    name = mapped_column(String)  # 分类名称

    # 添加父分类关联
    parent_id = mapped_column(Integer, ForeignKey("categories.id"), nullable=True)
    parent = relationship("Category", remote_side=[id], backref="children")

    # 保留与Dataset和DataField的多对多关系
    datasets = relationship(
        "Dataset", secondary=dataset_categories, back_populates="categories"
    )

    data_fields = relationship(
        "DataField", secondary=datafield_categories, back_populates="categories"
    )


class Dataset(Base):
    """数据集类，用于表示具体的数据集信息。

    数据集包含了完整的元数据信息，如数据来源、覆盖范围、延迟等，
    用于描述一个完整的可用于分析的数据集合。

    Attributes:
        id: 自增主键ID。
        dataset_id: 数据集唯一标识。
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

    __tablename__ = "datasets"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id = mapped_column(String)  # 数据集唯一标识
    name = mapped_column(String)  # 数据集名称
    description = mapped_column(String)  # 数据集描述
    region = mapped_column(
        Enum(Regoin), nullable=False, default=Regoin.DEFAULT
    )  # 数据集所属区域
    delay = mapped_column(Integer)  # 数据延迟(小时)
    universe = mapped_column(String)  # 数据集的覆盖范围
    coverage = mapped_column(Float)  # 数据覆盖率(0.0-1.0)
    value_score = mapped_column(Float)  # 数据价值评分(0.0-10.0)
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha数量
    field_count = mapped_column(Integer)  # 字段数量
    # themes = mapped_column(JSON, nullable=True)  # 数据集主题

    # 保留与Category的多对多关系
    categories = relationship(
        "Category", secondary=dataset_categories, back_populates="datasets"
    )

    # 将subcategory修改为多对多关系，使用同一个中间表
    subcategories = relationship(
        "Category",
        secondary=dataset_subcategories,
        overlaps="categories",  # 指示这个关系与categories重叠
    )

    data_fields = relationship("DataField", back_populates="dataset")  # 数据字段关系
    stats_data = relationship("StatsData", back_populates="data_set")  # 统计数据关系
    research_papers = relationship(
        "ResearchPaper",
        secondary=dataset_research_papers,
        back_populates="datasets",
    )  # 研究论文关系
    pyramid_multiplier = mapped_column(Float, nullable=True)  # 金字塔乘数

    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "region",
            "universe",
            "delay",
            name="_dataset_region_universe_delay_uc",
        ),
    )


class DataField(Base):
    """数据字段类，用于表示数据集中的字段信息。

    数据字段是数据集的组成部分，包含了字段的详细信息和统计数据。

    Attributes:
        id: 自增主键ID。
        field_id: 字段唯一标识。
        description: 字段描述。
        dataset_id: 外键，关联到datasets表。
        dataset: 字段所属的数据集(关联Dataset)。
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

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_id = mapped_column(String)  # 字段唯一标识
    description = mapped_column(String)  # 字段描述
    dataset_id = mapped_column(Integer, ForeignKey("datasets.id"))  # 数据集 ID
    dataset = relationship("Dataset", back_populates="data_fields")  # 数据集关系

    # 保留与Category的多对多关系
    categories = relationship(
        "Category", secondary=datafield_categories, back_populates="data_fields"
    )

    # 将subcategory修改为多对多关系，使用同一个中间表
    subcategories = relationship(
        "Category",
        secondary=datafield_subcategories,
        overlaps="categories",  # 指示这个关系与categories重叠
    )

    region = mapped_column(
        Enum(Regoin), nullable=False, default=Regoin.DEFAULT
    )  # 字段所属区域
    delay = mapped_column(Integer)  # 字段延迟
    universe = mapped_column(String)  # 字段范围
    type = mapped_column(String)  # 字段类型
    coverage = mapped_column(Float)  # 字段覆盖率
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    # themes = mapped_column(JSON)  # 字段主题
    stats_data = relationship("StatsData", back_populates="data_field")  # 统计数据关系
    pyramid_multiplier = mapped_column(Float, nullable=True)  # 金字塔乘数


class StatsData(Base):
    """统计数据类，用于表示与数据集或字段相关的统计信息。

    统计数据包含了与数据集或字段相关的统计信息，如覆盖率、价值评分等。

    Attributes:
        id: 自增主键ID。
        data_set_id: 外键，关联到datasets表。
        data_set: 统计数据所属的数据集(关联Dataset)。
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

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_set_id = mapped_column(Integer, ForeignKey("datasets.id"))  # 数据集 ID
    data_set = relationship("Dataset", back_populates="stats_data")  # 数据集关系
    data_field_id = mapped_column(Integer, ForeignKey("data_fields.id"))  # 数据字段 ID
    data_field = relationship("DataField", back_populates="stats_data")  # 数据字段关系
    region = mapped_column(
        Enum(Regoin), nullable=False, default=Regoin.DEFAULT
    )  # 统计数据所属区域
    delay = mapped_column(Integer)  # 统计数据延迟
    universe = mapped_column(String)  # 统计数据范围
    coverage = mapped_column(Float)  # 统计数据覆盖率
    value_score = mapped_column(Float)  # 统计数据价值评分
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    field_count = mapped_column(Integer)  # 字段数量
    # themes = mapped_column(JSON)  # 统计数据主题


class ResearchPaper(Base):
    """研究论文类，用于表示与数据集相关的研究论文信息。

    研究论文包含了与数据集相关的研究信息，如论文标题、链接等。

    Attributes:
        id: 自增主键ID。
        type: 论文类型。
        title: 论文标题。
        url: 论文链接。
        datasets: 与论文关联的数据集列表。
    """

    __tablename__ = "research_papers"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    type = mapped_column(String)  # 论文类型
    title = mapped_column(String)  # 论文标题
    url = mapped_column(String)  # 论文链接
    datasets = relationship(
        "Dataset", secondary=dataset_research_papers, back_populates="research_papers"
    )  # 数据集关系
