"""数据模型类定义模块。

本模块包含用于表示数据集、数据字段、统计数据、研究论文等信息的ORM模型类。
这些模型类用于定义数据的结构和关系，便于在应用中统一管理和操作数据。

典型用法:
    dataset = Dataset(dataset_id="DS001", name="金融数据集")
    category = DataCategory(category_id="CAT001", name="金融数据")
    dataset.category = category
"""

from sqlalchemy import (
    JSON,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship


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


class Category(Base):
    """数据类别类，用于表示数据的分类信息。

    一个数据类别可以包含多个数据集和数据字段，用于对数据进行分类管理。

    Attributes:
        id: 自增主键ID。
        category_id: 分类唯一标识，不可重复。
        name: 分类名称。
        datasets: 与该分类关联的数据集列表。
        data_fields: 与该分类关联的数据字段列表。
    """

    __tablename__ = "categories"  # 修改表名，去掉data前缀

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id = mapped_column(String, unique=True)  # 分类唯一标识
    name = mapped_column(String)  # 分类名称
    datasets = relationship("Dataset", back_populates="category")  # 与数据集的关系
    data_fields = relationship(
        "DataField", back_populates="category"
    )  # 与数据字段的关系


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
        category_id: 外键，关联到categories表。
        category: 数据集所属的分类(关联DataCategory)。
        subcategory_id: 外键，关联到categories表。
        subcategory: 数据集所属的子分类(关联DataCategory)。
        data_fields: 与数据集关联的数据字段列表。
        stats_data: 与数据集关联的统计数据列表。
        research_papers: 与数据集关联的研究论文列表。
        pyramid_multiplier: 金字塔乘数。
    """

    __tablename__ = "datasets"  # 保持不变，已无data前缀

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id = mapped_column(String)  # 数据集唯一标识
    name = mapped_column(String)  # 数据集名称
    description = mapped_column(String)  # 数据集描述
    region = mapped_column(String)  # 数据集所属区域
    delay = mapped_column(Integer)  # 数据延迟(小时)
    universe = mapped_column(String)  # 数据集的覆盖范围
    coverage = mapped_column(Float)  # 数据覆盖率(0.0-1.0)
    value_score = mapped_column(Float)  # 数据价值评分(0.0-10.0)
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha数量
    field_count = mapped_column(Integer)  # 字段数量
    themes = mapped_column(JSON)  # 数据集主题
    category_id = mapped_column(Integer, ForeignKey("categories.id"))  # 修改外键引用
    category = relationship("DataCategory", back_populates="datasets")  # 分类关系
    subcategory_id = mapped_column(
        Integer, ForeignKey("categories.id")  # 修改外键引用
    )  # 子分类 ID
    subcategory = relationship("DataCategory", back_populates="datasets")  # 子分类关系
    data_fields = relationship("DataField", back_populates="datasets")  # 数据字段关系
    stats_data = relationship("StatsData", back_populates="datasets")  # 统计数据关系
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
        category_id: 外键，关联到categories表。
        category: 字段所属的分类(关联DataCategory)。
        subcategory_id: 外键，关联到categories表。
        subcategory: 字段所属的子分类(关联DataCategory)。
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

    __tablename__ = "data_fields"  # 保持不变，按要求保留

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_id = mapped_column(String)  # 字段唯一标识
    description = mapped_column(String)  # 字段描述
    dataset_id = mapped_column(Integer, ForeignKey("datasets.id"))  # 数据集 ID
    dataset = relationship("Dataset", back_populates="data_fields")  # 数据集关系
    category_id = mapped_column(Integer, ForeignKey("categories.id"))  # 修改外键引用
    category = relationship("DataCategory", back_populates="data_fields")  # 分类关系
    subcategory_id = mapped_column(
        Integer, ForeignKey("categories.id")  # 修改外键引用
    )  # 子分类 ID
    subcategory = relationship(
        "DataCategory", back_populates="data_fields"
    )  # 子分类关系
    region = mapped_column(String)  # 字段所属区域
    delay = mapped_column(Integer)  # 字段延迟
    universe = mapped_column(String)  # 字段范围
    type = mapped_column(String)  # 字段类型
    coverage = mapped_column(Float)  # 字段覆盖率
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    themes = mapped_column(JSON)  # 字段主题
    stats_data = relationship("StatsData", back_populates="data_fields")  # 统计数据关系
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

    __tablename__ = "stats_data"  # 保持不变，按要求保留

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_set_id = mapped_column(Integer, ForeignKey("datasets.id"))  # 数据集 ID
    data_set = relationship("Dataset", back_populates="stats_data")  # 数据集关系
    data_field_id = mapped_column(Integer, ForeignKey("data_fields.id"))  # 数据字段 ID
    data_field = relationship("DataField", back_populates="stats_data")  # 数据字段关系
    region = mapped_column(String)  # 统计数据所属区域
    delay = mapped_column(Integer)  # 统计数据延迟
    universe = mapped_column(String)  # 统计数据范围
    coverage = mapped_column(Float)  # 统计数据覆盖率
    value_score = mapped_column(Float)  # 统计数据价值评分
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    field_count = mapped_column(Integer)  # 字段数量
    themes = mapped_column(JSON)  # 统计数据主题


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

    __tablename__ = "research_papers"  # 保持不变，已无data前缀

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    type = mapped_column(String)  # 论文类型
    title = mapped_column(String)  # 论文标题
    url = mapped_column(String)  # 论文链接
    datasets = relationship(
        "Dataset", secondary=dataset_research_papers, back_populates="research_papers"
    )  # 数据集关系
