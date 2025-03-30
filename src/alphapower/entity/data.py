"""
数据模型类定义
用于表示数据集、数据字段、统计数据、研究论文等信息的 ORM 模型类。
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
    """基础类，所有 ORM 模型类都继承自该类。"""

    pass


# 中间表，用于表示 Dataset 和 ResearchPaper 之间的多对多关系
dataset_research_papers = Table(
    "dataset_research_papers",
    Base.metadata,
    Column("dataset_id", Integer, ForeignKey("datasets.id"), primary_key=True),
    Column(
        "research_paper_id", Integer, ForeignKey("research_papers.id"), primary_key=True
    ),
)


class DataCategory(Base):
    """数据类别类，用于表示数据的分类信息。"""

    __tablename__ = "data_categories"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id = mapped_column(String, unique=True)  # 分类唯一标识
    name = mapped_column(String)  # 分类名称
    datasets = relationship("Dataset", back_populates="category")  # 与数据集的关系
    data_fields = relationship(
        "DataField", back_populates="category"
    )  # 与数据字段的关系


class Dataset(Base):
    """数据集类，用于表示具体的数据集信息。"""

    __tablename__ = "datasets"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id = mapped_column(String)  # 数据集唯一标识
    name = mapped_column(String)  # 数据集名称
    description = mapped_column(String)  # 数据集描述
    region = mapped_column(String)  # 数据集所属区域
    delay = mapped_column(Integer)  # 数据延迟
    universe = mapped_column(String)  # 数据集的范围
    coverage = mapped_column(Float)  # 数据覆盖率
    value_score = mapped_column(Float)  # 数据价值评分
    user_count = mapped_column(Integer)  # 用户数量
    alpha_count = mapped_column(Integer)  # Alpha 数量
    field_count = mapped_column(Integer)  # 字段数量
    themes = mapped_column(JSON)  # 数据集主题
    category_id = mapped_column(Integer, ForeignKey("data_categories.id"))  # 分类 ID
    category = relationship("DataCategory", back_populates="datasets")  # 分类关系
    subcategory_id = mapped_column(
        Integer, ForeignKey("data_categories.id")
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
    """数据字段类，用于表示数据集中的字段信息。"""

    __tablename__ = "data_fields"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_id = mapped_column(String)  # 字段唯一标识
    description = mapped_column(String)  # 字段描述
    dataset_id = mapped_column(Integer, ForeignKey("datasets.id"))  # 数据集 ID
    dataset = relationship("Dataset", back_populates="data_fields")  # 数据集关系
    category_id = mapped_column(Integer, ForeignKey("data_categories.id"))  # 分类 ID
    category = relationship("DataCategory", back_populates="data_fields")  # 分类关系
    subcategory_id = mapped_column(
        Integer, ForeignKey("data_categories.id")
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
    """统计数据类，用于表示与数据集或字段相关的统计信息。"""

    __tablename__ = "stats_data"

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
    """研究论文类，用于表示与数据集相关的研究论文信息。"""

    __tablename__ = "research_papers"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    type = mapped_column(String)  # 论文类型
    title = mapped_column(String)  # 论文标题
    url = mapped_column(String)  # 论文链接
    datasets = relationship(
        "Dataset", secondary=dataset_research_papers, back_populates="research_papers"
    )  # 数据集关系
