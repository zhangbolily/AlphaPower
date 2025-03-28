from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# 中间表，用于表示 DataSet 和 ResearchPaper 之间的多对多关系
data_set_research_papers = Table(
    "data_set_research_papers",
    Base.metadata,
    Column("data_set_id", Integer, ForeignKey("data_sets.id"), primary_key=True),
    Column(
        "research_paper_id", Integer, ForeignKey("research_papers.id"), primary_key=True
    ),
)


class Data_Category(Base):
    __tablename__ = "data_category"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id = mapped_column(String, unique=True)
    name = mapped_column(String)
    data_sets = relationship("DataSet", back_populates="category")
    data_fields = relationship("DataField", back_populates="category")


class Data_Subcategory(Base):
    __tablename__ = "data_subcategory"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    subcategory_id = mapped_column(String, unique=True)
    name = mapped_column(String)
    data_sets = relationship("DataSet", back_populates="subcategory")
    data_fields = relationship("DataField", back_populates="subcategory")


class DataSet(Base):
    __tablename__ = "data_sets"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id = mapped_column(String)
    name = mapped_column(String)
    description = mapped_column(String)
    region = mapped_column(String)
    delay = mapped_column(Integer)
    universe = mapped_column(String)
    coverage = mapped_column(Float)
    value_score = mapped_column(Float)
    user_count = mapped_column(Integer)
    alpha_count = mapped_column(Integer)
    field_count = mapped_column(Integer)
    themes = mapped_column(JSON)
    category_id = mapped_column(Integer, ForeignKey("data_category.id"))
    category = relationship("Data_Category", back_populates="data_sets")
    subcategory_id = mapped_column(Integer, ForeignKey("data_subcategory.id"))
    subcategory = relationship("Data_Subcategory", back_populates="data_sets")
    data_fields = relationship("DataField", back_populates="dataset")
    stats_data = relationship("StatsData", back_populates="data_set")
    research_papers = relationship(
        "ResearchPaper",
        secondary=data_set_research_papers,
        back_populates="data_sets",
    )
    pyramid_multiplier = mapped_column(Float, nullable=True)

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
    __tablename__ = "data_fields"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    field_id = mapped_column(String)
    description = mapped_column(String)
    dataset_id = mapped_column(Integer, ForeignKey("data_sets.id"))
    dataset = relationship("DataSet", back_populates="data_fields")
    category_id = mapped_column(Integer, ForeignKey("data_category.id"))
    category = relationship("Data_Category", back_populates="data_fields")
    subcategory_id = mapped_column(Integer, ForeignKey("data_subcategory.id"))
    subcategory = relationship("Data_Subcategory", back_populates="data_fields")
    region = mapped_column(String)
    delay = mapped_column(Integer)
    universe = mapped_column(String)
    type = mapped_column(String)
    coverage = mapped_column(Float)
    user_count = mapped_column(Integer)
    alpha_count = mapped_column(Integer)
    themes = mapped_column(JSON)
    stats_data = relationship("StatsData", back_populates="data_field")
    pyramid_multiplier = mapped_column(Float, nullable=True)


class StatsData(Base):
    __tablename__ = "stats_data"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_set_id = mapped_column(Integer, ForeignKey("data_sets.id"))
    data_set = relationship("DataSet", back_populates="stats_data")
    data_field_id = mapped_column(Integer, ForeignKey("data_fields.id"))
    data_field = relationship("DataField", back_populates="stats_data")
    region = mapped_column(String)
    delay = mapped_column(Integer)
    universe = mapped_column(String)
    coverage = mapped_column(Float)
    value_score = mapped_column(Float)
    user_count = mapped_column(Integer)
    alpha_count = mapped_column(Integer)
    field_count = mapped_column(Integer)
    themes = mapped_column(JSON)


class ResearchPaper(Base):
    __tablename__ = "research_papers"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    type = mapped_column(String)
    title = mapped_column(String)
    url = mapped_column(String)
    data_sets = relationship(
        "DataSet", secondary=data_set_research_papers, back_populates="research_papers"
    )
