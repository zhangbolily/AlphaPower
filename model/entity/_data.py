from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    JSON,
    ForeignKey,
    Table,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(String, unique=True)
    name = Column(String)
    data_sets = relationship("DataSet", back_populates="category")
    data_fields = relationship("DataField", back_populates="category")


class Data_Subcategory(Base):
    __tablename__ = "data_subcategory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subcategory_id = Column(String, unique=True)
    name = Column(String)
    data_sets = relationship("DataSet", back_populates="subcategory")
    data_fields = relationship("DataField", back_populates="subcategory")


class DataSet(Base):
    __tablename__ = "data_sets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String, unique=False)
    name = Column(String)
    description = Column(String)
    region = Column(String)
    delay = Column(Integer)
    universe = Column(String)
    coverage = Column(Float)
    value_score = Column(Float)
    user_count = Column(Integer)
    alpha_count = Column(Integer)
    field_count = Column(Integer)
    themes = Column(JSON)
    category_id = Column(Integer, ForeignKey("data_category.id"))
    category = relationship("Data_Category", back_populates="data_sets")
    subcategory_id = Column(Integer, ForeignKey("data_subcategory.id"))
    subcategory = relationship("Data_Subcategory", back_populates="data_sets")
    data_fields = relationship("DataField", back_populates="dataset")
    stats_data = relationship("StatsData", back_populates="data_set")
    research_papers = relationship(
        "ResearchPaper",
        secondary=data_set_research_papers,
        back_populates="data_sets",
    )

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

    id = Column(Integer, primary_key=True, autoincrement=True)
    field_id = Column(String, unique=True)
    description = Column(String)
    dataset_id = Column(Integer, ForeignKey("data_sets.id"))
    dataset = relationship("DataSet", back_populates="data_fields")
    category_id = Column(Integer, ForeignKey("data_category.id"))
    category = relationship("Data_Category", back_populates="data_fields")
    subcategory_id = Column(Integer, ForeignKey("data_subcategory.id"))
    subcategory = relationship("Data_Subcategory", back_populates="data_fields")
    region = Column(String)
    delay = Column(Integer)
    universe = Column(String)
    type = Column(String)
    coverage = Column(Float)
    user_count = Column(Integer)
    alpha_count = Column(Integer)
    themes = Column(JSON)
    stats_data = relationship("StatsData", back_populates="data_field")


class StatsData(Base):
    __tablename__ = "stats_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_set_id = Column(Integer, ForeignKey("data_sets.id"))
    data_set = relationship("DataSet", back_populates="stats_data")
    data_field_id = Column(Integer, ForeignKey("data_fields.id"))
    data_field = relationship("DataField", back_populates="stats_data")
    region = Column(String)
    delay = Column(Integer)
    universe = Column(String)
    coverage = Column(Float)
    value_score = Column(Float)
    user_count = Column(Integer)
    alpha_count = Column(Integer)
    field_count = Column(Integer)
    themes = Column(JSON)


class ResearchPaper(Base):
    __tablename__ = "research_papers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)
    title = Column(String)
    url = Column(String)
    data_sets = relationship(
        "DataSet", secondary=data_set_research_papers, back_populates="research_papers"
    )
