from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Table,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# 中间表，用于表示 Alphas 和 Alphas_Classification 之间的多对多关系
alphas_classifications = Table(
    "alphas_classifications_ref",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "classification_id",
        Integer,
        ForeignKey("alphas_classification.id"),
        primary_key=True,
    ),
)

# 中间表，用于表示 Alphas 和 Competitions 之间的多对多关系
alphas_competitions = Table(
    "alphas_competitions_ref",
    Base.metadata,
    Column("alpha_id", Integer, ForeignKey("alphas.id"), primary_key=True),
    Column(
        "competition_id",
        Integer,
        ForeignKey("alphas_competition.id"),
        primary_key=True,
    ),
)


class Alphas_Settings(Base):
    __tablename__ = "alphas_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_type = Column(String)
    region = Column(String)
    universe = Column(String)
    delay = Column(Integer)
    decay = Column(Integer)
    neutralization = Column(String)
    truncation = Column(Float)
    pasteurization = Column(String)
    unit_handling = Column(String)
    nan_handling = Column(String)
    language = Column(String)
    visualization = Column(Boolean)
    test_period = Column(String, nullable=True)
    max_trade = Column(Float, nullable=True)  # 新增 maxTrade 字段


class Alphas_Regular(Base):
    __tablename__ = "alphas_regular"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String)
    description = Column(String, nullable=True)
    operator_count = Column(Integer)


class Alphas_Classification(Base):
    __tablename__ = "alphas_classification"

    id = Column(Integer, primary_key=True, autoincrement=True)
    classification_id = Column(String, nullable=False, unique=True)
    name = Column(String)


class Alphas_Competition(Base):
    __tablename__ = "alphas_competition"

    id = Column(Integer, primary_key=True, autoincrement=True)
    competition_id = Column(String, nullable=False, unique=True)
    name = Column(String)


class Alphas_Sample_Check(Base):
    __tablename__ = "alphas_sample_check"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    result = Column(String)
    limit = Column(Float, nullable=True)
    value = Column(Float, nullable=True)
    date = Column(DateTime, nullable=True)
    competitions = Column(String, nullable=True)
    message = Column(String, nullable=True)


class Alphas_Sample(Base):
    __tablename__ = "alphas_sample"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pnl = Column(Float, nullable=True)
    book_size = Column(Float, nullable=True)
    long_count = Column(Integer, nullable=True)
    short_count = Column(Integer, nullable=True)
    turnover = Column(Float, nullable=True)
    returns = Column(Float, nullable=True)
    drawdown = Column(Float, nullable=True)
    margin = Column(Float, nullable=True)
    sharpe = Column(Float, nullable=True)
    fitness = Column(Float, nullable=True)
    start_date = Column(DateTime)
    checks_id = Column(
        Integer, ForeignKey("alphas_sample_check.id")
    )  # 添加外键连接到 Alphas_Sample_Check 表
    checks = relationship(
        "Alphas_Sample_Check", backref="alphas_sample"
    )  # 定义 checks 字段的关系
    self_correration = Column(Float, nullable=True)
    prod_correration = Column(Float, nullable=True)
    os_is_sharpe_ratio = Column(Float, nullable=True)
    pre_close_sharpe_ratio = Column(Float, nullable=True)


class Alphas(Base):
    __tablename__ = "alphas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alpha_id = Column(String, nullable=False, unique=True)
    type = Column(String)
    author = Column(String)
    settings_id = Column(Integer, ForeignKey("alphas_settings.id"))
    settings = relationship("Alphas_Settings", backref="alphas")
    regular_id = Column(Integer, ForeignKey("alphas_regular.id"))
    regular = relationship("Alphas_Regular", backref="alphas")
    date_created = Column(DateTime)
    date_submitted = Column(DateTime, nullable=True)
    date_modified = Column(DateTime, nullable=True)
    name = Column(String, nullable=True)
    favorite = Column(Boolean)
    hidden = Column(Boolean)
    color = Column(String, nullable=True)
    category = Column(String, nullable=True)
    tags = Column(String)
    classifications = relationship(
        "Alphas_Classification", secondary=alphas_classifications, backref="alphas"
    )  # 使用多对多关系
    grade = Column(String)
    stage = Column(String)
    status = Column(String)
    in_sample_id = Column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    in_sample = relationship(
        "Alphas_Sample", foreign_keys=[in_sample_id], backref="alphas_inSample"
    )
    out_sample_id = Column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    out_sample = relationship(
        "Alphas_Sample", foreign_keys=[out_sample_id], backref="alphas_outSample"
    )
    train_id = Column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    train = relationship(
        "Alphas_Sample", foreign_keys=[train_id], backref="alphas_train"
    )
    test_id = Column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    test = relationship("Alphas_Sample", foreign_keys=[test_id], backref="alphas_test")
    prod_id = Column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    prod = relationship("Alphas_Sample", foreign_keys=[prod_id], backref="alphas_prod")
    competitions = relationship(
        "Alphas_Competition", secondary=alphas_competitions, backref="alphas"
    )  # 使用多对多关系
    themes = Column(String, nullable=True)
    pyramids = Column(String, nullable=True)
    team = Column(String, nullable=True)
