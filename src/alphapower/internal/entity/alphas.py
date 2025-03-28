from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship


class Base(DeclarativeBase):
    pass


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

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_type = mapped_column(String)
    region = mapped_column(String)
    universe = mapped_column(String)
    delay = mapped_column(Integer)
    decay = mapped_column(Integer)
    neutralization = mapped_column(String)
    truncation = mapped_column(Float)
    pasteurization = mapped_column(String)
    unit_handling = mapped_column(String)
    nan_handling = mapped_column(String)
    language = mapped_column(String)
    visualization = mapped_column(Boolean)
    test_period = mapped_column(String, nullable=True)
    max_trade = mapped_column(Float, nullable=True)  # 新增 maxTrade 字段


class Alphas_Regular(Base):
    __tablename__ = "alphas_regular"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    code = mapped_column(String)
    description = mapped_column(String, nullable=True)
    operator_count = mapped_column(Integer)


class Alphas_Classification(Base):
    __tablename__ = "alphas_classification"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    classification_id = mapped_column(String, nullable=False, unique=True)
    name = mapped_column(String)


class Alphas_Competition(Base):
    __tablename__ = "alphas_competition"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    competition_id = mapped_column(String, nullable=False, unique=True)
    name = mapped_column(String)


class Alphas_Sample_Check(Base):
    __tablename__ = "alphas_sample_check"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    name = mapped_column(String)
    result = mapped_column(String)
    limit = mapped_column(Float, nullable=True)
    value = mapped_column(Float, nullable=True)
    date = mapped_column(DateTime, nullable=True)
    competitions = mapped_column(String, nullable=True)
    message = mapped_column(String, nullable=True)


class Alphas_Sample(Base):
    __tablename__ = "alphas_sample"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    pnl = mapped_column(Float, nullable=True)
    book_size = mapped_column(Float, nullable=True)
    long_count = mapped_column(Integer, nullable=True)
    short_count = mapped_column(Integer, nullable=True)
    turnover = mapped_column(Float, nullable=True)
    returns = mapped_column(Float, nullable=True)
    drawdown = mapped_column(Float, nullable=True)
    margin = mapped_column(Float, nullable=True)
    sharpe = mapped_column(Float, nullable=True)
    fitness = mapped_column(Float, nullable=True)
    start_date = mapped_column(DateTime)
    checks_id = mapped_column(
        Integer, ForeignKey("alphas_sample_check.id")
    )  # 添加外键连接到 Alphas_Sample_Check 表
    checks = relationship(
        "Alphas_Sample_Check", backref="alphas_sample"
    )  # 定义 checks 字段的关系
    self_correration = mapped_column(Float, nullable=True)
    prod_correration = mapped_column(Float, nullable=True)
    os_is_sharpe_ratio = mapped_column(Float, nullable=True)
    pre_close_sharpe_ratio = mapped_column(Float, nullable=True)


class Alphas(Base):
    __tablename__ = "alphas"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id = mapped_column(String, nullable=False, unique=True)
    type = mapped_column(String)
    author = mapped_column(String)
    settings_id = mapped_column(Integer, ForeignKey("alphas_settings.id"))
    settings = relationship("Alphas_Settings", backref="alphas")
    regular_id = mapped_column(Integer, ForeignKey("alphas_regular.id"))
    regular = relationship("Alphas_Regular", backref="alphas")
    date_created = mapped_column(DateTime)
    date_submitted = mapped_column(DateTime, nullable=True)
    date_modified = mapped_column(DateTime, nullable=True)
    name = mapped_column(String, nullable=True)
    favorite = mapped_column(Boolean)
    hidden = mapped_column(Boolean)
    color = mapped_column(String, nullable=True)
    category = mapped_column(String, nullable=True)
    tags = mapped_column(String)
    classifications = relationship(
        "Alphas_Classification", secondary=alphas_classifications, backref="alphas"
    )  # 使用多对多关系
    grade = mapped_column(String)
    stage = mapped_column(String)
    status = mapped_column(String)
    in_sample_id = mapped_column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    in_sample = relationship(
        "Alphas_Sample", foreign_keys=[in_sample_id], backref="alphas_inSample"
    )
    out_sample_id = mapped_column(
        Integer, ForeignKey("alphas_sample.id"), nullable=True
    )
    out_sample = relationship(
        "Alphas_Sample", foreign_keys=[out_sample_id], backref="alphas_outSample"
    )
    train_id = mapped_column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    train = relationship(
        "Alphas_Sample", foreign_keys=[train_id], backref="alphas_train"
    )
    test_id = mapped_column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    test = relationship("Alphas_Sample", foreign_keys=[test_id], backref="alphas_test")
    prod_id = mapped_column(Integer, ForeignKey("alphas_sample.id"), nullable=True)
    prod = relationship("Alphas_Sample", foreign_keys=[prod_id], backref="alphas_prod")
    competitions = relationship(
        "Alphas_Competition", secondary=alphas_competitions, backref="alphas"
    )  # 使用多对多关系
    themes = mapped_column(String, nullable=True)
    pyramids = mapped_column(String, nullable=True)
    team = mapped_column(String, nullable=True)
