import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Boolean,
    Index,
    Enum,
    DECIMAL,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AlphaSimulateResult(Base):
    __tablename__ = 'alpha_simulate_result'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='主键')
    progress_id = Column(String(64), nullable=False, comment='进度id')
    alpha_id = Column(String(64), nullable=True, comment='alpha id')
    name = Column(String(64), nullable=True, comment='名称')
    simulate_type = Column(Enum('DEFAULT', 'REGULAR'), nullable=False, comment='模拟类型')
    instrument_type = Column(Enum('DEFAULT', 'EQUITY'), nullable=False, comment='证券类型')
    region = Column(Enum('DEFAULT', 'USA'), nullable=False, comment='地区')
    universe = Column(Enum('DEFAULT', 'TOP3000'), nullable=False, comment='股票池')
    delay = Column(Integer, nullable=False, comment='延迟')
    decay = Column(Integer, nullable=False, comment='衰减')
    neutralization = Column(Enum('DEFAULT', 'SUBINDUSTRY'), nullable=False, comment='中性化')
    truncation = Column(DECIMAL(5, 3), nullable=False, comment='截断')
    pasteurization = Column(Enum('DEFAULT', 'ON'), nullable=False, comment='去除停牌')
    unit_handling = Column(Enum('DEFAULT', 'VERIFY'), nullable=False, comment='单位处理')
    nan_handling = Column(Enum('DEFAULT', 'ON'), nullable=False, comment='nan处理')
    language = Column(Enum('DEFAULT', 'FASTEXPR'), nullable=False, comment='语言')
    visualization = Column(Boolean, nullable=False, comment='可视化')
    regular = Column(Text, nullable=False, comment='表达式')
    factors = Column(Text, nullable=False, comment='因子列表，字母升序')
    status = Column(Enum('DEFAULT', 'IN_PROGRESS', 'COMPLETE', 'ERROR'), default='DEFAULT', comment='状态')
    err_msg = Column(Text, nullable=True, comment='错误信息')
    grade = Column(Enum('DEFAULT','INFERIOR', 'GOOD', 'SPECTACULAR'), nullable=True, comment='等级')
    stage = Column(Enum('DEFAULT', 'IS', 'OS'), nullable=True, comment='阶段')
    in_sample_pnl = Column(Integer, nullable=True, comment='IS PNL')
    in_sample_book_size = Column(Integer, nullable=True, comment='IS book size')
    in_sample_long_count = Column(Integer, nullable=True, comment='IS long count')
    in_sample_short_count = Column(Integer, nullable=True, comment='IS short count')
    in_sample_turnover = Column(Float, nullable=True, comment='IS turnover')
    in_sample_returns = Column(Float, nullable=True, comment='IS returns')
    in_sample_drawdown = Column(Float, nullable=True, comment='IS drawdown')
    in_sample_margin = Column(Float, nullable=True, comment='IS margin')
    in_sample_sharpe = Column(Float, nullable=True, comment='IS sharpe')
    in_sample_fitness = Column(Float, nullable=True, comment='IS fitness')
    create_time = Column(DateTime, default=datetime.datetime.now, comment='创建时间')
    update_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, comment='更新时间')
    delete_time = Column(DateTime, nullable=True, comment='删除时间')
    submited_time = Column(DateTime, nullable=True, comment='提交时间')

    __table__args__ = (
        Index('idx_progress_id', 'progress_id', unique=True),
        Index('idx_alpha_id', 'alpha_id', unique=True),
        Index('idx_regular', 'regular'),
    )

    def __str__(self):
        return f"object : <id:{self.id} name:{self.name}>"