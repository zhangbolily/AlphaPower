import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model.entity.alpha_simulate_result import AlphaSimulateResult, Base
from worldquant import WorldQuantClient, SelfAlphaListQueryParams
import json

# 配置数据库连接
DATABASE_URI = 'sqlite:///alphas.db'  # 使用 SQLite 数据库
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# 创建表
Base.metadata.create_all(engine)

def sync_alphas():
    # 初始化 API 客户端
    with open('credentials.json') as f:
        credentials = json.load(f)
        username = credentials['username']
        password = credentials['password']
    
    client = WorldQuantClient(username=username, password=password)

    # 创建 SelfAlphaListRequest 实例
    query = SelfAlphaListQueryParams(hidden="false", limit=5, offset=0, order="-dateCreated", status="UNSUBMITTED")

    offset = 0
    limit = 100
    count = 0
    while count < 500:
        # 获取 alphas 数据
        query.offset = offset
        query.limit = limit
        response = client.get_self_alphas(query)
        print(f"获取到 {len(response.results)} 条数据")

        if len(response.results) == 0:
            break

        offset += limit
        
        # 同步到本地数据库
        for alpha_data in response.results:
            alpha = AlphaSimulateResult(
                progress_id=alpha_data.id,
                alpha_id=alpha_data.id,
                name=alpha_data.name,
                simulate_type=alpha_data.type,
                instrument_type=alpha_data.settings.instrumentType,
                region=alpha_data.settings.region,
                universe=alpha_data.settings.universe,
                delay=alpha_data.settings.delay,
                decay=alpha_data.settings.decay,
                neutralization=alpha_data.settings.neutralization,
                truncation=alpha_data.settings.truncation,
                pasteurization=alpha_data.settings.pasteurization,
                unit_handling=alpha_data.settings.unitHandling,
                nan_handling=alpha_data.settings.nanHandling,
                language=alpha_data.settings.language,
                visualization=alpha_data.settings.visualization,
                regular=alpha_data.regular.code,
                factors=",".join(alpha_data.tags),
                status=alpha_data.status,
                grade=alpha_data.grade,
                stage=alpha_data.stage,
                in_sample_pnl=alpha_data.inSample.pnl,
                in_sample_book_size=alpha_data.inSample.bookSize,
                in_sample_long_count=alpha_data.inSample.longCount,
                in_sample_short_count=alpha_data.inSample.shortCount,
                in_sample_turnover=alpha_data.inSample.turnover,
                in_sample_returns=alpha_data.inSample.returns,
                in_sample_drawdown=alpha_data.inSample.drawdown,
                in_sample_margin=alpha_data.inSample.margin,
                in_sample_sharpe=alpha_data.inSample.sharpe,
                in_sample_fitness=alpha_data.inSample.fitness,
                create_time=datetime.datetime.fromisoformat(alpha_data.dateCreated),
                update_time=datetime.datetime.fromisoformat(alpha_data.dateModified),
                delete_time=None
            )
            session.add(alpha)  # 使用 merge 方法避免重复插入
        
        count += len(response.results)
    session.commit()

if __name__ == "__main__":
    sync_alphas()
    print("Alphas 同步完成")