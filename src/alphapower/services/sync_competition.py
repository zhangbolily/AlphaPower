"""
模块名称: sync_competition

模块功能:
    提供从 WQ 客户端同步竞赛数据到数据库的功能。模块包含以下主要职责:
    1. 从 WQ 客户端获取竞赛数据。
    2. 将竞赛数据转换为竞赛实体对象。
    3. 将竞赛实体对象保存到数据库中。

主要组件:
    - create_competition: 根据视图对象创建竞赛实体。
    - fetch_competitions: 从 WQ 客户端获取竞赛数据。
    - process_competitions: 处理竞赛数据并保存到数据库。
    - sync_competition: 主同步逻辑，协调上述功能完成数据同步。

日志规范:
    - 使用 structlog 记录日志，支持异步日志接口。
    - 日志级别包括 DEBUG、INFO、ERROR 等，覆盖函数入参、出参及异常信息。
    - 使用 Emoji 丰富日志内容，便于快速识别日志信息。

异常处理:
    - 捕获并记录所有异常，确保日志中包含完整的堆栈信息，便于排查问题。

使用方法:
    运行模块的主函数 `sync_competition` 即可完成竞赛数据的同步。
"""

from typing import Any, Dict, List

from structlog.stdlib import BoundLogger

from alphapower.client import CompetitionListView, CompetitionView, wq_client
from alphapower.constants import Database
from alphapower.dal.alphas import CompetitionDAL
from alphapower.dal.base import DALFactory
from alphapower.entity import Competition
from alphapower.internal.db_session import get_db_session
from alphapower.internal.logging import get_logger

logger: BoundLogger = get_logger(__name__)


async def create_competition(view: CompetitionView) -> Competition:
    """
    根据视图对象创建竞赛实体。

    Args:
        view (CompetitionView): 竞赛视图对象。

    Returns:
        Competition: 竞赛实体对象。
    """
    # 记录函数入参
    await logger.adebug("创建竞赛实体", view=view)

    if view.universities:
        await logger.adebug("竞赛包含大学", universities=view.universities)

    competition = Competition(
        competition_id=view.id,
        name=view.name,
        description=view.description,
        universities=view.universities,
        countries=view.countries,
        excluded_countries=view.excluded_countries,
        status=view.status,
        team_based=view.team_based,
        start_date=view.start_date,
        end_date=view.end_date,
        sign_up_start_date=view.sign_up_start_date,
        sign_up_end_date=view.sign_up_end_date,
        sign_up_date=view.sign_up_date,
        team=view.team,
        scoring=view.scoring,
        leaderboard=(
            view.leaderboard.model_dump(mode="python") if view.leaderboard else None
        ),
        prize_board=view.prize_board,
        university_board=view.university_board,
        submissions=view.submissions,
        faq=view.faq,
        progress=view.progress,
    )
    # 记录函数出参
    await logger.adebug("创建竞赛实体完成", competition=competition)
    return competition


async def fetch_competitions(page: int, page_size: int) -> CompetitionListView:
    """
    从 WQ 客户端获取竞赛数据。

    Args:
        page (int): 当前页码。
        page_size (int): 每页数据量。

    Returns:
        CompetitionListView: 包含竞赛数据的视图对象。
    """
    offset: int = (page - 1) * page_size
    params: Dict[str, Any] = {"limit": page_size, "offset": offset}

    # DEBUG 日志记录请求参数
    await logger.adebug("获取竞赛数据请求参数", params=params)

    try:
        result: CompetitionListView = await wq_client.alpha_fetch_competitions(
            params=params
        )
        # DEBUG 日志记录返回结果
        await logger.adebug("获取竞赛数据成功", result=result)
        return result
    except Exception as e:
        # ERROR 日志记录异常
        await logger.aerror("获取竞赛数据失败", error=str(e), emoji="❌", exc_info=True)
        raise


async def process_competitions(
    competition_dal: CompetitionDAL, competitions: List[Competition]
) -> None:
    """
    处理竞赛数据并保存到数据库。

    Args:
        competition_dal (CompetitionDAL): 数据访问层对象。
        competitions (List[Competition]): 竞赛实体列表。
    """
    try:
        for competition in competitions:
            # DEBUG 日志记录创建竞赛实体
            await logger.adebug("创建竞赛实体", competition=competition)
            await competition_dal.upsert_by_unique_key(competition, "competition_id")
        # DEBUG 日志记录批量创建成功
        await logger.adebug("批量创建竞赛数据成功", count=len(competitions))
    except Exception as e:
        # ERROR 日志记录异常
        await logger.aerror(
            "批量创建竞赛数据失败", error=str(e), emoji="❌", exc_info=True
        )
        raise


async def competition_data_expire_check() -> bool:
    """
    检查竞赛数据是否过期。

    Returns:
        bool: 如果数据过期返回 True，否则返回 False。
    """
    async with get_db_session(Database.ALPHAS) as session:
        competition_dal: CompetitionDAL = DALFactory.create_dal(
            CompetitionDAL, session=session
        )
        local_count: int = await competition_dal.count()

    async with wq_client:
        result: CompetitionListView = await fetch_competitions(1, 1)
        remote_count: int = result.count

    # DEBUG 日志记录本地和远程数据量
    await logger.adebug(
        "本地和远程数据量",
        local_count=local_count,
        remote_count=remote_count,
    )
    return local_count != remote_count


async def sync_competition() -> None:
    """
    从数据库同步竞赛数据到 WQ 客户端。
    """
    # INFO 日志记录方法进入
    await logger.ainfo("开始同步竞赛数据", emoji="🚀")

    page_size: int = 100
    page: int = 1

    async with get_db_session(Database.ALPHAS) as session:
        competition_dal: CompetitionDAL = DALFactory.create_dal(
            CompetitionDAL, session=session
        )

        async with wq_client:
            while True:
                result: CompetitionListView = await fetch_competitions(page, page_size)
                competitions: List[Competition] = [
                    await create_competition(view) for view in result.results
                ]

                await process_competitions(competition_dal, competitions)

                if not result.next:
                    # INFO 日志记录同步完成
                    await logger.ainfo("竞赛数据同步完成", emoji="✅")
                    break

                page += 1

    # INFO 日志记录方法退出
    await logger.ainfo("同步竞赛数据方法退出", emoji="🏁")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_competition())
