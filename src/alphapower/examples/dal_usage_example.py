"""
DAL 使用示例模块。

此模块演示如何正确地将数据访问层 (DAL) 与数据库会话上下文管理器结合使用。
"""

from typing import List, Optional

from alphapower.entity.alphas import Alpha
from alphapower.entity.dal import AlphaDAL, SettingDAL
from alphapower.internal.db_session import get_db_session


async def get_alpha_by_id(alpha_id: str) -> Optional[Alpha]:
    """
    通过 ID 获取 Alpha 对象的示例。

    Args:
        alpha_id: Alpha 的唯一标识。

    Returns:
        找到的 Alpha 对象或 None。
    """
    async with get_db_session("main_db") as session:
        # 在会话上下文中创建 DAL 实例
        alpha_dal = AlphaDAL(session)
        return await alpha_dal.find_by_alpha_id(alpha_id)


async def get_all_alphas_by_author(author: str) -> List[Alpha]:
    """
    获取指定作者的所有 Alpha 对象。

    Args:
        author: 作者名称。

    Returns:
        作者的 Alpha 列表。
    """
    async with get_db_session("main_db") as session:
        # 使用工厂方法创建 DAL
        alpha_dal = AlphaDAL.create(session)
        return await alpha_dal.find_by_author(author)


async def update_alpha_with_settings(
    alpha_id: str, alpha_name: str, instrument_type: str
) -> Optional[Alpha]:
    """
    更新 Alpha 及其关联的设置。

    此示例展示如何在同一事务中操作多个相关实体。

    Args:
        alpha_id: Alpha 的唯一标识。
        alpha_name: 新的 Alpha 名称。
        instrument_type: 新的工具类型设置。

    Returns:
        更新后的 Alpha 对象或 None。
    """
    async with get_db_session("main_db") as session:
        # 在同一会话中创建多个 DAL 实例
        alpha_dal = AlphaDAL(session)
        setting_dal = SettingDAL(session)

        # 查找 Alpha
        alpha = await alpha_dal.find_by_alpha_id(alpha_id)
        if not alpha:
            return None

        # 更新 Alpha
        await alpha_dal.update(alpha.id, name=alpha_name)

        # 如果有关联的设置，也更新设置
        if alpha.settings_id:
            await setting_dal.update(alpha.settings_id, instrument_type=instrument_type)

        # 重新获取更新后的 Alpha（包括关联对象）
        return await alpha_dal.find_by_alpha_id(alpha_id)


async def create_alpha_with_settings(
    alpha_id: str, author: str, instrument_type: str, region: str
) -> Alpha:
    """
    创建新的 Alpha 及其设置。

    此示例展示如何在单一事务中创建多个相关实体。

    Args:
        alpha_id: Alpha 的唯一标识。
        author: 作者名称。
        instrument_type: 工具类型设置。
        region: 区域设置。

    Returns:
        新创建的 Alpha 对象。
    """
    async with get_db_session("main_db") as session:
        # 创建 DAL 实例
        alpha_dal = AlphaDAL(session)
        setting_dal = SettingDAL(session)

        # 首先创建设置
        setting = await setting_dal.create_entity(
            instrument_type=instrument_type,
            region=region,
            universe="default",
            delay=1,
            decay=0,
            neutralization="industry",
            truncation=0.1,
            pasteurization="standard",
            unit_handling="default",
            nan_handling="fill_zero",
            language="python",
            visualization=True,
        )

        # 然后使用设置创建 Alpha
        alpha = await alpha_dal.create_entity(
            alpha_id=alpha_id,
            type="standard",
            author=author,
            settings_id=setting.id,  # 引用新创建的设置
            favorite=False,
            hidden=False,
            tags="new",
            grade="A",
            stage="development",
            status="active",
        )

        return alpha


async def complex_query_example(min_sharpe: float, author: str) -> List[Alpha]:
    """
    执行复杂查询的示例。

    此示例展示如何使用自定义查询构建更复杂的数据获取逻辑。

    Args:
        min_sharpe: 最小夏普比率。
        author: 作者名称。

    Returns:
        满足条件的 Alpha 列表。
    """
    async with get_db_session("main_db") as session:
        alpha_dal = AlphaDAL(session)

        # 创建自定义查询
        from sqlalchemy import and_, select

        from alphapower.entity import Alpha, Sample

        query = (
            select(Alpha)
            .join(Alpha.in_sample)
            .where(
                and_(
                    Sample.sharpe >= min_sharpe,
                    Alpha.author == author,
                    Alpha.hidden == False,
                )
            )
            .order_by(Sample.sharpe.desc())
        )

        # 执行查询
        return await alpha_dal.execute_query(query, session=session)


async def batch_operation_example(status: str, new_grade: str) -> int:
    """
    批量操作的示例。

    此示例展示如何执行批量更新操作。

    Args:
        status: 要更新的 Alpha 状态。
        new_grade: 新的等级值。

    Returns:
        更新的记录数量。
    """
    async with get_db_session("main_db") as session:
        alpha_dal = AlphaDAL(session)

        # 执行批量更新
        return await alpha_dal.update_by_query(
            filter_kwargs={"status": status},
            update_kwargs={"grade": new_grade},
            session=session,
        )
