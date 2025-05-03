from typing import Dict, Final, List, Optional, Set

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from structlog.stdlib import BoundLogger

from alphapower.client import wq_client
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import Database, RecordSetType, Status, TagType
from alphapower.dal import alpha_dal, evaluate_record_dal, record_set_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import EvaluateRecord
from alphapower.internal.decorator import async_timed
from alphapower.internal.logging import get_logger
from alphapower.manager.correlation_manager import CorrelationManager
from alphapower.manager.record_sets_manager import RecordSetsManager
from alphapower.settings import settings
from alphapower.view.alpha import (
    AlphaPropertiesPayload,
    CreateTagsPayload,
    SelfTagListQuery,
    SelfTagListView,
    TagView,
)

logger: BoundLogger = get_logger(__name__)
client: WorldQuantBrainClient = WorldQuantBrainClient(
    username=settings.credential.username,
    password=settings.credential.password,
)

POWER_POOL_TAG: Final[str] = "PowerPoolSelected"


async def fetch_evaluate_records(session: AsyncSession) -> List[str]:
    """获取评估记录并提取唯一的 Alpha ID 列表"""
    ppac_2025_records: List[EvaluateRecord] = await evaluate_record_dal.find_by(
        EvaluateRecord.evaluator == "power_pool",
        session=session,
    )
    alpha_ids: List[str] = list({record.alpha_id for record in ppac_2025_records})
    await logger.ainfo(
        "评估记录已获取",
        alpha_ids=alpha_ids,
        count=len(alpha_ids),
        emoji="📊",
    )
    return alpha_ids


async def fetch_active_alphas(session: AsyncSession) -> Dict[str, Alpha]:
    """获取状态为 ACTIVE 的 Alpha 并构建 alpha_id 到 Alpha 的映射"""
    alphas: List[Alpha] = await alpha_dal.find_by(
        Alpha.status == Status.ACTIVE,
        session=session,
    )
    alpha_id_map: Dict[str, Alpha] = {alpha.alpha_id: alpha for alpha in alphas}
    await logger.ainfo(
        "活跃的 Alpha 已获取",
        count=len(alpha_id_map),
        emoji="🟢",
    )
    return alpha_id_map


async def process_alpha_record_sets(
    alpha_ids: List[str],
    alpha_id_map: Dict[str, Alpha],
    record_sets_manager: RecordSetsManager,
) -> Dict[Alpha, List[float]]:
    """处理 Alpha 的记录集并生成序列字典"""
    sequences_dict: Dict[Alpha, List[float]] = {}
    four_years_ago: pd.DateOffset = pd.DateOffset(years=-4)

    for alpha_id in alpha_ids:
        alpha: Optional[Alpha] = alpha_id_map.get(alpha_id)
        if alpha is None:
            await logger.aerror("Alpha 未找到", alpha_id=alpha_id, emoji="❌")
            raise ValueError(f"Alpha 未找到: {alpha_id}")

        if alpha.in_sample.fitness < 1.2:
            await logger.aerror(
                "Alpha 不符合条件",
                alpha_id=alpha.alpha_id,
                fitness=alpha.in_sample.fitness,
                emoji="❌",
            )
            continue

        record_set_df: pd.DataFrame = await record_sets_manager.get_record_sets(
            alpha=alpha,
            set_type=RecordSetType.DAILY_PNL,
            allow_local=True,
        )
        pnl_df: pd.DataFrame = record_set_df.copy()
        pnl_df["date"] = pd.to_datetime(pnl_df["date"])
        pnl_df = pnl_df.set_index("date")
        pnl_df = pnl_df[pnl_df.index >= (pnl_df.index.max() + four_years_ago)]
        pnl_df = pnl_df.sort_index(ascending=True).fillna(0).ffill()
        sequences_dict[alpha] = pnl_df["pnl"].tolist()

    await logger.ainfo(
        "Alpha 记录集已处理",
        count=len(sequences_dict),
        emoji="📈",
    )
    return sequences_dict


async def update_alpha_tags(
    alphas: List[Alpha],
    least_relevant_set: Set[str],
    client: WorldQuantBrainClient,
) -> None:
    """更新 Alpha 的标签"""
    for alpha in alphas:
        if alpha.alpha_id in least_relevant_set:
            if POWER_POOL_TAG not in alpha.tags:
                alpha.tags = list(set(alpha.tags)) + [POWER_POOL_TAG]  # type: ignore

            await client.update_alpha_properties(
                alpha.alpha_id,
                AlphaPropertiesPayload(name=alpha.alpha_id, tags=alpha.tags),
            )
            await logger.ainfo(
                "Alpha 标签已更新",
                alpha_id=alpha.alpha_id,
                tags=alpha.tags,
                emoji="🏷️",
            )
        else:
            if POWER_POOL_TAG in alpha.tags:
                alpha.tags = [tag for tag in alpha.tags if tag != POWER_POOL_TAG]  # type: ignore
            await client.update_alpha_properties(
                alpha.alpha_id,
                AlphaPropertiesPayload(name=alpha.alpha_id, tags=alpha.tags),
            )
            await logger.ainfo(
                "Alpha 标签已移除",
                alpha_id=alpha.alpha_id,
                tags=alpha.tags,
                emoji="🏷️",
            )


@async_timed
async def main() -> None:
    """主函数，执行主要逻辑"""
    async with session_manager.get_session(Database.EVALUATE) as session:
        alpha_ids = await fetch_evaluate_records(session)
        alpha_id_map = await fetch_active_alphas(session)

    async with wq_client as legacy_client:
        record_sets_manager = RecordSetsManager(
            client=legacy_client,
            record_set_dal=record_set_dal,
        )
        correlation_manager = CorrelationManager()

        sequences_dict = await process_alpha_record_sets(
            alpha_ids, alpha_id_map, record_sets_manager
        )

        corr_matrix = await correlation_manager.compute_pearson_correlation_matrix(
            sequences_dict=sequences_dict
        )
        corr_matrix_df = pd.DataFrame(
            {
                alpha.alpha_id: {
                    other_alpha.alpha_id: value
                    for other_alpha, value in inner_dict.items()
                }
                for alpha, inner_dict in corr_matrix.items()
            }
        )
        await logger.adebug(
            "相关性矩阵已生成",
            shape=corr_matrix_df.shape,
            head=corr_matrix_df.head().to_dict(),
            emoji="🧮",
        )

        least_relevant_set, corr = (
            await correlation_manager.find_least_relavant_submatrix(
                correlation_matrix=corr_matrix_df,
                submatrix_size=10,
                max_matrix_size=10000,
                max_workers=10,
            )
        )
        await logger.ainfo(
            "最不相关子矩阵已找到",
            least_relevant_set=least_relevant_set,
            correlation=corr,
            emoji="🔍",
        )

        await update_alpha_tags(list(alpha_id_map.values()), least_relevant_set, client)

    async with session_manager.get_session(Database.ALPHAS) as session, session.begin():
        for alpha in alpha_id_map.values():
            if session.is_modified(alpha):
                await alpha_dal.update(alpha, session=session)

    await logger.ainfo("Alpha 属性已更新", emoji="✅")

    query: SelfTagListQuery = SelfTagListQuery()
    tags_view: SelfTagListView = await client.fetch_user_tags(
        query=query,
    )

    for tag in tags_view.results:
        if tag.name == "least_relevant_set":
            await logger.ainfo(
                "最不相关子矩阵已存在，删除",
                tag_id=tag.id,
                emoji="🗑️",
            )
            await client.delete_alpha_list(
                tag_id=tag.id,
            )

    payload: CreateTagsPayload = CreateTagsPayload(
        name="least_relevant_set",
        type=TagType.LIST,
        alphas=list(least_relevant_set),
    )
    list_tag_alpha_view: TagView = await client.create_alpha_list(
        payload=payload,
    )

    await logger.ainfo(
        "最不相关子矩阵已创建",
        list_tag_alpha_view=list_tag_alpha_view.model_dump(mode="json"),
        emoji="✅",
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
