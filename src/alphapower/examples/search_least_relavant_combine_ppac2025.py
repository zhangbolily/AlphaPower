from typing import Dict, Final, List, Optional, Set

import pandas as pd
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


@async_timed
async def main() -> None:
    async with session_manager.get_session(Database.EVALUATE) as session:
        ppac_2025_records: List[EvaluateRecord] = await evaluate_record_dal.find_by(
            EvaluateRecord.evaluator == "power_pool",
            session=session,
        )

        alpha_ids: List[str] = []
        for record in ppac_2025_records:
            if record.alpha_id not in alpha_ids:
                alpha_ids.append(record.alpha_id)

        logger.info("Alpha IDs", alpha_ids=alpha_ids)
        logger.info("Total number of unique alpha IDs", count=len(alpha_ids))

        alphas: List[Alpha] = []
        alphas = await alpha_dal.find_by(
            Alpha.status == Status.ACTIVE,
            session=session,
        )
        alpha_id_map: Dict[str, Alpha] = {alpha.alpha_id: alpha for alpha in alphas}

    async with wq_client as legacy_client:
        # Get the record sets manager
        record_sets_manager = RecordSetsManager(
            client=legacy_client,
            record_set_dal=record_set_dal,
        )
        correlation_manager = CorrelationManager()

        four_years_ago: pd.DateOffset = pd.DateOffset(years=-4)
        alpha_record_set_map: Dict[Alpha, pd.DataFrame] = {}
        sequences_dict: Dict[Alpha, List[float]] = {}
        for alpha_id in alpha_ids:
            alpha: Optional[Alpha] = alpha_id_map.get(alpha_id)
            if alpha is None:
                logger.error("Alpha not found", alpha_id=alpha_id)
                raise ValueError(f"Alpha not found: {alpha_id}")

            record_set_df: pd.DataFrame = await record_sets_manager.get_record_sets(
                alpha=alpha,
                set_type=RecordSetType.DAILY_PNL,
                allow_local=True,
            )
            alpha_record_set_map[alpha] = record_set_df

            pnl_df: pd.DataFrame = record_set_df.copy()
            pnl_df["date"] = pd.to_datetime(pnl_df["date"])
            pnl_df = pnl_df.set_index("date")
            pnl_df = pnl_df[pnl_df.index >= (pnl_df.index.max() + four_years_ago)]
            pnl_df = pnl_df.sort_index(ascending=True)
            pnl_df = pnl_df.fillna(0).ffill()
            sequences_dict[alpha] = pnl_df["pnl"].tolist()

    corr_matrix: Dict[Alpha, Dict[Alpha, float]] = (
        await correlation_manager.compute_pearson_correlation_matrix(
            sequences_dict=sequences_dict,
        )
    )

    # 用 Alpha 的 alpha_id 字段作为行列名，构建相关性矩阵 DataFrame
    corr_matrix_df: pd.DataFrame = pd.DataFrame(
        {
            alpha.alpha_id: {
                other_alpha.alpha_id: value for other_alpha, value in inner_dict.items()
            }
            for alpha, inner_dict in corr_matrix.items()
        }
    )
    # 日志输出相关性矩阵的维度和部分内容，方便调试
    await logger.adebug(
        "相关性矩阵已生成",
        shape=corr_matrix_df.shape,
        head=corr_matrix_df.head().to_dict(),
        emoji="🧮",
    )

    least_relevant_set: Set[str] = set()
    corr: float = 0.0
    least_relevant_set, corr = await correlation_manager.find_least_relavant_submatrix(
        correlation_matrix=corr_matrix_df,
        submatrix_size=10,
        max_matrix_size=10000,
        max_workers=10,
    )

    await logger.ainfo(
        "最不相关子矩阵已找到",
        least_relevant_set=least_relevant_set,
        least_relevant_set_size=len(least_relevant_set),
        correlation=corr,
        emoji="🔍",
    )

    alpha_properties_payload: AlphaPropertiesPayload
    for alpha in alphas:
        if alpha.alpha_id in least_relevant_set:
            await logger.ainfo(
                "最不相关子矩阵中的 Alpha",
                alpha=alpha,
                emoji="🔍",
            )
            # 确保 tags 唯一，避免重复添加 POWER_POOL_TAG
            if POWER_POOL_TAG not in alpha.tags:
                alpha.tags = list(set(alpha.tags)) + [POWER_POOL_TAG]  # type: ignore
                alpha_properties_payload = AlphaPropertiesPayload(
                    tags=alpha.tags,
                )
                await client.update_alpha_properties(
                    alpha.alpha_id, alpha_properties_payload
                )
        else:
            # 确保 tags 唯一，避免重复添加 POWER_POOL_TAG
            if POWER_POOL_TAG in alpha.tags:
                await logger.ainfo(
                    "最不相关子矩阵外的 Alpha",
                    tags=alpha.tags,
                    emoji="🔍",
                )

                # 删除 POWER_POOL_TAG
                alpha.tags = [tag for tag in alpha.tags if tag != POWER_POOL_TAG]  # type: ignore
                alpha_properties_payload = AlphaPropertiesPayload(
                    tags=alpha.tags,
                )
                await client.update_alpha_properties(
                    alpha.alpha_id, alpha_properties_payload
                )

    async with session_manager.get_session(Database.ALPHAS) as session, session.begin():
        for alpha in alphas:
            if session.is_modified(alpha):
                await alpha_dal.update(alpha, session=session)

    await logger.ainfo(
        "Alpha 属性已更新",
        alpha_ids=alpha_ids,
        emoji="✅",
    )

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
