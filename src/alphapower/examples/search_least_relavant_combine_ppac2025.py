from typing import Dict, List, Set

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import wq_client
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import Database, RecordSetType, TagType
from alphapower.dal import alpha_dal, evaluate_record_dal, record_set_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import EvaluateRecord
from alphapower.internal.decorator import async_timed
from alphapower.internal.logging import get_logger
from alphapower.manager.correlation_manager import CorrelationManager
from alphapower.manager.record_sets_manager import RecordSetsManager
from alphapower.settings import settings
from alphapower.view.alpha import CreateTagsPayload, ListTagAlphaView

logger: BoundLogger = get_logger(__name__)
client: WorldQuantBrainClient = WorldQuantBrainClient(
    username=settings.credential.username,
    password=settings.credential.password,
)


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
            Alpha.alpha_id.in_(alpha_ids),
            session=session,
        )
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
        for alpha in alphas:
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

    payload: CreateTagsPayload = CreateTagsPayload(
        name="least_relevant_set",
        type=TagType.LIST,
        alphas=list(least_relevant_set),
    )
    list_tag_alpha_view: ListTagAlphaView = await client.create_alpha_list(
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
