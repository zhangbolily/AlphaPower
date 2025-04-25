from typing import Dict, List

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import WorldQuantClient
from alphapower.constants import Database, Status
from alphapower.dal.alphas import AlphaDAL
from alphapower.dal.evaluate import CorrelationDAL, RecordSetDAL
from alphapower.entity.alphas import Alpha
from alphapower.internal.logging import get_logger

from .correlation_calculator import CorrelationCalculator


class CorrelationMatrix(CorrelationCalculator):

    def __init__(
        self,
        client: WorldQuantClient,
        alpha_dal: AlphaDAL,
        record_set_dal: RecordSetDAL,
        correlation_dal: CorrelationDAL,
    ) -> None:
        """
        Initialize the CorrelationMatrix class.

        Args:
            client (WorldQuantClient): Client for WorldQuant API.
            alpha_stream (AsyncGenerator[Alpha, None]): Stream of Alpha objects.
            alpha_dal (AlphaDAL): Data Access Layer for Alpha.
            record_set_dal (RecordSetDAL): Data Access Layer for RecordSet.
            correlation_dal (CorrelationDAL): Data Access Layer for Correlation.
            multiprocess (bool, optional): Flag to enable multiprocessing. Defaults to False.
        """
        super().__init__(
            client=client,
            alpha_stream=None,
            alpha_dal=alpha_dal,
            record_set_dal=record_set_dal,
            correlation_dal=correlation_dal,
            multiprocess=False,
        )

        self.log: BoundLogger = get_logger(module_name=self.__class__.__name__)

    async def generate(
        self,
        alpha_list: List[Alpha],
    ) -> pd.DataFrame:
        alpha_df_list: List[pd.DataFrame] = []
        corr_matrix: pd.DataFrame = pd.DataFrame()
        alpha_id_map: Dict[str, Alpha] = {}

        for alpha_x in alpha_list:
            try:
                pnl_diff_df: pd.DataFrame = await self._get_pnl_dataframe(
                    alpha_id=alpha_x.alpha_id,
                    force_refresh=False,
                )

                alpha_df_list.append(
                    pnl_diff_df.rename(columns={"pnl": alpha_x.alpha_id})
                )
                alpha_id_map[alpha_x.alpha_id] = alpha_x
            except Exception as e:
                await self.log.aerror(
                    "Error in generating correlation matrix",
                    alpha_id=alpha_x.alpha_id,
                    error=str(e),
                )
            finally:
                await self.log.ainfo(
                    "Finished processing alpha",
                    alpha_id=alpha_x.alpha_id,
                )

        if not alpha_df_list:
            await self.log.ainfo("No alpha data available for correlation matrix.")
            return corr_matrix

        # Merge all dataframes on 'date' column
        merged_df: pd.DataFrame = pd.concat(alpha_df_list, axis=1)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        merged_df = merged_df.dropna()

        # Calculate the correlation matrix
        corr_matrix = merged_df.corr()
        # 列名排序
        corr_matrix = corr_matrix.reindex(sorted(corr_matrix.columns), axis=1)
        corr_matrix = corr_matrix.reindex(sorted(corr_matrix.index), axis=0)

        return corr_matrix


if __name__ == "__main__":
    import asyncio

    from alphapower.client import wq_client
    from alphapower.dal.session_manager import session_manager

    async def test() -> None:
        alpha_dal = AlphaDAL()
        record_set_dal = RecordSetDAL()
        correlation_dal = CorrelationDAL()

        async with wq_client as client:
            correlation_matrix = CorrelationMatrix(
                client=client,
                alpha_dal=alpha_dal,
                record_set_dal=record_set_dal,
                correlation_dal=correlation_dal,
            )

            alpha_list: List[Alpha] = []

            async with session_manager.get_session(Database.ALPHAS) as session:
                for alpha in await alpha_dal.find_by_status(
                    session=session, status=Status.ACTIVE
                ):
                    for classification in alpha.classifications:
                        if classification.id == "POWER_POOL:POWER_POOL_ELIGIBLE":
                            alpha_list.append(alpha)

            result: pd.DataFrame = await correlation_matrix.generate(alpha_list)
            print(result)

    asyncio.run(test())
