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

    async def _print_correlation_matrix(
        self, alpha_corr_matrix: Dict[Alpha, Dict[Alpha, float]]
    ) -> None:
        """
        格式化打印相关性矩阵。

        Args:
            alpha_corr_matrix (Dict[Alpha, Dict[Alpha, float]]): 相关性矩阵。
        """
        alpha_ids = [alpha.alpha_id for alpha in alpha_corr_matrix.keys()]
        header = "Alpha ID".ljust(15) + "\t".join(alpha_ids)
        print(header)
        print("-" * len(header))

        for alpha_x, correlations in alpha_corr_matrix.items():
            row = alpha_x.alpha_id.ljust(15)
            for alpha_y in alpha_corr_matrix.keys():
                correlation = correlations.get(alpha_y, 0.0)
                row += f"\t{correlation:.2f}"
            print(row)

    async def generate(
        self,
        alpha_list: List[Alpha],
    ) -> Dict[Alpha, Dict[Alpha, float]]:
        alpha_df_map: Dict[Alpha, pd.DataFrame] = {}
        alpha_corr_matrix: Dict[Alpha, Dict[Alpha, float]] = {}

        for alpha_x in alpha_list:
            try:
                pnl_series_df: pd.DataFrame = await self._get_pnl_dataframe(
                    alpha_id=alpha_x.alpha_id,
                    force_refresh=False,
                )
                pnl_series_df = await self._validate_pnl_dataframe(
                    pnl_df=pnl_series_df, alpha_id=alpha_x.alpha_id
                )
                pnl_series_df = await self._prepare_pnl_dataframe(pnl_df=pnl_series_df)
                pnl_diff_series_df: pd.DataFrame = (
                    pnl_series_df - pnl_series_df.ffill().shift(1)
                )

                alpha_df_map[alpha_x] = pnl_diff_series_df
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

        for alpha_x, pnl_diff_series_x in alpha_df_map.items():
            if alpha_x not in alpha_corr_matrix:
                alpha_corr_matrix[alpha_x] = {}

            for alpha_y, pnl_diff_series_y in alpha_df_map.items():
                if alpha_y not in alpha_corr_matrix:
                    alpha_corr_matrix[alpha_y] = {}

                if alpha_x == alpha_y:
                    await self.log.adebug(
                        "Skipping self-correlation",
                        alpha_id=alpha_x.alpha_id,
                    )
                    alpha_corr_matrix[alpha_x][alpha_y] = 1.0

                if alpha_y in alpha_corr_matrix.get(alpha_x, {}):
                    await self.log.adebug(
                        "Already calculated correlation",
                        alpha_id_x=alpha_x.alpha_id,
                        alpha_id_y=alpha_y.alpha_id,
                    )
                    continue

                try:
                    correlation: float = pnl_diff_series_x.corrwith(
                        pnl_diff_series_y, axis=0
                    ).iloc[0]

                    if pd.isna(correlation):
                        await self.log.aerror(
                            "Correlation is NaN",
                            alpha_id_x=alpha_x.alpha_id,
                            alpha_id_y=alpha_y.alpha_id,
                        )
                        raise ValueError(
                            f"Correlation is NaN for alphas {alpha_x.alpha_id} and {alpha_y.alpha_id}"
                        )

                    alpha_corr_matrix[alpha_x][alpha_y] = correlation
                    alpha_corr_matrix[alpha_y][alpha_x] = correlation
                except Exception as e:
                    await self.log.aerror(
                        "Error in calculating correlation",
                        alpha_id=alpha_x.alpha_id,
                        error=str(e),
                    )

        await self._print_correlation_matrix(alpha_corr_matrix)
        return alpha_corr_matrix


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

            result: Dict[Alpha, Dict[Alpha, float]] = await correlation_matrix.generate(
                alpha_list
            )
            print(result)

    asyncio.run(test())
