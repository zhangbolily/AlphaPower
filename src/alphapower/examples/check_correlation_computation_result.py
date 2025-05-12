import asyncio
from typing import Dict, List, Optional

import pandas as pd

from alphapower.client.common_view import RecordSetTimeSeriesView, TableView
from alphapower.client.worldquant_brain_client import WorldQuantBrainClient
from alphapower.constants import CorrelationCalcType, Database, RecordSetType, Stage
from alphapower.dal import alpha_dal, record_set_dal
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import Alpha
from alphapower.internal.decorator import async_exception_handler
from alphapower.manager.correlation_manager import CorrelationManager
from alphapower.manager.record_sets_manager import RecordSetsManager
from alphapower.settings import settings
from alphapower.view.alpha import SelfCorrelationView

brain_client: WorldQuantBrainClient = WorldQuantBrainClient(
    username=settings.credential.username,
    password=settings.credential.password,
)

record_sets_manager = RecordSetsManager(
    brain_client=brain_client,
    record_set_dal=record_set_dal,
)

correlation_manager = CorrelationManager(
    brain_client=brain_client,
)

target_alpha_id: str = "ZANArd8"


@async_exception_handler
async def main() -> None:
    correlation_data: TableView = await correlation_manager.get_correlation_platform(
        target_alpha_id=target_alpha_id,
        corr_type=CorrelationCalcType.PLATFORM_SELF,
    )

    correlation_view: SelfCorrelationView = (
        await correlation_manager.build_self_correlation_from_table(
            target_alpha_id=target_alpha_id,
            corr_table_data=correlation_data,
        )
    )

    pnl_series_map: Dict[Alpha, RecordSetTimeSeriesView] = {}
    alpha_corr_map: Dict[Alpha, float] = {}
    alpha_map: Dict[str, Alpha] = {}
    usa_ppac_alphas: List[Alpha] = []

    async with session_manager.get_session(Database.ALPHAS) as session:
        os_alphas: List[Alpha] = await alpha_dal.find_by_stage(
            session=session,
            stage=Stage.OS,
        )

        for os_alpha in os_alphas:
            for classification in os_alpha.classifications:
                if classification.id == "POWER_POOL:POWER_POOL_ELIGIBLE":
                    # 仅处理 Power Pool 的因子
                    usa_ppac_alphas.append(os_alpha)
                    break

        for correlation_item in correlation_view.correlations + [
            SelfCorrelationView.CorrelationItem(
                alpha_id=target_alpha_id, correlation=1.0
            )
        ]:
            alpha: Optional[Alpha] = await alpha_dal.find_one_by(
                alpha_id=correlation_item.alpha_id,
                session=session,
            )

            if alpha is None:
                print(f"Alpha with ID {correlation_item.alpha_id} not found.")
                continue

            alpha_map[alpha.alpha_id] = alpha
            alpha_corr_map[alpha] = correlation_item.correlation

            pnl_df: pd.DataFrame = await record_sets_manager.get_record_sets(
                alpha=alpha,
                set_type=RecordSetType.PNL,
                allow_local=True,
            )

            pnl_series_view: RecordSetTimeSeriesView = (
                await record_sets_manager.build_time_series(
                    alpha=alpha,
                    set_type=RecordSetType.PNL,
                    data=pnl_df,
                )
            )

            pnl_series_map[alpha] = pnl_series_view
            print(f"Alpha ID: {alpha.alpha_id}, PnL Series: {pnl_series_view.series}")

    alpha_self_corr_data_map: Dict[Alpha, pd.Series] = {}
    for alpha, pnl_series_view in pnl_series_map.items():
        series: pd.Series = pnl_series_view.series.diff()
        four_years: pd.DateOffset = pd.DateOffset(years=4)
        recent_4_years: pd.Series = series.last(four_years)
        alpha_self_corr_data_map[alpha] = recent_4_years

    target_alpha: Optional[Alpha] = alpha_map.get(target_alpha_id)
    if target_alpha is None:
        print(f"Target alpha with ID {target_alpha_id} not found.")
        return
    target_4_years: pd.Series = alpha_self_corr_data_map.get(
        target_alpha, pd.Series(dtype=float)
    )

    corr_result: Dict[Alpha, float] = (
        await correlation_manager.calculate_correlations_with(
            target_series=target_4_years,
            others_series_dict=alpha_self_corr_data_map,
        )
    )

    for alpha, correlation in corr_result.items():
        if alpha.alpha_id == target_alpha_id:
            continue

        platform_corr: float = alpha_corr_map.get(alpha, 0.0)
        if correlation != platform_corr:
            print(
                f"Alpha ID: {alpha.alpha_id}, "
                f"Calculated Correlation: {correlation}, "
                f"Platform Correlation: {platform_corr}"
            )

    # 计算 ppac 的相关系数
    ppac_alpha_pnl_series_map: Dict[Alpha, pd.Series] = {}
    for alpha in usa_ppac_alphas + [target_alpha]:
        pnl_df = await record_sets_manager.get_record_sets(
            alpha=alpha,
            set_type=RecordSetType.PNL,
            allow_local=True,
        )

        pnl_series_view = await record_sets_manager.build_time_series(
            alpha=alpha,
            set_type=RecordSetType.PNL,
            data=pnl_df,
        )

        pnl_series = pnl_series_view.series.diff()
        four_years = pd.DateOffset(years=4)
        recent_4_years = pnl_series.last(four_years)
        ppac_alpha_pnl_series_map[alpha] = recent_4_years

    ppac_corr_result: Dict[Alpha, float] = (
        await correlation_manager.calculate_correlations_with(
            target_series=target_4_years,
            others_series_dict=ppac_alpha_pnl_series_map,
        )
    )
    for alpha, correlation in ppac_corr_result.items():
        if alpha.alpha_id == target_alpha_id:
            continue

        platform_corr = alpha_corr_map.get(alpha, 0.0)
        if correlation != platform_corr:
            print(
                f"Alpha ID: {alpha.alpha_id}, "
                f"Calculated Correlation: {correlation}, "
                f"Platform Correlation: {platform_corr}"
            )


if __name__ == "__main__":
    asyncio.run(main())
