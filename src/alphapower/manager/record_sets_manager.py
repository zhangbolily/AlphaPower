# 不要自动补全 docstring

from datetime import timedelta
from typing import Dict, Optional

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client.common_view import RecordSetTimeSeriesView, TableView
from alphapower.client.worldquant_brain_client_abc import AbstractWorldQuantBrainClient
from alphapower.constants import Database, LoggingEmoji, RecordSetType
from alphapower.dal.evaluate import RecordSetDAL
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import RecordSet
from alphapower.internal.decorator import async_exception_handler
from alphapower.internal.logging import get_logger


class RecordSetsManager:
    def __init__(
        self,
        brain_client: AbstractWorldQuantBrainClient,
        record_set_dal: RecordSetDAL,
    ) -> None:
        self.brain_client: AbstractWorldQuantBrainClient = brain_client
        self.record_set_dal: RecordSetDAL = record_set_dal
        self.log: BoundLogger = get_logger(
            module_name=f"{__name__}.{self.__class__.__name__}"
        )

    async def get_record_local(
        self,
        alpha: Alpha,
        set_type: RecordSetType,
        local_expire_time: Optional[timedelta] = None,
    ) -> pd.DataFrame:
        # ⚡️DEBUG: 记录函数入参
        await self.log.adebug(
            "开始获取本地记录集",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            emoji="🔍",
        )
        try:
            async with session_manager.get_session(
                Database.EVALUATE, readonly=True
            ) as session:
                record_set: Optional[RecordSet] = await self.record_set_dal.find_one_by(
                    session=session,
                    alpha_id=alpha.alpha_id,
                    set_type=set_type,
                    order_by=RecordSet.created_at.desc(),
                )
            # ⚡️DEBUG: 记录查询结果
            await self.log.adebug(
                "查询RecordSet结果",
                record_set_id=getattr(record_set, "id", None),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="📦",
            )

            if record_set is None:
                await self.log.aerror(
                    "未找到RecordSet",
                    alpha_id=alpha.alpha_id,
                    set_type=set_type,
                    emoji="❌",
                )
                raise LookupError(
                    f"未找到对应的RecordSet，alpha_id: {alpha.alpha_id}, set_type: {set_type}"
                )

            if record_set.created_at is None:
                await self.log.aerror(
                    "RecordSet创建时间为空",
                    record_set_id=record_set.id,
                    alpha_id=alpha.alpha_id,
                    set_type=set_type,
                    emoji="⚠️",
                )
                raise ValueError(
                    f"RecordSet创建时间为空，alpha_id: {alpha.alpha_id}, set_type: {set_type}"
                )
            if local_expire_time is not None:
                now: pd.Timestamp = pd.Timestamp.now()

                if record_set.created_at + local_expire_time < now:
                    await self.log.aerror(
                        "RecordSet已过期",
                        record_set_id=record_set.id,
                        alpha_id=alpha.alpha_id,
                        set_type=set_type,
                        local_expire_time=local_expire_time,
                        created_at=record_set.created_at,
                        current_time=now,
                        emoji="⏳",
                    )
                    raise LookupError(
                        f"RecordSet已过期，alpha_id: {alpha.alpha_id}, set_type: {set_type}"
                    )

            if record_set.content is None:
                await self.log.aerror(
                    "RecordSet内容为空",
                    record_set_id=record_set.id,
                    alpha_id=alpha.alpha_id,
                    set_type=set_type,
                    emoji="⚠️",
                )
                raise ValueError(
                    f"RecordSet内容为空，alpha_id: {alpha.alpha_id}, set_type: {set_type}"
                )

            record_set_df: pd.DataFrame = record_set.content.to_dataframe()
            # ⚡️DEBUG: 记录DataFrame基本信息
            await self.log.adebug(
                "RecordSet内容转换为DataFrame",
                shape=record_set_df.shape,
                columns=record_set_df.columns.tolist(),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="🧾",
            )
            if record_set_df.empty:
                await self.log.aerror(
                    "RecordSet DataFrame为空",
                    record_set_id=record_set.id,
                    alpha_id=alpha.alpha_id,
                    set_type=set_type,
                    emoji="🚫",
                )
                raise ValueError(
                    f"RecordSet DataFrame为空，alpha_id: {alpha.alpha_id}, set_type: {set_type}"
                )
            # ⚡️INFO: 成功返回DataFrame
            await self.log.ainfo(
                "成功获取本地记录集",
                record_set_id=record_set.id,
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="✅",
            )
            return record_set_df
        except (LookupError, ValueError) as e:
            # ⚡️WARNING: 业务异常已捕获并抛出
            await self.log.awarning(
                "业务异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            # ⚡️CRITICAL: 未知异常，打印完整堆栈
            await self.log.acritical(
                "获取本地记录集发生未知异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                exc_info=True,
                emoji="💥",
            )
            raise RuntimeError(
                f"获取本地记录集失败，alpha_id: {alpha.alpha_id}, set_type: {set_type}，原因: {e}"
            ) from e

    async def fetch_and_save_record_sets(
        self,
        alpha: Alpha,
        set_type: RecordSetType,
    ) -> pd.DataFrame:
        await self.log.ainfo(
            event=f"进入 {self.fetch_and_save_record_sets.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        # ⚡️DEBUG: 记录函数入参
        await self.log.adebug(
            "开始从平台获取记录集",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            emoji="🔄",
        )
        try:
            alpha_id: str = alpha.alpha_id

            record_sets_view: TableView = (
                await self.brain_client.fetch_alpha_record_sets(
                    alpha_id=alpha_id,
                    record_set_type=set_type,
                    override_retry_after=None,
                )
            )

            record_set_entity: RecordSet = RecordSet(
                alpha_id=alpha_id,
                set_type=set_type,
                content=record_sets_view,
            )

            async with (
                session_manager.get_session(Database.EVALUATE) as session,
                session.begin(),
            ):
                existing_record_set: Optional[RecordSet] = (
                    await self.record_set_dal.find_one_by(
                        alpha_id=alpha_id,
                        set_type=set_type,
                        session=session,
                    )
                )

                if existing_record_set is None:
                    await self.record_set_dal.create(
                        record_set_entity,
                        session=session,
                    )
                    await self.log.ainfo(
                        "新建记录集数据",
                        alpha_id=alpha_id,
                        set_type=set_type,
                        emoji="🆕",
                    )
                else:
                    record_set_entity.id = existing_record_set.id
                    await self.record_set_dal.update(
                        record_set_entity,
                        session=session,
                    )
                    await self.log.ainfo(
                        "更新已有记录集数据",
                        alpha_id=alpha_id,
                        set_type=set_type,
                        emoji="♻️",
                    )

            record_set_df: Optional[pd.DataFrame] = record_sets_view.to_dataframe()
            if record_set_df is None or record_set_df.empty:
                await self.log.aerror(
                    "记录集数据转换为 DataFrame 失败或内容为空",
                    alpha_id=alpha_id,
                    set_type=set_type,
                    emoji="❌",
                )
                raise ValueError(
                    f"记录集数据转换为 DataFrame 失败或内容为空，alpha_id: {alpha_id}, set_type: {set_type}"
                )

            await self.log.adebug(
                "成功从平台加载记录集数据",
                alpha_id=alpha_id,
                set_type=set_type,
                shape=record_set_df.shape,
                columns=record_set_df.columns.tolist(),
                emoji="✅",
            )
            return record_set_df
        except TimeoutError as e:
            await self.log.awarning(
                "获取记录集超时",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="⏰",
            )
            raise
        except ValueError as e:
            await self.log.awarning(
                "获取记录集发生业务异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.acritical(
                "获取记录集发生未知异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                exc_info=True,
                emoji="💥",
            )
            raise RuntimeError(
                f"获取记录集失败，alpha_id: {alpha.alpha_id}, set_type: {set_type}，原因: {e}"
            ) from e
        finally:
            await self.log.ainfo(
                event=f"退出 {self.fetch_and_save_record_sets.__qualname__} 方法",
                emoji=LoggingEmoji.STEP_OUT_FUNC.value,
            )

    async def get_record_sets(
        self,
        alpha: Alpha,
        set_type: RecordSetType,
        allow_local: bool = True,
        local_expire_time: Optional[timedelta] = None,
    ) -> pd.DataFrame:
        # ⚡️DEBUG: 记录函数入参
        await self.log.adebug(
            "开始获取记录集",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            allow_local=allow_local,
            emoji="🚀",
        )
        try:
            record_sets_df: pd.DataFrame

            if allow_local:
                try:
                    record_sets_df = await self.get_record_local(
                        alpha=alpha,
                        set_type=set_type,
                        local_expire_time=local_expire_time,
                    )
                    await self.log.ainfo(
                        "优先使用本地记录集",
                        alpha_id=alpha.alpha_id,
                        set_type=set_type,
                        emoji="📦",
                    )
                    return record_sets_df
                except (LookupError, ValueError) as e:
                    await self.log.awarning(
                        "获取本地记录集失败，尝试从平台获取",
                        alpha_id=alpha.alpha_id,
                        set_type=set_type,
                        error=str(e),
                        emoji="🔄",
                    )
            # 本地不可用或未找到，尝试从平台获取
            record_sets_df = await self.fetch_and_save_record_sets(
                alpha=alpha,
                set_type=set_type,
            )
            await self.log.ainfo(
                "成功获取平台记录集",
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="🌐",
            )
            return record_sets_df
        except TimeoutError as e:
            await self.log.awarning(
                "获取平台记录集超时",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="⏰",
            )
            raise
        except ValueError as e:
            await self.log.awarning(
                "获取平台记录集发生业务异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="⚠️",
            )
            raise
        except Exception as e:
            await self.log.acritical(
                "获取记录集发生未知异常",
                error=str(e),
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                exc_info=True,
                emoji="💥",
            )
            raise RuntimeError(
                f"获取记录集失败，alpha_id: {alpha.alpha_id}, set_type: {set_type}，原因: {e}"
            ) from e
        finally:
            await self.log.ainfo(
                "获取记录集流程结束",
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji="✅",
            )

    @async_exception_handler
    async def build_time_series(
        self,
        alpha: Alpha,
        set_type: RecordSetType,
        data: pd.DataFrame,
    ) -> RecordSetTimeSeriesView:
        await self.log.ainfo(
            event=f"进入 {self.build_time_series.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_IN_FUNC.value,
        )

        await self.log.adebug(
            "开始构建时间序列",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            data_shape=data.shape,
            emoji=LoggingEmoji.DEBUG.value,
        )

        if data.empty:
            await self.log.aerror(
                "数据为空，无法构建时间序列",
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError("数据为空，无法构建时间序列")

        column_map: Dict[RecordSetType, str] = {
            RecordSetType.PNL: "pnl",
            RecordSetType.DAILY_PNL: "daily-pnl",
            RecordSetType.SHARPE: "sharpe",
            RecordSetType.TURNOVER: "turnover",
            RecordSetType.YEARLY_STATS: "yearly-stats",
        }

        if set_type not in column_map:
            await self.log.aerror(
                "不支持的记录集类型",
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"不支持的记录集类型: {set_type}")

        column_name: str = column_map[set_type]
        if column_name not in data.columns:
            await self.log.aerror(
                "数据中缺少必要的列",
                alpha_id=alpha.alpha_id,
                set_type=set_type,
                missing_column=column_name,
                emoji=LoggingEmoji.ERROR.value,
            )
            raise ValueError(f"数据中缺少必要的列: {column_name}")

        # 构建时间序列
        data["date"] = pd.to_datetime(data["date"])
        time_series: pd.Series = data.set_index("date")[column_name]

        time_series_view: RecordSetTimeSeriesView = RecordSetTimeSeriesView(
            type=set_type,
            alpha_id=alpha.alpha_id,
            series=time_series,
        )

        await self.log.ainfo(
            "成功构建时间序列",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            series_shape=time_series.shape,
            emoji=LoggingEmoji.INFO.value,
        )

        await self.log.ainfo(
            event=f"退出 {self.build_time_series.__qualname__} 方法",
            emoji=LoggingEmoji.STEP_OUT_FUNC.value,
        )
        return time_series_view
