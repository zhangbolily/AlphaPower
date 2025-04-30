# 不要自动补全 docstring

import asyncio
from datetime import timedelta
from typing import Optional

import pandas as pd
from structlog.stdlib import BoundLogger

from alphapower.client import WorldQuantClient
from alphapower.client.common_view import TableView
from alphapower.constants import Database, RecordSetType
from alphapower.dal.evaluate import RecordSetDAL
from alphapower.dal.session_manager import session_manager
from alphapower.entity.alphas import Alpha
from alphapower.entity.evaluate import RecordSet
from alphapower.internal.logging import get_logger


class RecordSetsManager:
    def __init__(
        self,
        client: WorldQuantClient,
        record_set_dal: RecordSetDAL,
    ) -> None:
        self.client: WorldQuantClient = client
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
        # ⚡️DEBUG: 记录函数入参
        await self.log.adebug(
            "开始从平台获取记录集",
            alpha_id=alpha.alpha_id,
            set_type=set_type,
            emoji="🔄",
        )
        try:
            async with self.client as client:
                record_sets_view: Optional[TableView] = None
                finished: bool = False
                retry_after: float = 0.0
                timeout: float = 30.0  # 超时时间 30 秒
                start_time: float = asyncio.get_event_loop().time()
                alpha_id: str = alpha.alpha_id

                while not finished:
                    elapsed_time: float = asyncio.get_event_loop().time() - start_time
                    if elapsed_time > timeout:
                        await self.log.aerror(
                            "加载记录集超时",
                            alpha_id=alpha_id,
                            set_type=set_type,
                            timeout=timeout,
                            emoji="⏰",
                        )
                        raise TimeoutError(
                            f"加载记录集超时，alpha_id: {alpha_id}, set_type: {set_type}"
                        )

                    finished, record_sets_view, retry_after, _ = (
                        await client.alpha_fetch_record_sets(
                            alpha_id=alpha_id, record_type=set_type
                        )
                    )

                    if not finished:
                        await self.log.ainfo(
                            "记录集数据加载中，等待重试",
                            alpha_id=alpha_id,
                            set_type=set_type,
                            retry_after=retry_after,
                            emoji="⏳",
                        )
                        await asyncio.sleep(retry_after)

            if record_sets_view is None:
                await self.log.aerror(
                    "平台返回的记录集数据为 None",
                    alpha_id=alpha_id,
                    set_type=set_type,
                    emoji="❌",
                )
                raise ValueError(
                    f"平台返回的记录集数据为 None，alpha_id: {alpha_id}, set_type: {set_type}"
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
