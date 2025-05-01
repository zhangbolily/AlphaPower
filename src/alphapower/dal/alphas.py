"""
Alpha数据访问层模块
提供对Alpha模型及其相关实体的数据访问操作。
"""

from typing import List, Optional, Type

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import Select

from alphapower.constants import Stage, Status
from alphapower.dal.base import EntityDAL
from alphapower.entity.alphas import (
    AggregateData,
    Alpha,
    Competition,
)


class AlphaDAL(EntityDAL[Alpha]):
    """
    Alpha 数据访问层类，提供对 Alpha 实体的特定操作。

    实现了Alpha实体的查询和管理功能，包括按ID、作者、状态等多种方式查询。
    """

    entity_class: Type[Alpha] = Alpha

    async def find_by_alpha_id(
        self, alpha_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Alpha]:
        """
        通过 alpha_id 查询 Alpha 实体。

        Args:
            alpha_id: Alpha的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的Alpha实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, alpha_id=alpha_id)

    async def find_by_author(
        self, author: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询指定作者的所有 Alpha。

        Args:
            author: 作者名称或标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            该作者创建的所有Alpha列表。
        """
        return await self.deprecated_find_by(session=session, author=author)

    async def find_by_status(
        self, status: Status, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询指定状态的所有 Alpha。

        Args:
            status: Alpha的状态值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定状态的所有Alpha列表。
        """
        return await self.deprecated_find_by(session=session, status=status)

    async def find_by_stage(
        self, stage: Stage, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询指定阶段的所有 Alpha。

        Args:
            stage: Alpha的阶段值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            指定阶段的所有Alpha列表。
        """
        return await self.deprecated_find_by(session=session, stage=stage)

    async def find_favorites(
        self, author: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询指定作者的收藏 Alpha。

        Args:
            author: 作者名称或标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            该作者收藏的所有Alpha列表。
        """
        return await self.deprecated_find_by(session=session, author=author, favorite=True)

    async def upsert(
        self,
        entity: Alpha,
        session: Optional[AsyncSession] = None,
    ) -> Alpha:
        """
        合并 Alpha 实体。

        如果实体已存在，则更新其信息；否则，创建新的实体。

        Args:
            entity: 要合并的 Alpha 实体。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            合并后的 Alpha 实体。
        """
        actual_session: AsyncSession = self._actual_session(session)

        # ⚡️ 日志：合并 Alpha 实体，调试级别，输出关键参数
        await self.log.adebug(
            event=f"{self.__class__.__name__}.upsert",
            alpha_id=entity.alpha_id,
            author=entity.author,
            status=entity.status,
            stage=entity.stage,
            session_id=actual_session.info.get("session_id", None),
            message="合并 Alpha 实体，准备写入数据库。",
            emoji="📝",
        )

        try:
            existing_entity = await self.find_by_alpha_id(
                entity.alpha_id, session=actual_session
            )

            if existing_entity:
                await self._update_entity_references(existing_entity, entity)
                entity = await actual_session.merge(entity)
            else:
                actual_session.add(entity)
            await actual_session.flush()
            return entity
        except Exception as e:
            # ⚡️ 日志：合并 Alpha 实体失败，错误级别，输出异常信息
            await self.log.aerror(
                event=f"{self.__class__.__name__}.upsert",
                session_id=actual_session.info.get("session_id", None),
                message=f"合并 Alpha 实体失败：{str(e)}",
                emoji="❌",
            )
            raise e

    async def bulk_upsert(
        self, entities: List[Alpha], session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        批量合并 Alpha 实体。

        如果实体已存在，则更新其信息；否则，创建新的实体。

        Args:
            entities: 要合并的 Alpha 实体列表。

        Returns:
            合并后的 Alpha 实体列表。
        """
        return [await self.upsert(entity, session=session) for entity in entities]

    async def bulk_upsert_by_unique_key(
        self,
        entities: List[Alpha],
        unique_key: str,
        session: Optional[AsyncSession] = None,
    ) -> List[Alpha]:
        """
        批量合并 Alpha 实体。

        根据唯一键更新或插入多个 Alpha 实体。

        Args:
            entities: 要合并的 Alpha 实体列表。
            unique_key: 唯一键字段名称。

        Returns:
            合并后的 Alpha 实体列表。
        """
        actual_session: AsyncSession = self._actual_session(session)

        for entity in entities:
            unique_value = getattr(entity, unique_key)
            existing_entity = await self.find_one_by(
                session=session, **{unique_key: unique_value}
            )
            if existing_entity:
                await self._update_entity_references(existing_entity, entity)
                await actual_session.merge(entity)
            else:
                actual_session.add(entity)
        await actual_session.flush()
        return entities

    async def _update_entity_references(
        self, existing_entity: Alpha, new_entity: Alpha
    ) -> None:
        """
        更新实体的引用字段。

        Args:
            existing_entity: 数据库中已存在的实体。
            new_entity: 新的实体数据。
        """

        # 更新引用字段的 ID
        if existing_entity.id:
            new_entity.id = existing_entity.id
        if existing_entity.in_sample_id and new_entity.in_sample:
            new_entity.in_sample.id = existing_entity.in_sample_id
        if existing_entity.out_sample_id and new_entity.out_sample:
            new_entity.out_sample.id = existing_entity.out_sample_id
        if existing_entity.test_id and new_entity.test:
            new_entity.test.id = existing_entity.test_id
        if existing_entity.train_id and new_entity.train:
            new_entity.train.id = existing_entity.train_id
        if existing_entity.prod_id and new_entity.prod:
            new_entity.prod.id = existing_entity.prod_id


class CompetitionDAL(EntityDAL[Competition]):
    """
    Competition 数据访问层类，提供对 Competition 实体的特定操作。

    管理比赛相关的数据访问，支持通过ID查询比赛信息。
    """

    entity_class: Type[Competition] = Competition

    async def find_by_competition_id(
        self, competition_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Competition]:
        """
        通过 competition_id 查询比赛。

        Args:
            competition_id: 比赛的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的比赛实体，若不存在则返回None。
        """
        return await self.find_one_by(session=session, competition_id=competition_id)


class AggregateDataDAL(EntityDAL[AggregateData]):
    """
    Sample 数据访问层类，提供对 Sample 实体的特定操作。

    管理样本数据的访问，包括性能指标分析和查询。
    """

    entity_class: Type[AggregateData] = AggregateData

    async def find_by_performance(
        self, min_sharpe: float, session: Optional[AsyncSession] = None
    ) -> List[AggregateData]:
        """
        查询 sharpe 比率大于指定值的所有样本。

        此方法用于性能分析，找出达到特定夏普比率阈值的样本。

        Args:
            min_sharpe: 最小的夏普比率阈值。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的样本列表。
        """
        actual_session: AsyncSession = self._actual_session(session)
        query: Select = select(AggregateData).where(AggregateData.sharpe >= min_sharpe)
        result = await actual_session.execute(query)
        return list(result.scalars().all())
