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
    Classification,
    Competition,
    Expression,
    Setting,
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
        return await self.find_by(session=session, author=author)

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
        return await self.find_by(session=session, status=status)

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
        return await self.find_by(session=session, stage=stage)

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
        return await self.find_by(session=session, author=author, favorite=True)

    async def find_by_classification(
        self, classification_id: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询属于特定分类的所有 Alpha。

        通过分类ID查找所有关联的Alpha模型，使用连接查询实现。

        Args:
            classification_id: 分类 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的 Alpha 列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = (
            select(Alpha)
            .join(Alpha.classifications)
            .where(Classification.classification_id == classification_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

    async def find_by_competition(
        self, competition_id: str, session: Optional[AsyncSession] = None
    ) -> List[Alpha]:
        """
        查询参与特定比赛的所有 Alpha。

        通过比赛ID查找所有参与的Alpha模型，使用连接查询实现。

        Args:
            competition_id: 比赛 ID 字符串。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的 Alpha 列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = (
            select(Alpha)
            .join(Alpha.competitions)
            .where(Competition.competition_id == competition_id)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())

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
        actual_session: AsyncSession = session or self.session
        existing_entity = await self.find_by_alpha_id(
            entity.alpha_id, session=actual_session
        )
        if existing_entity:
            await self._update_entity_references(existing_entity, entity)
            entity = await actual_session.merge(entity)
        else:
            actual_session.add(entity)
        return entity

    async def bulk_upsert(self, entities: List[Alpha]) -> List[Alpha]:
        """
        批量合并 Alpha 实体。

        如果实体已存在，则更新其信息；否则，创建新的实体。

        Args:
            entities: 要合并的 Alpha 实体列表。

        Returns:
            合并后的 Alpha 实体列表。
        """
        return [await self.upsert(entity) for entity in entities]

    async def bulk_upsert_by_unique_key(
        self, entities: List[Alpha], unique_key: str
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
        for entity in entities:
            unique_value = getattr(entity, unique_key)
            existing_entity = await self.find_one_by(
                session=self.session, **{unique_key: unique_value}
            )
            if existing_entity:
                await self._update_entity_references(existing_entity, entity)
                await self.session.merge(entity)
            else:
                self.session.add(entity)
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
        if existing_entity.regular_id and new_entity.regular:
            new_entity.regular.id = existing_entity.regular_id
        if existing_entity.combo_id and new_entity.combo:
            new_entity.combo.id = existing_entity.combo_id
        if existing_entity.selection_id and new_entity.selection:
            new_entity.selection.id = existing_entity.selection_id
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


class SettingDAL(EntityDAL[Setting]):
    """
    Setting 数据访问层类，提供对 Setting 实体的特定操作。

    管理Alpha相关的设置信息，支持通用的CRUD操作。
    """

    entity_class: Type[Setting] = Setting


class RegularDAL(EntityDAL[Expression]):
    """
    Regular 数据访问层类，提供对 Regular 实体的特定操作。

    管理Alpha规则相关的数据访问，包括规则查询和代码分析。
    """

    entity_class: Type[Expression] = Expression

    async def find_similar_code(
        self, code_fragment: str, session: Optional[AsyncSession] = None
    ) -> List[Expression]:
        """
        查询包含特定代码片段的所有规则。

        此方法用于代码相似性分析，可帮助识别重复或相似的规则代码。

        Args:
            code_fragment: 要搜索的代码片段。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            符合条件的规则列表。
        """
        actual_session: AsyncSession = session or self.session
        query: Select = select(Expression).where(
            Expression.code.contains(code_fragment)
        )
        result = await actual_session.execute(query)
        return list(result.scalars().all())


class ClassificationDAL(EntityDAL[Classification]):
    """
    Classification 数据访问层类，提供对 Classification 实体的特定操作。

    管理Alpha分类相关的数据访问，支持通过ID查询分类信息。
    """

    entity_class: Type[Classification] = Classification

    async def find_by_classification_id(
        self, classification_id: str, session: Optional[AsyncSession] = None
    ) -> Optional[Classification]:
        """
        通过 classification_id 查询分类。

        Args:
            classification_id: 分类的唯一标识符。
            session: 可选的会话对象，若提供则优先使用。

        Returns:
            找到的分类实体，若不存在则返回None。
        """
        return await self.find_one_by(
            session=session, classification_id=classification_id
        )


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


class SampleDAL(EntityDAL[AggregateData]):
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
        actual_session: AsyncSession = session or self.session
        query: Select = select(AggregateData).where(AggregateData.sharpe >= min_sharpe)
        result = await actual_session.execute(query)
        return list(result.scalars().all())
