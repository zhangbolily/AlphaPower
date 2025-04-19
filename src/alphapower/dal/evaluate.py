from typing import List, Optional, Type

from sqlalchemy import and_, func, or_, select

from alphapower.constants import CheckRecordType
from alphapower.dal.base import EntityDAL
from alphapower.entity import CheckRecord, Correlation, RecordSet


class CorrelationDAL(EntityDAL[Correlation]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[Correlation] = Correlation

    async def bulk_upsert(self, entities: List[Correlation]) -> List[Correlation]:
        """
        批量插入或更新实体对象。

        Correlation 批量更新唯一索引为 alpha_pair + calc_type

        Args:
            entities: 实体对象列表。
        Returns:
            插入或更新后的实体对象列表。
        """
        if not entities:
            return []

        merged_entities: List[Correlation] = []
        for entity in entities:
            exist_entity: Optional[Correlation] = await self.find_one_by(
                alpha_id_a=entity.alpha_id_a,
                alpha_id_b=entity.alpha_id_b,
                calc_type=entity.calc_type,
            )
            if exist_entity:
                # 正确处理 merge 的返回值
                merged_entity: Correlation = await self.session.merge(entity)
                merged_entities.append(merged_entity)
            else:
                self.session.add(entity)
                merged_entities.append(entity)  # 添加新创建的实体
        # await self.session.commit() # 不应在此处提交，由调用者管理事务
        await self.session.flush()  # 刷新以获取 ID 等信息
        return merged_entities

    async def get_latest_max_corr(
        self,
        alpha_id: str,
        calc_type: CheckRecordType,
    ) -> Optional[Correlation]:
        """
        获取指定 Alpha 对的最新相关性记录。
            calc_type: 计算类型 (Calculation Type)。

        Returns:
            最新的 Correlation 对象，如果未找到则返回 None。
        """

        await self.logger.adebug(
            "🔍 正在查询最新的相关性记录",
            alpha_id=alpha_id,
            calc_type=calc_type.value,
            emoji="🔍",
        )

        query = (
            select(self.entity_class)
            .where(
                and_(
                    or_(
                        self.entity_class.alpha_id_a == alpha_id,
                        self.entity_class.alpha_id_b == alpha_id,
                    ),
                    self.entity_class.calc_type == calc_type,
                    self.entity_class.correlation
                    == func.max(self.entity_class.correlation),
                )
            )
            .order_by(self.entity_class.created_at.desc())
        )
        result = await self.session.execute(query.limit(1))
        latest_record: Optional[Correlation] = result.scalars().first()

        await self.logger.adebug(
            "✅ 查询最新相关性记录完成",
            found=latest_record is not None,
            record_id=latest_record.id if latest_record else None,
            emoji="✅",
        )
        return latest_record


class CheckRecordDAL(EntityDAL[CheckRecord]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[CheckRecord] = CheckRecord


class RecordSetDAL(EntityDAL[RecordSet]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[RecordSet] = RecordSet
