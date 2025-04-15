from typing import List, Type

from alphapower.dal.base import EntityDAL
from alphapower.entity.evaluate import CheckRecord, Correlation


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

        for entity in entities:
            exist_entity = await self.find_one_by(
                alpha_id_a=entity.alpha_id_a,
                alpha_id_b=entity.alpha_id_b,
                calc_type=entity.calc_type,
            )
            if exist_entity:
                entity = await self.session.merge(entity)
            else:
                self.session.add(entity)
        await self.session.commit()
        return entities


class CheckRecordDAL(EntityDAL[CheckRecord]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[CheckRecord] = CheckRecord
