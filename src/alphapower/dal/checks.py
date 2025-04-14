from typing import Type

from alphapower.dal.base import EntityDAL
from alphapower.entity.checks import CheckRecord


class CheckRecordDAL(EntityDAL[CheckRecord]):
    """
    Dataset 数据访问层类，提供对 Dataset 实体的特定操作。

    管理数据集的CRUD操作，支持按区域、价值、分类等多种方式查询数据集。
    """

    entity_class: Type[CheckRecord] = CheckRecord
