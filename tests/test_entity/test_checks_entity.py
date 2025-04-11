"""
测试模块：测试 alphapower.entity.checks 模块中的 Correlation ORM 模型类。

本模块的主要功能包括：
1. 测试 Correlation 类的初始化和属性赋值是否正确。
2. 测试 Correlation 类与数据库的交互功能是否正常。
"""

from datetime import datetime
from typing import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from alphapower.constants import Database
from alphapower.entity.checks import Correlation
from alphapower.internal.db_session import get_db_session


@pytest.fixture(name="async_session")
async def async_session_fixture() -> AsyncGenerator[AsyncSession, None]:
    """
    异步会话的 fixture，用于测试数据库交互。

    返回值:
        AsyncSession: 异步数据库会话实例。
    """
    # 这里应该是创建异步数据库会话的逻辑
    async with get_db_session(Database.CHECKS) as session:
        yield session


class TestCorrelation:
    """测试 Correlation ORM 模型类的单元测试类"""

    @pytest.fixture
    def correlation_data(self) -> dict:
        """
        生成测试用的 Correlation 数据。

        返回值:
            dict: 包含测试用的 Correlation 数据，包括 alpha_id、相关性范围、
                  表结构 (schema) 和记录 (records)。
        """
        return {
            "alpha_id": 1,
            "correlation_max": 0.95,
            "correlation_min": 0.1,
            "table_schema": {"columns": ["col1", "col2"]},
            "records": [{"col1": 1, "col2": 2}],
            "created_at": datetime.now(),
        }

    @pytest.mark.asyncio
    async def test_correlation_initialization(
        self, async_session: AsyncSession, correlation_data: dict
    ) -> None:
        """
        测试 Correlation 类的初始化和属性赋值。

        测试内容:
        1. 验证 Correlation 对象的属性是否正确赋值。
        2. 验证 Correlation 对象是否可以正确插入数据库。
        3. 验证从数据库中查询的 Correlation 对象是否与插入的数据一致。

        参数:
            async_session (AsyncSession): 异步数据库会话，用于测试数据库交互。
            correlation_data (dict): 测试用的 Correlation 数据。
        """
        correlation = Correlation(**correlation_data)

        # 验证属性是否正确赋值
        assert correlation.alpha_id == correlation_data["alpha_id"]
        assert correlation.correlation_max == correlation_data["correlation_max"]
        assert correlation.correlation_min == correlation_data["correlation_min"]
        assert correlation.table_schema == correlation_data["table_schema"]
        assert correlation.records == correlation_data["records"]
        assert isinstance(correlation.created_at, datetime)

        # 测试是否可以正确插入数据库
        async with async_session.begin():
            async_session.add(correlation)

        # 从数据库中查询并验证
        async with async_session.begin():
            result = await async_session.execute(select(Correlation))
            fetched_correlation = result.scalars().first()

        assert fetched_correlation is not None
        assert fetched_correlation.alpha_id == correlation_data["alpha_id"]
        assert (
            fetched_correlation.correlation_max == correlation_data["correlation_max"]
        )
        assert (
            fetched_correlation.correlation_min == correlation_data["correlation_min"]
        )
        assert fetched_correlation.table_schema == correlation_data["table_schema"]
        assert fetched_correlation.records == correlation_data["records"]
