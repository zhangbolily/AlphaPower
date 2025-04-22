from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import Select, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import compiler

from alphapower.constants import AlphaType
from alphapower.dal.alphas import AggregateDataDAL, AlphaDAL, SettingDAL
from alphapower.engine.evaluate.base_alpha_fetcher import BaseAlphaFetcher
from alphapower.entity import Alpha

"""测试 Alpha 数据获取器基础实现 `BaseAlphaFetcher`。"""

# pylint: disable=W0212


@pytest.fixture(name="mock_alpha_dal")
def fixture_mock_alpha_dal() -> MagicMock:
    """创建 AlphaDAL 的模拟对象。"""
    mock = MagicMock(spec=AlphaDAL)
    mock.stream = AsyncMock()
    mock.count = AsyncMock()
    return mock


@pytest.fixture(name="mock_sample_dal")
def fixture_mock_sample_dal() -> MagicMock:
    """创建 SampleDAL 的模拟对象。"""
    return MagicMock(spec=AggregateDataDAL)


@pytest.fixture(name="mock_setting_dal")
def fixture_mock_setting_dal() -> MagicMock:
    """创建 SettingDAL 的模拟对象。"""
    return MagicMock(spec=SettingDAL)


@pytest.fixture(name="base_fetcher")
def fixture_base_fetcher(
    mock_alpha_dal: MagicMock,
    mock_sample_dal: MagicMock,
    mock_setting_dal: MagicMock,
) -> BaseAlphaFetcher:
    """创建 BaseAlphaFetcher 的测试实例。"""
    return BaseAlphaFetcher(mock_alpha_dal, mock_sample_dal, mock_setting_dal)


@pytest.fixture(name="mock_alpha_list")
def fixture_mock_alpha_list() -> list[Alpha]:
    """创建 Alpha 对象的模拟列表。"""
    # 创建一些 Alpha 实例用于测试 stream
    alpha1 = Alpha(id=1, name="alpha1", type=AlphaType.REGULAR)
    alpha2 = Alpha(id=2, name="alpha2", type=AlphaType.SUPER)
    return [alpha1, alpha2]


class TestBaseAlphaFetcher:
    """测试 BaseAlphaFetcher 类。"""

    def test_initialization(self, base_fetcher: BaseAlphaFetcher) -> None:
        """测试 BaseAlphaFetcher 的初始化。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        assert isinstance(base_fetcher.alpha_dal, MagicMock)
        assert isinstance(base_fetcher.sample_dal, MagicMock)
        assert isinstance(base_fetcher.setting_dal, MagicMock)
        assert base_fetcher._fetched_count == 0

    @pytest.mark.asyncio
    async def test_build_alpha_select_query_structure(
        self, base_fetcher: BaseAlphaFetcher
    ) -> None:
        """测试 _build_alpha_select_query 构建的查询逻辑（通过编译后的 SQL 片段）。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        query: Select = await base_fetcher._build_alpha_select_query()

        # 验证查询主体是 Alpha
        # assert query.selectable.entity_namespace is Alpha

        # 编译查询为字符串（使用特定方言以获得确定性输出）
        # 注意：这仍然可能因 SQLAlchemy 版本或方言实现而变化
        dialect = postgresql.dialect()
        statement_compiler = compiler.SQLCompiler(dialect, query)
        compiled_sql = statement_compiler.process(query, literal_binds=True)
        # 转换为小写以便不区分大小写比较
        compiled_sql_lower = compiled_sql.lower()

        # 验证 JOIN 子句（检查是否包含必要的表连接）
        assert "join settings on" in compiled_sql_lower
        assert "join samples on" in compiled_sql_lower

        # 验证 WHERE 子句包含关键条件（检查 SQL 片段）
        assert "samples.self_correration < 0.7" in compiled_sql_lower
        assert "samples.turnover > 0.01" in compiled_sql_lower
        assert "samples.turnover < 0.7" in compiled_sql_lower
        # 检查 CASE 语句是否存在于 WHERE 子句中
        assert "case when" in compiled_sql_lower
        # 检查 AND 连接符
        assert "and" in compiled_sql_lower  # 检查多个条件是否用 AND 连接

    @pytest.mark.asyncio
    async def test_fetch_alphas_success(
        self,
        base_fetcher: BaseAlphaFetcher,
        mock_alpha_dal: MagicMock,
        mock_alpha_list: list[Alpha],
    ) -> None:
        """测试 fetch_alphas 成功获取数据。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
            mock_alpha_dal: AlphaDAL 的模拟对象。
            mock_alpha_list: Alpha 对象的模拟列表。
        """
        # 模拟 _build_alpha_select_query 返回一个简单的 Select 对象
        mock_query = select(Alpha)
        with patch.object(
            base_fetcher,
            "_build_alpha_select_query",
            new_callable=AsyncMock,
            return_value=mock_query,
        ) as mock_build_query:

            # 配置模拟的 stream 方法返回异步生成器
            async def mock_stream_gen() -> AsyncGenerator[Alpha, None]:
                for alpha in mock_alpha_list:
                    yield alpha

            mock_alpha_dal.execute_stream_query.return_value = mock_stream_gen()

            fetched_alphas = []
            async for alpha in base_fetcher.fetch_alphas(some_arg="value"):
                fetched_alphas.append(alpha)

            # 验证 _build_alpha_select_query 被调用
            mock_build_query.assert_awaited_once_with(some_arg="value")
            # 验证 alpha_dal.stream 被调用
            mock_alpha_dal.execute_stream_query.assert_called_once()
            # 验证返回的 Alpha 列表
            assert fetched_alphas == mock_alpha_list
            # 验证 _fetched_count 被更新
            assert base_fetcher._fetched_count == len(mock_alpha_list)

    @pytest.mark.asyncio
    async def test_fetch_alphas_exception(
        self, base_fetcher: BaseAlphaFetcher, mock_alpha_dal: MagicMock
    ) -> None:
        """测试 fetch_alphas 在 DAL 层发生异常。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
            mock_alpha_dal: AlphaDAL 的模拟对象。
        """
        mock_query = select(Alpha)
        with patch.object(
            base_fetcher,
            "_build_alpha_select_query",
            new_callable=AsyncMock,
            return_value=mock_query,
        ):
            # 配置模拟的 stream 方法抛出异常
            mock_alpha_dal.execute_stream_query.side_effect = Exception(
                "数据库连接失败"
            )

            with pytest.raises(Exception, match="数据库连接失败"):
                async for _ in base_fetcher.fetch_alphas():
                    pass  # pragma: no cover

            # 验证 _fetched_count 未被错误更新
            assert base_fetcher._fetched_count == 0

    @pytest.mark.asyncio
    async def test_fetched_alpha_count(self, base_fetcher: BaseAlphaFetcher) -> None:
        """测试 fetched_alpha_count 返回正确的计数值。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        # 手动设置内部计数器
        base_fetcher._fetched_count = 15
        count = await base_fetcher.fetched_alpha_count()
        assert count == 15

        # 再次设置
        base_fetcher._fetched_count = 0
        count = await base_fetcher.fetched_alpha_count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_remaining_alpha_count_success(
        self, base_fetcher: BaseAlphaFetcher
    ) -> None:
        """测试 remaining_alpha_count 计算剩余数量。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        total = 50
        fetched = 20
        expected_remaining = total - fetched

        # 使用 patch.object 模拟 total_alpha_count 和 fetched_alpha_count
        with (
            patch.object(
                base_fetcher,
                "total_alpha_count",
                new_callable=AsyncMock,
                return_value=total,
            ) as mock_total,
            patch.object(
                base_fetcher,
                "fetched_alpha_count",
                new_callable=AsyncMock,
                return_value=fetched,
            ) as mock_fetched,
        ):
            remaining = await base_fetcher.remaining_alpha_count(arg1="test")

            # 验证模拟方法被调用
            mock_total.assert_awaited_once_with(arg1="test")
            mock_fetched.assert_awaited_once_with(arg1="test")
            # 验证返回的剩余数量
            assert remaining == expected_remaining

    @pytest.mark.asyncio
    async def test_remaining_alpha_count_total_exception(
        self, base_fetcher: BaseAlphaFetcher
    ) -> None:
        """测试 remaining_alpha_count 在 total_alpha_count 发生异常。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        # 模拟 total_alpha_count 抛出异常
        with (
            patch.object(
                base_fetcher,
                "total_alpha_count",
                new_callable=AsyncMock,
                side_effect=Exception("获取总数失败"),
            ) as mock_total,
            patch.object(
                base_fetcher, "fetched_alpha_count", new_callable=AsyncMock
            ) as mock_fetched,
        ):
            with pytest.raises(Exception, match="获取总数失败"):
                await base_fetcher.remaining_alpha_count()

            # 验证 total_alpha_count 被调用
            mock_total.assert_awaited_once()
            # 验证 fetched_alpha_count 未被调用
            mock_fetched.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_remaining_alpha_count_fetched_exception(
        self, base_fetcher: BaseAlphaFetcher
    ) -> None:
        """测试 remaining_alpha_count 在 fetched_alpha_count 发生异常。

        Args:
            base_fetcher: BaseAlphaFetcher 的测试实例。
        """
        total = 50
        # 模拟 fetched_alpha_count 抛出异常
        with (
            patch.object(
                base_fetcher,
                "total_alpha_count",
                new_callable=AsyncMock,
                return_value=total,
            ) as mock_total,
            patch.object(
                base_fetcher,
                "fetched_alpha_count",
                new_callable=AsyncMock,
                side_effect=Exception("获取已获取数量失败"),
            ) as mock_fetched,
        ):
            with pytest.raises(Exception, match="获取已获取数量失败"):
                await base_fetcher.remaining_alpha_count()

            # 验证 total_alpha_count 被调用
            mock_total.assert_awaited_once()
            # 验证 fetched_alpha_count 被调用
            mock_fetched.assert_awaited_once()
