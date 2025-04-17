"""
日志模块单元测试

本模块包含对日志设置功能的测试用例，确保日志配置能够正确处理控制台输出和文件输出。
测试覆盖了不同场景下的日志配置，包括控制台输出、日志目录创建和异步环境下的日志记录。
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest
from structlog.stdlib import BoundLogger

from alphapower.internal.logging import get_logger

# pylint: disable=W0613


class TestSetupLogging:
    """测试日志设置功能类

    该测试类验证日志设置函数的各种功能，包括：
    - 控制台日志输出配置
    - 文件日志输出配置
    - 日志目录创建
    - 异步环境下的日志记录
    """

    @pytest.fixture
    def temp_log_dir(self, tmp_path: Path) -> str:
        """创建临时日志目录

        Args:
            tmp_path: pytest 提供的临时路径对象

        Returns:
            创建的临时日志目录
        """
        log_dir: Path = tmp_path / "logs"
        log_dir.mkdir(exist_ok=True)
        return str(log_dir)

    @pytest.fixture
    def mock_settings(self, temp_log_dir: str) -> Generator[MagicMock, None, None]:
        """模拟设置对象

        为测试提供模拟的应用程序设置，将日志目录指向临时目录

        Args:
            temp_log_dir: 临时日志目录

        Yields:
            模拟的设置对象
        """
        with patch("alphapower.internal.logging.settings") as mock_settings:
            mock_settings.log_dir = temp_log_dir
            mock_settings.log_level = logging.DEBUG
            yield mock_settings

    @pytest.fixture
    def logger_cleanup(self) -> Generator[None, None, None]:
        """测试后清理日志记录器

        在测试执行后清理所有已创建的日志处理器，确保测试之间互不影响

        Yields:
            无返回值
        """
        yield
        # 测试后清理所有处理器
        logger_dict: Dict[str, Any] = (
            logging.Logger.manager.loggerDict
        )  # 修复访问 loggerDict 的方式
        for logger_name in logger_dict:
            logger: logging.Logger = logging.getLogger(logger_name)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

    @patch("structlog.configure")
    @patch("structlog.get_logger")
    def test_setup_logging_only_console(
        self,
        mock_get_logger: MagicMock,
        mock_configure: MagicMock,
        mock_settings: MagicMock,
        logger_cleanup: None,
    ) -> None:
        """测试设置仅控制台输出的日志配置

        验证当启用控制台输出时，日志系统应该正确配置控制台和文件处理器。

        Args:
            mock_get_logger: 模拟的 get_logger 函数
            mock_configure: 模拟的 configure 函数
            mock_settings: 模拟的设置对象
            logger_cleanup: 日志清理夹具
        """
        # 准备测试数据
        module_name: str = "test_module"
        mock_logger: MagicMock = MagicMock()
        mock_get_logger.return_value = mock_logger

        # 执行测试函数
        result: BoundLogger = get_logger(module_name, enable_console=True)

        # 验证结果
        assert result == mock_logger
        mock_get_logger.assert_called_once_with(module_name)
        mock_configure.assert_called_once()

        # 验证日志记录器配置
        logger: logging.Logger = logging.getLogger(module_name)
        assert logger.level == mock_settings.log_level
        assert not logger.propagate

        # 验证处理器配置
        handlers: list[logging.Handler] = logger.handlers
        assert len(handlers) == 2  # 一个控制台处理器和一个文件处理器

        # 确认控制台处理器存在
        console_handlers: list[logging.Handler] = [
            h
            for h in handlers
            if isinstance(h, logging.StreamHandler) and not hasattr(h, "baseFilename")
        ]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == mock_settings.log_level

    @patch("structlog.configure")
    @patch("structlog.get_logger")
    def test_setup_logging_no_console(
        self,
        mock_get_logger: MagicMock,
        mock_configure: MagicMock,
        mock_settings: MagicMock,
        logger_cleanup: None,
    ) -> None:
        """测试设置无控制台输出的日志配置

        验证当 enable_console=False 时，日志系统应仅配置文件处理器，
        不包含控制台处理器。

        Args:
            mock_get_logger: 模拟的 get_logger 函数
            mock_configure: 模拟的 configure 函数
            mock_settings: 模拟的设置对象
            logger_cleanup: 日志清理夹具
        """
        # 准备测试数据
        module_name: str = "test_no_console"
        mock_logger: MagicMock = MagicMock()
        mock_get_logger.return_value = mock_logger

        # 执行测试函数
        _: BoundLogger = get_logger(module_name, enable_console=False)

        # 验证结果
        logger: logging.Logger = logging.getLogger(module_name)
        handlers: list[logging.Handler] = logger.handlers

        # 应该只有文件处理器，没有控制台处理器
        console_handlers: list[logging.Handler] = [
            h
            for h in handlers
            if isinstance(h, logging.StreamHandler) and not hasattr(h, "baseFilename")
        ]
        assert len(console_handlers) == 0
        assert len(handlers) == 1  # 只有文件处理器

    @patch("os.makedirs")
    def test_log_dir_creation(
        self, mock_makedirs: MagicMock, mock_settings: MagicMock, logger_cleanup: None
    ) -> None:
        """测试日志目录创建功能

        验证当日志目录不存在时，setup_logging 函数会创建该目录。

        Args:
            mock_makedirs: 模拟的 makedirs 函数
            mock_settings: 模拟的设置对象
            logger_cleanup: 日志清理夹具
        """
        # 模拟目录不存在
        with patch("os.path.exists", return_value=False):
            get_logger("test_log_dir")
            # 验证目录创建调用
            mock_makedirs.assert_called_once_with(mock_settings.log_dir)

    @pytest.mark.asyncio
    async def test_async_logging(
        self, mock_settings: MagicMock, logger_cleanup: None
    ) -> None:
        """测试异步环境下的日志设置

        验证在异步环境中日志系统能够正常工作，包括协程标识的记录。

        Args:
            mock_settings: 模拟的设置对象
            logger_cleanup: 日志清理夹具
        """
        # 使用真实的结构化日志配置
        logger: BoundLogger = get_logger("test_async")

        # 简单测试日志输出
        await asyncio.sleep(0.1)  # 确保在异步上下文中

        # 异步环境中的日志输出测试
        with patch.object(logger, "_logger") as mock_logger:
            logger.info("测试异步日志消息 📝")
            mock_logger.info.assert_called_once()
