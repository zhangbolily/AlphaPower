"""
Mock WQB Server for Testing
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, Generator
from urllib.parse import urlparse

import pytest
from pytest_httpserver import HTTPServer

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_json_fixture(filename: str) -> Dict[str, Any]:
    """从 fixtures 目录加载 JSON 文件"""
    with open(FIXTURES_DIR / filename, encoding="UTF-8") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def mock_server() -> Generator[HTTPServer, None, None]:
    """会话级别的 mock server fixture"""
    with HTTPServer() as server:
        yield server


def add_wildcard_route(pattern: str):
    """添加通配符路由"""

    def wildcard_matcher(request) -> bool:
        # 将模式转换为正则表达式
        regex_pattern = pattern.replace("*", "[^/]*").replace("**", ".*")
        path = urlparse(request.url).path
        return re.fullmatch(regex_pattern, path) is not None

    return wildcard_matcher


@pytest.fixture(autouse=True, scope="function")
def setup_mock_responses(mock_server: HTTPServer) -> Generator[str, None, None]:
    """自动设置所有测试共用的 mock 响应"""

    # 清除之前的 mock 设置
    mock_server.clear()

    # 加载并设置各种 API 的 mock 响应
    setup_alphas_check(mock_server)
    setup_alphas_detail(mock_server)

    # 返回 mock server 的 base URL 供测试使用
    yield mock_server.url_for("/")

    mock_server.check_assertions()


def setup_alphas_check(mock_server: HTTPServer):
    """设置 Alphas Check 的 mock 响应"""
    mock_server.expect_request(uri="/alphas/alpha_id/check").respond_with_json(
        load_json_fixture("alphas_check.json")
    )


def setup_alphas_detail(mock_server: HTTPServer):
    """设置 Alphas Detail 的 mock 响应"""
    mock_server.expect_request(uri="/alphas/alpha_id").respond_with_json(
        load_json_fixture("alphas_detail_0.json")
    )
