"""
mock_api.py
Mock API Responses for Testing
"""

import json
from pathlib import Path
from typing import Any, Dict, Generator

import pytest
from pytest_httpserver import HTTPServer

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True, scope="function")
def setup_mock_responses(mock_server: HTTPServer) -> Generator[str, None, None]:
    """自动设置所有测试共用的 mock 响应"""

    # 清除之前的 mock 设置
    mock_server.clear()

    # 加载并设置各种 API 的 mock 响应
    setup_alphas_check(mock_server)
    setup_alphas_detail(mock_server)
    setup_self_alpha_list(mock_server)

    setup_simulation_result(mock_server)
    setup_simulation_failed_result(mock_server)
    setup_alpha_competitions(mock_server)

    # 返回 mock server 的 base URL 供测试使用
    yield mock_server.url_for("/")

    mock_server.check_assertions()


def load_json_fixture(filename: str) -> Dict[str, Any]:
    """从 fixtures 目录加载 JSON 文件"""
    with open(FIXTURES_DIR / filename, encoding="UTF-8") as f:
        return json.load(f)


def setup_alphas_check(mock_server: HTTPServer) -> None:
    """设置 Alphas Check 的 mock 响应"""
    mock_server.expect_request(uri="/alphas/regular_alpha_0/check").respond_with_json(
        load_json_fixture("alpha_check_0.json")
    )
    mock_server.expect_request(uri="/alphas/regular_alpha_1/check").respond_with_json(
        load_json_fixture("alpha_check_1.json")
    )


def setup_alphas_detail(mock_server: HTTPServer) -> None:
    """设置 Alphas Detail 的 mock 响应"""
    mock_server.expect_request(uri="/alphas/regular_alpha_0").respond_with_json(
        load_json_fixture("alpha_detail_0.json")
    )
    mock_server.expect_request(uri="/alphas/regular_alpha_1").respond_with_json(
        load_json_fixture("alpha_detail_1.json")
    )
    mock_server.expect_request(uri="/alphas/super_alpha_0").respond_with_json(
        load_json_fixture("super_alpha_detail_0.json")
    )


def setup_self_alpha_list(mock_server: HTTPServer) -> None:
    """设置 Self Alpha List 的 mock 响应"""
    mock_server.expect_request(uri="/users/self/alphas").respond_with_json(
        load_json_fixture("self_alpha_list_0.json")
    )


def setup_simulation_result(mock_server: HTTPServer) -> None:
    """设置 Simulation Result 的 mock 响应"""
    mock_server.expect_request(uri="/simulations/single_0").respond_with_json(
        load_json_fixture("single_simulation_result_0.json")
    )
    mock_server.expect_request(uri="/simulations/single_1").respond_with_json(
        load_json_fixture("single_simulation_result_1.json")
    )
    mock_server.expect_request(uri="/simulations/super_0").respond_with_json(
        load_json_fixture("super_simulation_result_0.json")
    )


def setup_simulation_failed_result(mock_server: HTTPServer) -> None:
    """设置 Simulation Failed Result 的 mock 响应"""
    mock_server.expect_request(uri="/simulations/single_failed_0").respond_with_json(
        load_json_fixture("single_simulation_failed_result_0.json")
    )
    mock_server.expect_request(uri="/simulations/single_failed_1").respond_with_json(
        load_json_fixture("single_simulation_failed_result_1.json")
    )
    mock_server.expect_request(uri="/simulations/super_failed_0").respond_with_json(
        load_json_fixture("super_simulation_failed_result_0.json")
    )
    mock_server.expect_request(uri="/simulations/multi_failed_0").respond_with_json(
        load_json_fixture("multi_simulation_failed_result_0.json")
    )


def setup_alpha_competitions(mock_server: HTTPServer) -> None:
    """设置 Alpha Competitions 的 mock 响应"""
    mock_server.expect_request(uri="/competitions").respond_with_json(
        load_json_fixture("alpha_competitions_0.json")
    )
