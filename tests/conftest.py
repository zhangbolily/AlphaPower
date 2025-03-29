"""
Mock WQB Server for Testing
"""

from typing import Generator

import pytest
from pytest_httpserver import HTTPServer


@pytest.fixture(scope="session")
def mock_server() -> Generator[HTTPServer, None, None]:
    """会话级别的 mock server fixture"""
    with HTTPServer() as server:
        yield server
