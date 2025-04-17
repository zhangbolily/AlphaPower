import os

from alphapower.internal.logging import get_logger
from alphapower.settings import settings

logger = get_logger(__name__)


def test_settings() -> None:
    """
    测试配置加载
    """

    logger.info(settings.model_dump)
    assert os.getenv("ENVIRONMENT") == "test"
