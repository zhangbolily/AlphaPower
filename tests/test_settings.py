import os

from alphapower.internal.logging import setup_logging
from alphapower.settings import settings

logger = setup_logging(__name__)


def test_settings() -> None:
    """
    测试配置加载
    """

    logger.info(settings.model_dump)
    assert os.getenv("ENVIRONMENT") == "test"
