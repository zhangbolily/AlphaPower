from functools import wraps

from alphapower.internal.utils import setup_logging

# 配置日志
logger = setup_logging(__name__)


def exception_handler(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"执行 {func.__name__} 时发生异常: {e}", exc_info=True)
            raise

    return wrapper
