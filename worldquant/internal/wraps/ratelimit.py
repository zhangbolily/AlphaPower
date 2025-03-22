import asyncio
from functools import wraps

from worldquant.internal.http_api.common import RateLimit
from worldquant.internal.utils import setup_logging

logger = setup_logging(__name__)


def rate_limit_handler(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        response = await func(*args, **kwargs)
        if isinstance(response, tuple) and isinstance(response[-1], RateLimit):
            rate_limit = response[-1]
            if rate_limit.remaining == 0:
                logger.warning(f"已达到速率限制。将在 {rate_limit.reset} 秒后重试。")
                await asyncio.sleep(rate_limit.reset)
                response = await func(*args, **kwargs)
        return response

    return wrapper
