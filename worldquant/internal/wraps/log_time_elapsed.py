import asyncio
import time
from functools import wraps

from worldquant.internal.utils import setup_logging

logger = setup_logging(__name__, enable_console=False)


def log_time_elapsed(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        task_id = id(asyncio.current_task())
        logger.info(
            f"[Task {task_id}] 函数 {func.__name__} 的入参: args={list(map(str, args))}, kwargs={kwargs}"
        )
        start_time = time.time()
        await func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(
            f"[Task {task_id}] 函数 {func.__name__} 耗时: {elapsed_time:.2f} 秒"
        )

    return wrapper
