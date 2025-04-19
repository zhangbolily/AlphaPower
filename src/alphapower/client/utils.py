"""
限流处理器
"""

import asyncio
from asyncio import Lock  # 替换为 asyncio.Lock
from functools import wraps
from typing import Any, Awaitable, Callable

from alphapower.internal.logging import get_logger

from .models import RateLimit

# 配置日志
log = get_logger(__name__)

# 用于存储限流状态的线程安全全局变量
rate_limit_status: dict[str, RateLimit] = {}
rate_limit_lock: Lock = Lock()  # 用于保护 rate_limit_status 的协程锁


def rate_limit_handler(
    func: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """
    一个装饰器，用于处理速率限制。

    优化点：
    1. 动态调整请求间隔，减少触发限流规则的可能性。
    2. 使用指数退避机制，在触发限流时逐步增加重试间隔。
    3. 增强日志记录，便于分析限流行为。

    参数:
        func (Callable[..., Awaitable[Any]]): 被装饰的异步函数。

    返回:
        Callable[..., Awaitable[Any]]: 包装后的异步函数。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        retry_interval: float = 0.1  # 初始重试间隔（秒）
        max_retry_interval: float = 10.0  # 最大重试间隔（秒）
        func_name: str = func.__name__  # 获取函数名称作为限流状态的键
        rate_limit: RateLimit

        while True:
            # 检查并更新本地限流状态
            async with rate_limit_lock:  # 使用 asyncio.Lock 确保协程安全
                if func_name in rate_limit_status:
                    rate_limit = rate_limit_status[func_name]
                    if rate_limit.remaining == 0:
                        await log.awarning(
                            "本地限流状态：已达到速率限制，等待 %s 秒后重试",
                            rate_limit.reset,
                            emoji="⏳",
                        )
                        await asyncio.sleep(rate_limit.reset)
                        retry_interval = min(retry_interval * 2, max_retry_interval)
                        # 等待结束就立刻尝试请求，目的是更新本地限流状态到最新值
                        # 否则会出现本地资源枯竭，全部都在等待资源释放的情况
                    else:
                        # 提前扣减请求配额
                        rate_limit.remaining -= 1
                        rate_limit_status[func_name] = rate_limit
                        await log.adebug(
                            "提前扣减请求配额",
                            remaining=rate_limit.remaining,
                            emoji="➖",
                        )

            try:
                response = await func(*args, **kwargs)
                if isinstance(response, tuple) and isinstance(response[-1], RateLimit):
                    rate_limit = response[-1]
                    await log.adebug(
                        "请求返回限流信息",
                        limit=rate_limit.limit,
                        remaining=rate_limit.remaining,
                        reset=rate_limit.reset,
                        emoji="📊",
                    )

                    # 更新本地限流状态
                    async with rate_limit_lock:  # 使用 asyncio.Lock 确保协程安全
                        rate_limit_status[func_name] = rate_limit

                    # 如果剩余请求数为 0，等待重置时间
                    if rate_limit.remaining == 0:
                        await log.awarning(
                            "已达到速率限制，等待 %s 秒后重试",
                            rate_limit.reset,
                            emoji="⏳",
                        )
                        await asyncio.sleep(rate_limit.reset)
                        retry_interval = min(retry_interval * 2, max_retry_interval)
                        continue

                    # 动态调整请求间隔
                    interval: float = max(
                        0,
                        rate_limit.reset / rate_limit.limit,
                    )
                    await log.adebug(
                        "动态调整请求间隔",
                        interval=interval,
                        limit=rate_limit.limit,
                        remaining=rate_limit.remaining,
                        reset=rate_limit.reset,
                        emoji="⏱️",
                    )
                    await asyncio.sleep(interval)
                return response
            except Exception as e:
                log.error(
                    "请求处理时发生异常",
                    error=str(e),
                    exc_info=True,
                    emoji="❌",
                )
                raise

    return wrapper
