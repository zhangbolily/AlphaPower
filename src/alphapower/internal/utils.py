"""
通用工具函数
"""

import asyncio
from typing import Coroutine


def safe_async_run(coro: Coroutine) -> asyncio.Task:
    """通用安全运行协程函数"""
    try:
        # 尝试获取当前事件循环
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 无运行中的循环
        return asyncio.run(coro)
    else:  # 已有循环运行
        if loop.is_running():
            # 在已有循环中创建任务
            return loop.create_task(coro)
        else:
            # 使用现有未运行的循环
            return loop.run_until_complete(coro)
