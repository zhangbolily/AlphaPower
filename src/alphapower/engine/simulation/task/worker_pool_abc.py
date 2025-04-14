from abc import ABC, abstractmethod


class AbstractWorkerPool(ABC):
    """抽象工作池，提供工作者数量动态管理、优雅退出和获取状态等功能。"""

    @abstractmethod
    async def start(self) -> None:
        """启动工作池。"""

    @abstractmethod
    async def stop(self) -> None:
        """停止工作池，并回收所有工作者相关资源。"""

    @abstractmethod
    async def scale_up(self, count: int) -> None:
        """向上扩容指定数量的工作者。"""

    @abstractmethod
    async def scale_down(self, count: int) -> None:
        """
        向下缩容指定数量的工作者。

        Args:
            count: 要减少的工作者数量。实现时应处理请求数量超过当前工作者总数的情况。
        """

    @abstractmethod
    async def get_status(self) -> dict:
        """获取工作池的运行状态和各项参数。"""

    @abstractmethod
    async def worker_count(self) -> int:
        """获取当前工作者数量。"""
