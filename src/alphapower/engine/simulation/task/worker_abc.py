"""
@file
@brief 抽象工作者类
@details
    该模块定义了一个抽象工作者类，所有具体的工作者都应该继承这个类。
@note
    该模块是 AlphaPower 引擎的一部分
"""

from abc import ABC, abstractmethod
from typing import Awaitable, Callable, List, Union

from alphapower.entity import SimulationTask

from .scheduler_abc import AbstractScheduler


class AbstractWorker(ABC):
    """
    抽象类，用于定义工作者的通用接口。
    """

    @abstractmethod
    async def set_scheduler(self, scheduler: AbstractScheduler) -> None:
        """
        设置调度器。

        :param scheduler: 调度器实例
        """

    @abstractmethod
    async def run(self) -> None:
        """
        运行工作者，执行任务。
        """

    @abstractmethod
    async def stop(self, cancel_tasks: bool = False) -> None:
        """
        停止工作者，清理资源。
        """

    @abstractmethod
    async def add_task_complete_callback(self, callback: Callable) -> None:
        """
        添加任务完成回调函数。
        TODO: 回调函数的入参类型需要确定一下

        :param callback: 回调函数
        """

    @abstractmethod
    async def add_heartbeat_callback(
        self,
        callback: Union[
            Callable[["AbstractWorker"], None],
            Callable[["AbstractWorker"], Awaitable[None]],
        ],
    ) -> None:
        """
        添加心跳回调函数。

        :param callback: 回调函数
        """

    @abstractmethod
    async def get_current_tasks(self) -> List[SimulationTask]:
        """
        获取当前任务的信息。
        """
