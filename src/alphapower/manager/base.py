from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import BaseModel, PrivateAttr
from structlog.stdlib import BoundLogger

from alphapower.internal.logging import get_logger


# 进程安全、可序列化的基类，所有子类都应继承此类
class BaseProcessSafeClass(ABC, BaseModel):
    # 私有属性用于缓存日志对象，避免序列化时被 pickle
    _log: Optional[BoundLogger] = PrivateAttr(default=None)

    # 懒加载日志对象，确保每个子进程独立获取
    @property
    def log(self) -> BoundLogger:
        if self._log is None:
            self._log: BoundLogger = get_logger(f"{__name__}.{self.__class__.__name__}")
        return self._log

    # 允许对象被 pickle，避免 logger 导致序列化失败
    def __getstate__(self) -> dict:
        # 拷贝对象状态，去除不可序列化的 _log
        state = self.__dict__.copy()
        if "_log" in state:
            del state["_log"]
        return state

    def __setstate__(self, state: dict) -> None:
        # 恢复对象状态，_log 重新懒加载
        self.__dict__.update(state)
        self._log = None


class ProcessSafeFactory(BaseProcessSafeClass):

    @abstractmethod
    def __call__(self, *args: Any, **kwds: Any) -> BaseProcessSafeClass:
        raise NotImplementedError(
            f"{self.__class__.__name__} is an abstract class and cannot be instantiated directly."
        )
