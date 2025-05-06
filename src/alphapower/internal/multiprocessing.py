import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar

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


# 定义一个泛型变量，用于表示工厂生成的对象类型
T = TypeVar("T")


class BaseProcessSafeFactory(BaseProcessSafeClass, Generic[T]):
    def __init__(self, **kwargs: Any) -> None:
        """
        初始化工厂类，接收主进程中的基础变量。
        """
        self._state: Dict[str, Any] = kwargs
        self._last_injected_pid: Optional[int] = None  # 记录上次注入依赖的进程 ID

    def __getstate__(self) -> Dict[str, Any]:
        """
        序列化工厂对象的状态，移除不可序列化的属性。
        """
        state = self.__dict__.copy()
        state.pop("_log", None)  # 移除日志对象
        state.pop("_last_injected_pid", None)  # 移除进程 ID
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """
        反序列化工厂对象的状态，重新初始化不可序列化的属性。
        """
        self.__dict__.update(state)
        self._log = None  # 日志对象重新懒加载
        self._last_injected_pid = None  # 进程 ID 重新初始化

    @abstractmethod
    async def _dependency_factories(self) -> Dict[str, "BaseProcessSafeFactory"]:
        """
        返回不可序列化对象的工厂方法字典。
        子类需要实现此方法，定义依赖对象的构建逻辑。
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} 必须实现 '_dependency_factories' 方法。"
        )

    async def _reinject_dependencies(self) -> None:
        """
        在子进程中重新构建并注入不可序列化的依赖对象。
        支持同步和异步工厂方法。
        """
        current_pid = os.getpid()
        if self._last_injected_pid == current_pid:
            await self.log.adebug(
                "当前进程已注入依赖，无需重复注入",
                process_id=current_pid,
                emoji="🔄",
            )
            return

        factories = await self._dependency_factories()
        for name, factory in factories.items():
            try:
                # 工厂方法必须是 BaseProcessSafeFactory 的子类实例
                if not isinstance(factory, BaseProcessSafeFactory):
                    raise ValueError(
                        f"工厂方法 '{name}' 必须是 BaseProcessSafeFactory 的子类实例"
                    )

                dependency = await factory()  # 调用工厂方法构建依赖对象
                self._state[name] = dependency
                await self.log.adebug(
                    "成功注入依赖对象",
                    dependency_name=name,
                    dependency_type=type(dependency).__name__,
                    emoji="✅",
                )
            except Exception as e:
                await self.log.aerror(
                    "注入依赖对象失败",
                    dependency_name=name,
                    error=str(e),
                    emoji="❌",
                )
                raise

        self._last_injected_pid = current_pid  # 更新最后注入依赖的进程 ID

    @abstractmethod
    async def _build(self, *args: Any, **kwargs: Any) -> T:
        """
        构建目标对象，子类需要实现此方法。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 '_build' 方法。")

    async def __call__(self, *args: Any, **kwargs: Any) -> T:
        """
        工厂调用接口，构建目标对象并注入依赖。
        """
        await self.log.ainfo(
            "开始调用工厂构建目标对象",
            args=args,
            kwargs=kwargs,
            emoji="🚀",
        )
        try:
            await self._reinject_dependencies()
            result = await self._build(*args, **kwargs)
            await self.log.ainfo(
                "成功构建目标对象",
                result_type=type(result).__name__,
                emoji="✅",
            )
            return result
        except Exception as e:
            await self.log.acritical(
                "工厂构建目标对象失败",
                error=str(e),
                emoji="❌",
            )
            raise
