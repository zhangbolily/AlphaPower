import asyncio
import contextvars
import logging
import multiprocessing
import traceback
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional, Set, Type

from sqlalchemy import AsyncAdaptedQueuePool, NullPool, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from structlog.stdlib import BoundLogger

from alphapower.constants import Database
from alphapower.entity import DATABASE_BASE_CLASS_MAP
from alphapower.internal.logging import get_logger
from alphapower.settings import DatabaseConfig, settings


def setup_sqlalchemy_logging() -> None:
    logging.basicConfig()
    engine_logger: logging.Logger = logging.getLogger("sqlalchemy.engine")
    pool_logger: logging.Logger = logging.getLogger("sqlalchemy.pool")
    engine_logger.setLevel(settings.sql_log_level)
    pool_logger.setLevel(settings.sql_log_level)


class SessionManager:
    """数据库会话管理器，支持多个数据库的引擎注册、初始化和会话获取功能"""

    def __init__(self) -> None:
        # 多进程隔离，pid -> {Database: AsyncEngine}
        self._engines: Dict[int, Dict[Database, AsyncEngine]] = {}
        self._session_factories: Dict[
            int, Dict[Database, async_sessionmaker[AsyncSession]]
        ] = {}
        self._readonly_engines: Dict[int, Dict[Database, AsyncEngine]] = {}
        self._readonly_session_factories: Dict[
            int, Dict[Database, async_sessionmaker[AsyncSession]]
        ] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self.log: BoundLogger = get_logger(self.__class__.__name__)
        self._session_ctx: Dict[
            tuple[int, int, Database, bool],
            contextvars.ContextVar[Optional[AsyncSession]],
        ] = {}

    async def register_database(
        self,
        base: Type[DeclarativeBase],
        db: Database,
        config: DatabaseConfig,
        readonly: bool = False,
        force_recreate: bool = False,
    ) -> None:
        # 调试日志记录函数入参
        await self.log.adebug(
            "调用 register_database 方法",
            base=base.__name__,
            alias=db,
            readonly=readonly,
            force_recreate=force_recreate,
            emoji="🔧",
        )

        if "sqlite" in config.dsn.scheme and readonly:
            await self.log.awarning(
                "SQLite 数据库不支持只读模式，修改为默认模式",
                alias=db,
                emoji="⚠️",
            )
            readonly = False

        # 1. 先注册数据库引擎
        raw_pid: Optional[int] = multiprocessing.current_process().pid
        if raw_pid is None:
            raise RuntimeError("当前进程没有有效的 pid，无法注册数据库引擎")
        pid: int = raw_pid
        try:
            async with self._lock:
                # 初始化多进程隔离的 dict
                if pid not in self._engines:
                    self._engines[pid] = {}
                if pid not in self._session_factories:
                    self._session_factories[pid] = {}
                if pid not in self._readonly_engines:
                    self._readonly_engines[pid] = {}
                if pid not in self._readonly_session_factories:
                    self._readonly_session_factories[pid] = {}

                if db in self._engines[pid] and not force_recreate:
                    await self.log.awarning(
                        "数据库引擎已注册，跳过注册", alias=db, emoji="⚠️"
                    )
                    return

                connect_args: Dict[str, Any] = {}
                execution_options: Dict[str, Any] = {}
                poolclass: Optional[Type] = None
                engine_kwargs: Dict[str, Any] = {}

                if "sqlite" in config.dsn.scheme:
                    # SQLite 数据库需要设置连接参数
                    connect_args = {
                        "check_same_thread": False,
                        "timeout": 30,  # 连接超时时间，不设置的话出现连接并发冲突会直接报错
                    }
                    execution_options = {
                        "isolation_level": "SERIALIZABLE",
                    }
                    poolclass = NullPool

                if "mysql" in config.dsn.scheme:
                    connect_args = {
                        "charset": "utf8mb4",
                        "connect_timeout": 5,
                    }
                    poolclass = AsyncAdaptedQueuePool
                    engine_kwargs = {
                        "pool_pre_ping": True,
                        "pool_recycle": 3600,
                        "pool_size": 30,
                        "max_overflow": 10,
                        "pool_timeout": 60,
                        "pool_use_lifo": True,
                    }

                await self.log.ainfo(
                    "注册数据库引擎",
                    alias=db,
                    url=config.dsn.encoded_string(),
                    readonly=readonly,
                    force_recreate=force_recreate,
                    connect_args=connect_args,
                    execution_options=execution_options,
                    emoji="🔄",
                )

                # 创建异步数据库引擎
                engine: AsyncEngine = create_async_engine(
                    url=config.dsn.encoded_string(),
                    logging_name=db.value,
                    connect_args=connect_args,
                    execution_options=execution_options,
                    poolclass=poolclass,
                    **engine_kwargs,
                )
                session_factory = async_sessionmaker(
                    engine,
                    expire_on_commit=False,
                    autoflush=False,
                )

                if "sqlite" in config.dsn.scheme:
                    async with engine.begin() as conn:
                        await conn.execute(text("PRAGMA journal_mode=WAL;"))
                        await conn.execute(text("PRAGMA synchronous=NORMAL;"))
                        await self.log.ainfo(
                            "SQLite 数据库设置成功",
                            alias=db,
                            readonly=readonly,
                            journal_mode="wal",
                            synchronous="NORMAL",
                            force_recreate=force_recreate,
                            emoji="✅",
                        )

                # 注册数据库引擎和会话工厂
                if readonly:
                    self._readonly_engines[pid][db] = engine
                    self._readonly_session_factories[pid][db] = session_factory
                else:
                    self._engines[pid][db] = engine
                    self._session_factories[pid][db] = session_factory

                await self.log.adebug(
                    "数据库引擎注册信息",
                    alias=db,
                    url=config.dsn.encoded_string(),
                    readonly=readonly,
                    force_recreate=force_recreate,
                    connect_args=connect_args,
                    emoji="🔧",
                )
        except ValueError as ve:
            await self.log.aerror(
                "数据库引擎注册失败，配置错误",
                alias=db,
                error=str(ve),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        except Exception as e:
            await self.log.aerror(
                "数据库引擎注册失败，未知错误",
                alias=db,
                error=str(e),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        finally:
            await self.log.adebug(
                "register_database 方法执行完成",
                alias=db,
                readonly=readonly,
                force_recreate=force_recreate,
                emoji="🔚",
            )

        # 2. 注册好的引擎需要创建表
        try:
            async with self._lock:
                if db in self._engines[pid]:
                    await self.log.adebug(
                        "创建数据库表",
                        alias=db,
                        readonly=readonly,
                        force_recreate=force_recreate,
                    )
                    async with engine.begin() as conn:
                        # 强制重建表
                        if force_recreate:
                            await conn.run_sync(base.metadata.drop_all)
                            await self.log.ainfo(
                                "数据库表删除成功",
                                alias=db,
                                readonly=readonly,
                                force_recreate=force_recreate,
                            )
                        # 创建所有表
                        await conn.run_sync(base.metadata.create_all)
                    await self.log.ainfo(
                        "数据库表创建成功",
                        alias=db,
                        readonly=readonly,
                        force_recreate=force_recreate,
                    )
        except Exception as e:
            await self.log.aerror(
                "数据库表创建失败",
                alias=db,
                error=str(e),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        finally:
            await self.log.adebug(
                "数据库表创建完成",
                alias=db,
                readonly=readonly,
                force_recreate=force_recreate,
            )

    @asynccontextmanager
    async def get_session(
        self, db: Database, readonly: bool = False
    ) -> AsyncGenerator[AsyncSession, None]:
        """
        获取指定数据库的会话

        参数:
            db: 数据库别名
            readonly: 是否只读模式，默认为 False

        返回:
            AsyncSession: 数据库会话对象
        """
        # 获取当前进程和 event loop 标识
        pid_raw: Optional[int] = multiprocessing.current_process().pid
        if pid_raw is None:
            raise RuntimeError("当前进程没有有效的 pid，无法获取数据库会话")
        pid: int = pid_raw
        try:
            loop_id: int = id(asyncio.get_running_loop())
        except RuntimeError:
            # 没有 event loop
            raise RuntimeError("get_session 必须在异步 event loop 中调用")
        ctx_key = (pid, loop_id, db, readonly)

        # 获取/创建 contextvar
        if ctx_key not in self._session_ctx:
            self._session_ctx[ctx_key] = contextvars.ContextVar(
                f"session_{pid}_{loop_id}_{db}_{readonly}", default=None
            )
        session_var = self._session_ctx[ctx_key]

        session: Optional[AsyncSession] = session_var.get()
        if session is not None and not session.is_active:
            # 已有 session，直接复用
            await self.log.adebug(
                event="复用协程上下文 session",
                message=f"复用已存在 session，方法名 {self.get_session.__qualname__} "
                f"类名 {self.__class__.__name__} 模块名 {__name__}",
                database=db,
                session_id=session.info.get("session_id"),
                readonly=readonly,
                emoji="🔁",
            )
            try:
                yield session
            finally:
                # 不关闭，交由首次创建时关闭
                pass
            return

        if readonly:
            # TODO: 只读模式需要重构
            readonly = False
            await self.log.awarning(
                "暂不支持只读模式，已修改为默认模式",
                alias=db,
                emoji="⚠️",
            )

        # 懒加载注册数据库
        if (
            readonly
            and (
                pid not in self._readonly_session_factories
                or db not in self._readonly_session_factories[pid]
            )
        ) or (
            not readonly
            and (
                pid not in self._session_factories
                or db not in self._session_factories[pid]
            )
        ):
            # 懒加载注册数据库
            await self.log.ainfo(
                event="懒加载注册数据库",
                message=f"懒加载数据库 {db}，方法名 {self.get_session.__qualname__}",
                alias=db,
                readonly=readonly,
                emoji="🔄",
            )
            try:
                config: Optional[DatabaseConfig] = settings.databases.get(db)
                base_class: Optional[Type[DeclarativeBase]] = (
                    DATABASE_BASE_CLASS_MAP.get(db)
                )
                if not config:
                    await self.log.aerror(
                        event="数据库配置不存在",
                        message=f"数据库配置不存在: {db}",
                        alias=db,
                        emoji="❌",
                    )
                    raise ValueError(f"数据库配置不存在: {db}")

                if not base_class:
                    await self.log.aerror(
                        event="数据库基础类不存在",
                        message=f"数据库基础类不存在: {db}",
                        alias=db,
                        emoji="❌",
                    )
                    raise ValueError(f"数据库基础类不存在: {db}")

                await self.register_database(
                    base=base_class,
                    db=db,
                    config=config,
                    readonly=readonly,
                )
            except Exception as e:
                await self.log.aerror(
                    event="懒加载注册数据库失败",
                    message=f"懒加载注册数据库失败: {e}",
                    alias=db,
                    error=str(e),
                    emoji="❌",
                )
                raise
            finally:
                await self.log.adebug(
                    event="懒加载注册数据库完成",
                    message=f"懒加载注册数据库完成，方法名 {self.get_session.__qualname__}",
                    alias=db,
                    readonly=readonly,
                )

        session_factory = (
            self._readonly_session_factories[pid][db]
            if readonly
            else self._session_factories[pid][db]
        )
        session = session_factory()
        session_id: uuid.UUID = uuid.uuid4()
        session.info["session_id"] = session_id

        # 设置 contextvar
        token = session_var.set(session)

        await self.log.ainfo(
            event="成功获取数据库会话",
            message=f"新建 session，方法名 {self.get_session.__qualname__}",
            database=db,
            session_id=session_id,
            session=session,
            readonly=readonly,
            emoji="🔄",
        )

        try:
            yield session
        except Exception as e:
            await session.rollback()
            await self.log.aerror(
                event="会话执行出现异常，回滚事务",
                message=f"会话异常回滚，方法名 {self.get_session.__qualname__}",
                database=db,
                session_id=session_id,
                error=str(e),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        finally:
            await session.close()
            session_var.reset(token)
            await self.log.ainfo(
                event="会话关闭",
                message=f"会话关闭并清理 contextvar，方法名 {self.get_session.__qualname__}",
                database=db,
                session_id=session_id,
                readonly=readonly,
                emoji="✅",
            )
            # 清理 contextvar 对象，避免内存泄漏
            if session_var.get() is None and ctx_key in self._session_ctx:
                del self._session_ctx[ctx_key]

    async def release_database(self, db: Database) -> None:
        """释放数据库引擎和会话工厂"""
        # 调试日志记录函数入参
        await self.log.adebug(
            "调用 release_database 方法",
            alias=db,
            emoji="🔧",
        )

        raw_pid: Optional[int] = multiprocessing.current_process().pid
        if raw_pid is None:
            raise RuntimeError("当前进程没有有效的 pid，无法释放数据库资源")
        pid: int = raw_pid
        try:
            async with self._lock:
                if pid in self._engines and db in self._engines[pid]:
                    engine = self._engines[pid].pop(db)
                    await engine.dispose()
                    await self.log.ainfo(
                        "数据库引擎释放成功",
                        alias=db,
                        emoji="✅",
                    )
                else:
                    await self.log.awarning(
                        "数据库引擎不存在，无法释放",
                        alias=db,
                        emoji="⚠️",
                    )
                if (
                    pid in self._session_factories
                    and db in self._session_factories[pid]
                ):
                    session_factory = self._session_factories[pid].pop(db)
                    await self.log.ainfo(
                        "数据库会话工厂释放成功",
                        alias=db,
                        session_factory=session_factory,
                        emoji="✅",
                    )
                else:
                    await self.log.awarning(
                        "数据库会话工厂不存在，无法释放",
                        alias=db,
                        emoji="⚠️",
                    )
                if pid in self._readonly_engines and db in self._readonly_engines[pid]:
                    engine = self._readonly_engines[pid].pop(db)
                    await engine.dispose()
                    await self.log.ainfo(
                        "只读数据库引擎释放成功",
                        alias=db,
                        emoji="✅",
                    )
                else:
                    await self.log.awarning(
                        "只读数据库引擎不存在，无法释放",
                        alias=db,
                        emoji="⚠️",
                    )
                if (
                    pid in self._readonly_session_factories
                    and db in self._readonly_session_factories[pid]
                ):
                    session_factory = self._readonly_session_factories[pid].pop(db)
                    await self.log.ainfo(
                        "只读数据库会话工厂释放成功",
                        alias=db,
                        session_factory=session_factory,
                        emoji="✅",
                    )
                else:
                    await self.log.awarning(
                        "只读数据库会话工厂不存在，无法释放",
                        alias=db,
                        emoji="⚠️",
                    )
        except Exception as e:
            await self.log.aerror(
                "释放数据库资源失败",
                alias=db,
                error=str(e),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        finally:
            await self.log.adebug(
                "release_database 方法执行完成",
                alias=db,
                emoji="🔚",
            )

    async def dispose_all(self) -> None:
        """释放所有数据库引擎和会话工厂"""
        # 调试日志记录函数入参
        await self.log.adebug(
            "调用 dispose_all 方法",
            emoji="🔧",
        )

        raw_pid: Optional[int] = multiprocessing.current_process().pid
        if raw_pid is None:
            raise RuntimeError("当前进程没有有效的 pid，无法释放所有数据库资源")
        pid: int = raw_pid
        all_dbs: Set[Database] = set()
        if pid in self._engines:
            all_dbs.update(self._engines[pid].keys())
        if pid in self._readonly_engines:
            all_dbs.update(self._readonly_engines[pid].keys())

        if not all_dbs:
            await self.log.ainfo(
                "没有需要释放的数据库引擎和会话工厂",
                emoji="✅",
            )
            return
        await self.log.ainfo(
            "开始释放所有数据库引擎和会话工厂",
            all_dbs=list(all_dbs),  # 转换为列表以便于日志输出
            emoji="🔄",
        )

        try:
            async with self._lock:
                for db in list(all_dbs):
                    await self.release_database(db)
                await self.log.ainfo(
                    "所有数据库引擎和会话工厂释放成功",
                    emoji="✅",
                )
        except Exception as e:
            await self.log.aerror(
                "释放所有数据库资源失败",
                error=str(e),
                traceback=traceback.format_exc(),
                emoji="❌",
            )
            raise
        finally:
            await self.log.adebug(
                "dispose_all 方法执行完成",
                emoji="🔚",
            )


setup_sqlalchemy_logging()
session_manager = SessionManager()
