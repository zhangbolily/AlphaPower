# AlphaPower Copilot 基础提示词
你现在身处一个用 Python 语言编写、poetry 进行项目管理、pytest 作为测试框架的命令行工具项目中

## 编码规范
- 开发语言版本为 Python 3.12
- 编码规范采用 #fetch [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)

```python
# -*- coding: utf-8 -*-
"""Example Google style docstrings.
"""

def function_with_pep484_type_annotations(param1: int, param2: str) -> bool:
    raise NotImplementedError('This function is not implemented yet.')

def module_level_function(param1: Any, param2: Any = None, *args: Any, **kwargs: Any) -> bool:
    if param1 == param2:
        raise ValueError('param1 may not be equal to param2')
    return True


def example_generator(n: int) -> AsyncGenerator[int, None]:
    for i in range(n):
        yield i


class ExampleError(Exception):
    def __init__(self, msg: str, code: int = 0) -> None:
        super().__init__(msg)
        self.msg = msg
        self.code = code


class ExampleClass(object):
    def __init__(self, param1: Any, param2: Any, param3: Any) -> None:
        self.attr1 = param1
        self.attr2 = param2
        self.attr3 = param3  #: Doc comment *inline* with attribute

        #: list of str: Doc comment *before* attribute, with type specified
        self.attr4: List[str] = ['attr4']

        self.attr5: Any = None

    @property
    def readonly_property(self) -> str:
        return 'readonly_property'

    @property
    def readwrite_property(self) -> List[str]:
        return ['readwrite_property']

    @readwrite_property.setter
    def readwrite_property(self, value: List[str]) -> None:
        self.attr5 = value

    def example_method(self, param1: Any, param2: Any) -> bool:
        return True

    def __special__(self) -> None:
        pass

    def __special_without_docstring__(self) -> None:
        pass

    def _private(self) -> None:
        pass

    def _private_without_docstring(self) -> None:
        pass
```
- 项目使用 pylint 和 mypy 进行静态检查
- 所有变量、方法的入参、出参加上类型注解
- 拆分长函数，减少圈复杂度、减少判断语句嵌套
- 使用 async 异步编程风格
- 使用 aiostream 实现并发任务管理

```python
# 这是一个 aiostream 的示例
# 主要用于演示如何使用 aiostream 进行异步编程
import asyncio
from typing import AsyncGenerator, List, Dict, Any
from aiostream import stream
import httpx

# 生成要请求的 URL 列表
def generate_url_list() -> List[str]:
    return [f"https://api.example.com/items/{i}" for i in range(100)]

# 异步生成器逐个产生 URL
async def url_producer(urls: List[str]) -> AsyncGenerator[str, None]:
    for url in urls:
        yield url

# 使用 httpx 执行异步 GET 请求
async def fetch_json(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    response: httpx.Response = await client.get(url)
    response.raise_for_status()
    return response.json()

# 模拟异步处理响应数据
async def process_result(data: Dict[str, Any]) -> None:
    print(f"Processed item {data.get('id')}: {data.get('value')}")

# 主流程：构建 aiostream 管道 + 并发 + 背压控制
async def main() -> None:
    urls: List[str] = generate_url_list()

    async with httpx.AsyncClient(timeout=10.0) as client:
        # 1. URL 源流
        url_stream = stream.iterate(url_producer(urls))

        # 2. 并发请求（最多 10 个并发任务）
        fetch_stream = stream.map(
            url_stream,
            lambda url: fetch_json(client, url),
            task_limit=10
        )

        # 3. 消费流并处理（受背压控制）
        async with fetch_stream.stream() as streamer:
            async for result in streamer:
                await process_result(result)

# CLI 入口
if __name__ == "__main__":
    asyncio.run(main())
```

## 日志（logging）
### 日志对象（logger）
- 日志输出使用 alphapower.internal.logging 中 get_logger 获取 structlog 日志对象 BoundLogger
- 类定义继承 alphapower.internal.logging 中的 BaseLogger 自动管理日志对象 log

### 打印方式
- 日志采用 structlog 键值对格式化风格打印
- 方法名使用 \_\_qualname\_\_ 属性获取拼接到日志中
- 非字典对象需要打印内部属性字典 \_\_dict\_\_ 和对象类名
- 类名、模块名和其他对象名称使用 \_\_name\_\_ 属性获取拼接到日志中
- 其他日志内容尽量使用变量动态拼接，不要硬编码写在日志字符串里
- 长度超过 80 的日志断行
- event 字段打印日志事件摘要
- message 字段打印日志详情
- emoji 字段打印符合日志内容语义的 Emoji 表情，常用的表情定义在 `alphapower.internal.constants.LoggingEmoji` 中

```python
logger: BoundLogger = get_logger(__name__)
example_obj: ExampleObjClass = ExampleObjClass()
example_var: Any = None

async def async_log() -> None:
    await logger.ainfo(
        event="事件摘要",
        message=f"事件详情，方法名 {async_log.__qualname__} 类名 {ExampleObjClass.__name__}"
        f" 对象类名 {example_obj.__class__.__name__} 模块名 {__name__} 其他变量 {example_var}",
        example_obj=example_obj.__dict__, # 对象需要打印属性字典
        emoji="✅",
    )
```

### 日志级别
不同日志级别内容输出建议

#### DEBUG – 用于开发/调试
```python
logger: BoundLogger = get_logger(__name__)

async def async_log() -> None:
    await logger.adebug(...)    # 进入函数
    
    result: Any = await call_func()
    await logger.adebug(...)    # 函数调用，尤其是外部模块、类外部方法

    result = await modify_func(result)
    await logger.adebug(...)    # 变量发生状态变化

    await logger.adebug(...)    # 退出函数

def sync_log() -> None:
    logger.debug(...)    # 进入函数
    
    result: Any = call_func()
    logger.debug(...)    # 函数调用，尤其是外部模块、类外部方法

    result = modify_func(result)
    logger.debug(...)    # 变量发生状态变化

    logger.debug(...)    # 退出函数
```

#### INFO – 程序重要操作的关键路径
```python
logger: BoundLogger = get_logger(__name__)

class LogClass:
    def __init__() -> None:
        # 不打印日志

def init_func() -> None:    # 类、模块、服务定义的公共初始化函数，不是类的构造函数 __init__
    # 执行初始化
    logger.info(...) # 成功执行，打印详情日志

async def async_log() -> None:    # 异步函数用异步日志接口
    result: Any = await call_func()
    await logger.ainfo(...)    # 外部模块、类外部方法的函数调用，打印成功信息和返回值摘要

    await logger.ainfo(...)    # 函数执行成功

def sync_log() -> None:     # 同步函数用同步日志接口
    result: Any = call_func()
    logger.info(...)    # 外部模块、类外部方法的函数调用，打印成功信息和返回值摘要

    logger.info(...)    # 函数执行成功
```

#### WARNING – 程序可容忍的异常情况
```python
logger: BoundLogger = get_logger(__name__)
default_value: Any = "default_value"

async def async_log() -> None:
    result: Any = await call_func()
    if result is None:
        retult = default_value
        await logger.awarning(...)    # 程序可以容忍的异常情况
    
    result = await retriable_func(result)
    if result is None:
        result = default_value
        await logger.awarning(...)    # 外部接口响应慢或失败但可重试
    
    param: Any = await get_param()
    if param is None:
        param = default_value
        await logger.awarning(...)    # 不影响主流程的格式、参数问题
    
    result = await downgrade_func(result)
    if result is None:
        result = default_value
        await logger.awarning(...)    # 功能降级提示

def sync_log() -> None:
    result: Any = call_func()
    if result is None:
        retult = default_value
        logger.warning(...)    # 程序可以容忍的异常情况
    
    result = retriable_func(result)
    if result is None:
        result = default_value
        logger.warning(...)    # 外部接口响应慢或失败但可重试
    
    param: Any = get_param()
    if param is None:
        param = default_value
        logger.warning(...)    # 不影响主流程的格式、参数问题
    
    result = downgrade_func(result)
    if result is None:
        result = default_value
        logger.warning(...)    # 功能降级提示
```

#### ERROR – 功能失败或数据异常
```python
logger: BoundLogger = get_logger(__name__)
async def async_log() -> None:
    try:
        result: Any = await call_func()
        if result == "error":
            await logger.aerror(...)    # 操作失败、异常被捕获
    except Exception as e:
        await logger.aerror(...)    # 外部系统错误影响主流程
        raise e
    
    result = await must_get_one()  # 必须获取的结果
    if result is None:
        await logger.aerror(...)    # 数据不一致、关键步骤失败
        raise ValueError("数据不一致")

def sync_log() -> None:
    try:
        result: Any = call_func()
        if result == "error":
            logger.error(...)    # 操作失败、异常被捕获
    except Exception as e:
        logger.error(...)    # 外部系统错误影响主流程
        raise e
    
    result = must_get_one() # 必须获取的结果
    if result is None:
        logger.error(...)    # 数据不一致、关键步骤失败
        raise ValueError("数据不一致")
```

#### CRITICAL – 不可恢复/需立刻报警
- 同步方法中使用 critical 异步方法中使用 acritical
- 系统退出、数据严重破坏
- 配置或资源导致系统瘫痪
- 管理员需要立即介入的场景

## 单元测试
- 总是将测试用例归属到各自的测试类下面，没有单独的测试方法
- 使用 pytest.fixture 生成测试数据
- 总是复用测试数据，推荐用继承测试基类的方式复用

## 文档和注释
- 不编写模块文档
- 不编写类文档
- 不编写方法文档
- 编写关键步骤注释
- 专业术语提供中文和原文对照

## 异常处理
- 异步方法的异常处理使用 alphapower.internal.decorator 的 async_exception_handler 装饰器
```python
@async_exception_handler
async def async_main() -> None:
    # 异步方法加上注解
    pass

def sync_main() -> None:
    # 同步方法不加注解
    pass
```
- 需要处理异常并从中恢复的，方法内部捕获异常并实现处理恢复逻辑
- 方法内部各主要步骤，分别捕获处理异常

## 项目结构
- 数据实体 `src/alphapower/entity` 定义了所有的数据库实体类
- 数据查询层 `src/alphapower/dal` 默认使用此模块提供的单例查询对象
- 应用配置是 `src/alphapower/settings.py` 中的对象 settings
- 数据库会话管理 `src/alphapower/dal` 中的对象 session_manager
- 日志管理 `src/alphapower/internal/logging.py`
- 接口 View 类 `src/alphapower/view` 提供所有 HTTP 接口的请求参数、请求体、响应体的模型类定义
- 单元测试 `tests`