你现在身处一个用 Python 语言编写、poetry 进行项目管理、pytest 作为测试框架的命令行工具项目中

代码风格要求：
- #fetch https://google.github.io/styleguide/pyguide.html Google Python Style Guide 是本项目的编码规范 
- 总是使用中文回答内容、代码注释、异常内容
- 不常见或可能引起歧义的专业术语，加入中文和原文对照注释
- 总是在输出代码的时候将注释、文档补充完整
- 总是给变量、函数加上类型注解
- 总是对长函数需要进行逻辑拆分，保持可读性、易于理解和可维护性
- 总是使用异步 IO

日志输出规范：
- 日志输出使用 alphapower.internal.logging 中 setup_logging 获取日志对象
- 日志框架是 structlog 注意使用对应风格的字段打印方法
- async 方法中总是使用 structlog 的异步接口，用 await 去调用
- 同步方法中总是使用 structlog 的同步接口
- 变量解除引用输出内容，除非特别指明打印变量的指针地址
- 长度超过 80 的日志行必须进行断行拼接
- 调试日志至少要覆盖函数入参、出参
- 支持输出到控制台、轮转文件，并按模块名称分类
- DEBUG 日志输出参数，关键变量出现的变化，记录函数调用情况
- INFO 日志输出对象初始化、销毁，方法进入退出等内容
- WARNING 日志输出可能出现问题的情况，如资源使用接近上限、捕获异常但未继续抛出等
- ERROR 日志输出函数返回失败信息和原因，打印异常堆栈
- CRITICAL 记录导致程序完全退出的严重错误，例如程序初始化数据库失败、依赖的模块不可用等，打印完整的堆栈信息方便排查
- 不同级别日志内容不要重复，互补内容
- 总是使用 Emoji 表情丰富日志格式和内容，emoji 表情用单独的 emoji 字段打印

单元测试规范：
- 总是将测试用例归属到各自的测试类下面，没有单独的测试方法
- 使用 pytest.fixture 生成测试数据
- 总是复用测试数据，推荐用继承测试基类的方式复用

文档规范：
- 不要编写 docstring 类型的文档，会导致代码行数暴增
- docstring 文档等待指令去编写

项目结构：
- 数据实体定义在 src/alphapower/entity
- 数据查询层定义在 src/alphapower/dal 统一使用 DALFactory 生成实体 DAL
- 应用配置定义在 src/alphapower/settings.py
- 数据库会话定义 src/alphapower/internal/db_session.py
- 日志对象管理 src/alphapower/internal/logging.py
- 大部分的 View 类 src/alphapower/client
- 单元测试 tests