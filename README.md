# 🌟 Alpha Power

![Python Version](https://img.shields.io/badge/python-%3E%3D3.9-blue)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-90%25-yellowgreen)

## 📚 目录
- [🌟 Alpha Power](#-alpha-power)
  - [📚 目录](#-目录)
  - [📖 项目简介](#-项目简介)
  - [✨ 功能特性](#-功能特性)
  - [⚙️ 环境要求](#️-环境要求)
  - [🚀 安装步骤](#-安装步骤)
  - [🛠️ 使用方法](#️-使用方法)
    - [命令行工具](#命令行工具)
    - [测试](#测试)
  - [📂 项目结构](#-项目结构)
  - [🤝 贡献指南](#-贡献指南)
    - [提交规范](#提交规范)
    - [代码格式化](#代码格式化)
    - [代码审查流程](#代码审查流程)
  - [🗺️ 路线图](#️-路线图)
  - [📜 许可证](#-许可证)

## 📖 项目简介

Alpha Power 是一个用于管理 WorldQuant Brain 数据和操作的工具。它支持以下功能：
- 同步数据集
- 同步数据字段
- 同步因子
- 提供丰富的 API 接口与数据库交互
- 提供任务调度功能以自动化数据同步

## ✨ 功能特性

- **数据同步**：支持从 WorldQuant API 同步数据集、数据字段和因子。
- **数据库管理**：
  - 使用 SQLAlchemy 管理数据存储。
  - 支持 SQLite（默认配置）、PostgreSQL 和 MySQL 等多种数据库类型。
- **日志记录**：
  - 支持控制台和文件日志输出。
  - 提供多级别日志（DEBUG、INFO、WARNING、ERROR）。
  - 支持日志文件按日期滚动存储，便于长期监控。
- **命令行工具**：通过 CLI 快速执行同步任务。
- **任务调度**：
  - 支持定时任务的创建、管理和执行。
  - 提供任务失败的重试机制。
  - 支持任务执行日志的记录和分析。

## ⚙️ 环境要求

- Python 版本：`>=3.9`
- 数据库：支持 SQLite（默认配置）或其他 SQL 数据库。

## 🚀 安装步骤

1. 克隆项目代码：
   ```bash
   git clone <项目代码路径>
   cd alphapower
   ```

2. 安装依赖：
   检查是否已安装 `poetry`：
   ```bash
   poetry --version
   ```
   如果未安装，请先安装：
   ```bash
   pip install poetry
   ```
   如果安装失败，请尝试升级 `pip` 后重新安装：
   ```bash
   pip install --upgrade pip
   pip install poetry
   ```
   然后使用 `poetry` 安装项目依赖：
   ```bash
   poetry install
   ```

3. 配置环境变量：
   创建 `.env` 文件：
   ```bash
   touch .env
   ```
   根据以下模板配置数据库连接和凭据信息：
   ```plaintext
   DATABASE_ALPHAS_URL=sqlite:///alphas.db  # Alpha 数据库连接 URL
   DATABASE_DATASETS_URL=sqlite:///data.db  # 数据集数据库连接 URL
   LOG_LEVEL=DEBUG                          # 日志级别，可选值：DEBUG, INFO, WARNING, ERROR
   LOG_DIR=./logs                           # 日志文件存储目录

   CREDENTIALS_0_USERNAME=your_username     # 用户 0 的用户名
   CREDENTIALS_0_PASSWORD=your_password     # 用户 0 的密码
   CREDENTIALS_1_USERNAME=your_username     # 用户 1 的用户名
   CREDENTIALS_1_PASSWORD=your_password     # 用户 1 的密码

   SCHEDULER_ENABLED=true                   # 是否启用任务调度器
   SCHEDULER_INTERVAL=3600                  # 调度器运行间隔（秒）
   ```

4. 初始化数据库：
   执行以下命令以初始化数据库：
   ```bash
   python -m worldquant init-db
   ```
   如果初始化成功，您将看到类似以下的输出：
   ```
   Database initialized successfully.
   ```

## 🛠️ 使用方法

### 命令行工具

项目提供了一个命令行工具，支持以下操作：

1. **同步数据集**：
   ```bash
   python -m worldquant sync datasets --dataset_id 123 --region US --universe SP500 --delay 10
   ```
   - `--dataset_id`：要同步的数据集 ID。
   - `--region`：数据集所属的区域，例如 `US`。
   - `--universe`：数据集的宇宙范围，例如 `SP500`。
   - `--delay`：同步操作的延迟时间（秒）。

2. **同步因子**：
   ```bash
   python -m worldquant sync alphas --start_time 2023-01-01 --end_time 2023-01-31
   ```
   - `--start_time`：同步因子的起始时间（格式：YYYY-MM-DD）。
   - `--end_time`：同步因子的结束时间（格式：YYYY-MM-DD）。

3. **同步数据字段**：
   ```bash
   python -m worldquant sync datafields --instrument_type stock --parallel 4
   ```
   - `--instrument_type`：工具类型，例如 `stock`。
   - `--parallel`：并行任务数。

4. **启动任务调度器**：
   ```bash
   python -m worldquant scheduler start
   ```
   启动任务调度器以定时执行同步任务。

### 测试

1. 安装 `pytest`：
   ```bash
   pip install pytest pytest-cov
   ```

2. 运行单元测试：
   ```bash
   pytest
   ```

3. 运行单个测试用例：
   ```bash
   pytest tests/test_example.py::test_function_name
   ```

4. 生成测试覆盖率报告：
   ```bash
   pytest --cov=worldquant --cov-report=html
   ```
   覆盖率报告将生成在 `htmlcov/` 目录下。

5. 生成 XML 格式的测试报告（用于 CI/CD 工具）：
   ```bash
   pytest --junitxml=report.xml
   ```

6. 查看覆盖率报告：
   打开 `htmlcov/index.html` 文件即可查看详细的覆盖率报告。

7. 如果测试失败：
   - 检查失败的测试用例日志，定位问题。
   - 确保环境变量和依赖配置正确。
   - 如果问题无法解决，请联系项目维护者。

## 📂 项目结构

```
worldquant/
├── __init__.py          # 包初始化文件
├── __main__.py          # CLI 入口，处理命令行参数
├── _client.py           # 客户端实现，与外部 API 通信
├── config/              # 配置文件目录，存储项目配置
├── entity/              # 数据库实体定义，ORM 模型
├── internal/            # 内部 API 实现，核心逻辑
├── ops/                 # 算法操作模块，处理数据计算
├── scheduler/           # 任务调度器模块，管理定时任务
├── services/            # 服务层逻辑，封装业务功能
├── tests/               # 测试用例，包含单元测试和集成测试
├── utils/               # 工具函数，通用辅助功能
```

## 🤝 贡献指南

欢迎贡献代码！请遵循以下步骤：
1. 创建分支：`git checkout -b feature/your-feature-name`
2. 提交代码：`git commit -m "添加新功能"`
3. 推送分支：`git push origin feature/your-feature-name`
4. 提交代码变更供项目所有者审核。

### 提交规范
- 请确保代码符合 PEP 8 规范。
- 提交信息应简洁明了，描述清楚所做的更改。
- 如果添加了新功能，请确保编写了相应的测试用例。

### 代码格式化
在提交代码前，请运行以下命令以格式化代码：
```bash
pip install black
black .
```

### 代码审查流程
- 提交的 Pull Request 将由项目维护者进行代码审查。
- 审查内容包括代码质量、功能实现和测试覆盖率。
- 如果需要修改，维护者会在 Pull Request 中提供反馈。
- 修改完成后，重新提交以供审查。

## 🗺️ 路线图

我们计划在未来的版本中实现以下功能和改进：

1. **Brain 平台数据同步功能**：
   - 支持从世坤平台同步数据集、数据字段、Alpha 及关联对象到本地。
   - 支持使用不同的 RDB 管理本地同步数据。

2. **多用户支持**：
   - 实现多用户账号管理。
   - 实现用户级别的数据隔离，支持因子模拟、数据同步和分析。

3. **模拟回测任务管理**：
   - 模拟任务生成引擎：支持手动和模板生成任务，设置优先级。
   - 模拟任务执行引擎：支持直接调用和调度器调度。
   - 模拟任务调度引擎：支持优先级调度，提高资源利用率。
   - 模拟任务评估引擎：定时更新任务优先级和配置。
   - 模拟任务跟踪机制：收集失败任务错误信息，优化后续任务。

4. **性能优化**：
   - 优化数据库查询和数据同步性能。
   - 优化大批量数据分析性能。

5. **因子管理**
   - 基于场景可灵活配置的因子评估器，筛选出符合要求的因子，适配日常因子提交和竞赛的场景
   - 基于场景可灵活配置的因子提交器，可根据要求对因子进行排序，选择出合适的因子进行提交

## 📜 许可证

本项目为闭源项目，未经授权禁止复制、分发或使用。以下行为被明确禁止：
- 反向工程或试图提取项目的源代码。
- 未经授权的商业使用，包括但不限于将项目用于盈利目的。
- 未经许可的分发或共享项目代码。

如需获取更多信息或授权，请通过以下方式联系项目所有者：
- **电子邮件**：`contact@ballchang.com`
- **授权流程**：提交授权申请时，请提供您的用途说明和相关背景信息。项目所有者将在审核后决定是否授权。
