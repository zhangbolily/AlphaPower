# WorldQuant Brain Alpha Advisor

## 项目简介

WorldQuant Brain Alpha Advisor 是一个用于管理 WorldQuant Brain 数据和操作的工具。它支持以下功能：
- 同步数据集
- 同步数据字段
- 同步因子
- 提供丰富的 API 接口与数据库交互

## 功能特性

- **数据同步**：支持从 WorldQuant API 同步数据集、数据字段和因子。
- **数据库管理**：使用 SQLAlchemy 管理数据存储。
- **日志记录**：支持控制台和文件日志输出，便于调试和监控。
- **命令行工具**：通过 CLI 快速执行同步任务。

## 环境要求

- Python 版本：`>=3.9`
- 数据库：支持 SQLite（默认配置）或其他 SQL 数据库。

## 安装步骤

1. 克隆项目代码：
   ```bash
   git clone https://github.com/your-repo/worldquant-brain-alpha-advisor.git
   cd worldquant-brain-alpha-advisor
   ```

2. 安装依赖：
   使用 `poetry` 安装依赖：
   ```bash
   poetry install
   ```

3. 配置环境变量：
   根据 `.env` 文件模板，配置数据库连接和凭据信息：
   ```plaintext
   DATABASE_ALPHAS_URL=sqlite:///alphas.db
   DATABASE_DATASETS_URL=sqlite:///data.db
   LOG_LEVEL=DEBUG
   LOG_DIR=./logs

   CREDENTIALS_0_USERNAME=your_username
   CREDENTIALS_0_PASSWORD=your_password
   CREDENTIALS_1_USERNAME=your_username
   CREDENTIALS_1_PASSWORD=your_password
   ```

4. 初始化数据库：
   确保数据库文件已创建并初始化。

## 使用方法

### 命令行工具

项目提供了一个命令行工具，支持以下操作：

1. **同步数据集**：
   ```bash
   python -m worldquant sync datasets --dataset_id <数据集ID> --region <区域> --universe <宇宙> --delay <延迟>
   ```

2. **同步因子**：
   ```bash
   python -m worldquant sync alphas --start_time <开始时间> --end_time <结束时间>
   ```

3. **同步数据字段**：
   ```bash
   python -m worldquant sync datafields --instrument_type <工具类型> --parallel <并行数>
   ```

### 测试

运行单元测试：
```bash
pytest
```

## 项目结构

```
worldquant/
├── __init__.py
├── __main__.py          # CLI 入口
├── _client.py           # 客户端实现
├── config/              # 配置文件
├── entity/              # 数据库实体
├── internal/            # 内部 API 实现
├── ops/                 # 算法操作
├── services/            # 服务层逻辑
├── tests/               # 测试用例
├── utils/               # 工具函数
```

## 贡献指南

欢迎贡献代码！请遵循以下步骤：
1. Fork 本仓库。
2. 创建分支：`git checkout -b feature/your-feature-name`
3. 提交代码：`git commit -m "添加新功能"`
4. 推送分支：`git push origin feature/your-feature-name`
5. 创建 Pull Request。

## 许可证

本项目基于 MIT 许可证开源，详情请参阅 [LICENSE](LICENSE) 文件。
