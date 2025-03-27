import os

from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置
DATABASES = {
    "alphas": {
        "url": os.getenv("DATABASE_ALPHAS_URL", "sqlite:///db/alphas.db"),
        "description": "用户回测因子的信息",
    },
    "data": {
        "url": os.getenv("DATABASE_DATASETS_URL", "sqlite:///db/data.db"),
        "description": "数据集和数据字段的信息",
    },
    "simulation": {
        "url": os.getenv("DATABASE_SIMULATION_URL", "sqlite:///db/simulation.db"),
        "description": "用户回测任务的信息",
    },
}

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "./logs")

# 凭据配置
CREDENTIALS = {
    "0": {
        "username": os.getenv("CREDENTIALS_0_USERNAME", ""),
        "password": os.getenv("CREDENTIALS_0_PASSWORD", ""),
    },
    "1": {
        "username": os.getenv("CREDENTIALS_1_USERNAME", ""),
        "password": os.getenv("CREDENTIALS_1_PASSWORD", ""),
    },
}


def get_credentials(index: int = 0) -> dict:
    """
    获取指定索引的凭据。

    参数:
    index (int): 凭据索引，默认为 0。

    返回:
    dict: 包含用户名和密码的凭据字典。
    """
    return CREDENTIALS.get(str(index), {})
