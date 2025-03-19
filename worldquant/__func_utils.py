import json
import logging
import time
from functools import wraps

import requests

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            logger.info(
                f"Function '{func.__name__}' executed in {end_time - start_time:.4f} seconds"
            )
            return result
        except Exception as e:
            logger.error(f"Error in function '{func.__name__}': {e}")
            raise

    return wrapper


def exception_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.HTTPError as e:
            if e.response.content:
                logger.error(f"错误信息：{json.loads(e.response.content)}")

            if e.response.status_code == 400:
                logger.error(f"请求错误，请检查请求参数：{e}")
            elif e.response.status_code == 401:
                logger.error(f"未登录：{e}")
            elif e.response.status_code == 504:
                logger.error(f"网关超时：{e} 尝试重试一次请求")
            else:
                logger.error(f"HTTP错误：{e}")
                raise

    return wrapper
