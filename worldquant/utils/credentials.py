from worldquant import WorldQuantClient


def create_client(credentials, pool_connections=10, pool_maxsize=10):
    """
    创建 WorldQuant 客户端。

    参数:
    credentials (dict): 包含用户名和密码的凭据。

    返回:
    WorldQuantClient: 客户端实例。
    """
    client = WorldQuantClient(
        username=credentials["username"],
        password=credentials["password"],
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    return client
