import hashlib
from typing import Any

from .core import Expression


class ExpressionCache:
    def __init__(self):
        self.cache = {}

    def get(self, key: str):
        return self.cache.get(key)

    def put(self, key: str, value: Any):
        self.cache[key] = value

    def _generate_key(self, expression: Expression) -> str:
        return hashlib.sha256(
            f"{expression.op}{expression.args}{expression.params}".encode()
        ).hexdigest()
