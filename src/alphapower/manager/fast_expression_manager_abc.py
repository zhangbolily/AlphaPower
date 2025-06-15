from abc import ABC, abstractmethod
from typing import Any

from alphapower.constants import FastExpressionType


class AbstractFastExpressionManager(ABC):
    @abstractmethod
    def parse(self, expression: str, type: FastExpressionType) -> Any:
        pass