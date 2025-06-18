from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from alphapower.manager.alpha_profile_manager import AlphaProfileManagerFactory
from alphapower.manager.fast_expression_manager import FastExpressionManagerFactory


class AbstractAlphaProfilesService(ABC):
    """
    抽象基类，定义 AlphaProfileService 的接口。
    """

    @abstractmethod
    async def build_alpha_profiles(
        self,
        fast_expression_manager_factory: FastExpressionManagerFactory,
        alpha_profile_manager_factory: AlphaProfileManagerFactory,
        date_created_gt: Optional[datetime] = None,
        date_created_lt: Optional[datetime] = None,
        parallel: int = 1,
        **kwargs: Any,
    ) -> None:
        """
        Build alpha profiles.
        """
        raise NotImplementedError("build_alpha_profiles method not implemented")
