from abc import ABC, abstractmethod
from typing import Any, List

from alphapower.constants import UserPermission, UserRole
from alphapower.view.alpha import CreateTagsPayload, ListTagAlphaView
from alphapower.view.user import AuthenticationView


class AbstractWorldQuantBrainClient(ABC):
    """
    Abstract base class for clients.
    """

    @abstractmethod
    async def login(
        self, username: str, password: str, **kwargs: Any
    ) -> AuthenticationView:
        """
        Log in to the client with the given username and password.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def logout(self) -> None:
        """
        Log out of the client.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_user_id(self) -> str:
        """
        Get the user ID of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_user_permissions(self) -> List[UserPermission]:
        """
        Get the permissions of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_user_role(self) -> UserRole:
        """
        Get the role of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def create_alpha_list(self, payload: CreateTagsPayload) -> ListTagAlphaView:
        """
        Create an alpha list with the given payload.
        """
        raise NotImplementedError("Subclasses must implement this method.")
