from abc import ABC, abstractmethod
from typing import Any

from alphapower.constants import UserPermission, UserRole
from alphapower.view.user import AuthenticationView


class AbstractClient(ABC):
    """
    Abstract base class for clients.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the client with the given arguments.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def __aenter__(self) -> "AbstractClient":
        """
        Asynchronous context manager entry method.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """
        Asynchronous context manager exit method.
        """
        raise NotImplementedError("Subclasses must implement this method.")

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
    async def close(self) -> None:
        """
        Close the client connection.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_user_id(self) -> str:
        """
        Get the user ID of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def get_user_permissions(self) -> list[UserPermission]:
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
