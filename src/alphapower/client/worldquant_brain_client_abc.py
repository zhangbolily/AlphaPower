from abc import ABC, abstractmethod
from typing import Any, List, Optional

from alphapower.client.common_view import TableView
from alphapower.constants import (
    CorrelationType,
    RecordSetType,
    UserPermission,
    UserRole,
)
from alphapower.view.alpha import (
    AlphaDetailView,
    AlphaPropertiesPayload,
    CreateTagsPayload,
    SelfTagListQuery,
    SelfTagListView,
    TagView,
    UserAlphasQuery,
    UserAlphasSummaryView,
    UserAlphasView,
)
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
    async def create_alpha_list(self, payload: CreateTagsPayload) -> TagView:
        """
        Create an alpha list with the given payload.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def delete_alpha_list(self, tag_id: str) -> None:
        """
        Delete an alpha list with the given tag ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_user_tags(self, query: SelfTagListQuery) -> SelfTagListView:
        """
        Fetch the tags of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_user_alphas_summary(self) -> UserAlphasSummaryView:
        """
        Fetch the summary of the alphas of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_user_alphas(self, query: UserAlphasQuery) -> UserAlphasView:
        """
        Fetch the alphas of the logged-in user.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    # Alpha 对象操作
    @abstractmethod
    async def update_alpha_properties(
        self,
        alpha_id: str,
        payload: AlphaPropertiesPayload,
    ) -> AlphaDetailView:
        """
        Update the properties of an alpha with the given ID and payload.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_alpha_correlation(
        self,
        alpha_id: str,
        correlation_type: CorrelationType,
        override_retry_after: Optional[float] = None,
    ) -> TableView:
        """
        Fetch the correlation data for the given alpha ID and correlation type.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    async def fetch_alpha_record_sets(
        self,
        alpha_id: str,
        record_set_type: RecordSetType,
        override_retry_after: Optional[float] = None,
    ) -> TableView:
        """
        Fetch the record sets for the given alpha ID and record set type.
        """
        raise NotImplementedError("Subclasses must implement this method.")
