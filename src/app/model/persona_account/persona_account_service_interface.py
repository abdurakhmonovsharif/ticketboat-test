from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type
from app.model.persona_account.persona_account_client_interface import PersonaAccountClient


@dataclass
class PersonaSyncResult:
    """Result of persona sync for a single automator."""
    automator_id: str
    automator_name: str
    account_id: str
    successful: List[Dict[str, Any]]
    failed: List[Dict[str, Any]]
    error: Optional[str] = None


class PersonaAccountService(ABC):
    """
    Base class for persona account services.
    
    Receives a client CLASS (not instance) and instantiates it
    when needed with the appropriate api_key.
    """
    def __init__(
        self,
        persona_account_client: Type[PersonaAccountClient]
    ):
        self._client_class = persona_account_client

    @abstractmethod
    async def sync_account_to_automator(
        self,
        account_id: str,
        primary_ids: List[str],
        automator: Dict[str, Any]
    ) -> PersonaSyncResult:
        """
        Sync account's primaries to a single automator.
        
        Args:
            account_id: The account ID to sync
            primary_ids: List of primary IDs to sync for this account
            automator: Automator dict with id, name, brand, api_key
            
        Returns:
            PersonaSyncResult with success/failure details
        """
        pass