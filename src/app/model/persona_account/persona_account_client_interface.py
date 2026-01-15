from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

import httpx
from pydantic import BaseModel

from app.service.ticketsuite.utils.ticketsuite_models import RetryConfig


class PersonaAccountClient(ABC):
    """
    Abstract interface for persona management clients.
    
    This is a generic interface that doesn't depend on any specific
    implementation's data structures. All methods use Dict[str, Any]
    to remain agnostic of the underlying system.
    
    Implementations:
    - TicketSuitePersonaAccountClient (uses TicketSuite-specific DTOs internally)
    - TaciyonPersonaClient (future implementation)
    - Other automator clients...
    """

    def __init__(
        self,
        api_key: str,
        timeout: Optional[float] = None,
        retry_config: Optional[RetryConfig] = None,
        client: Optional[httpx.AsyncClient] = None
    ):
        self.api_key = api_key
        self.timeout = timeout
        self.retry_config = retry_config
        self._client = client
    
    @abstractmethod
    async def create(self, persona_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new persona.
        
        Args:
            persona_payload: Dictionary containing persona data
            
        Returns:
            Dictionary with creation result
        """
        pass

    @abstractmethod
    async def create_in_batch(
        self,
        personas: list[BaseModel],
    ) -> Dict[str, Any]:
        """
        Create multiple personas in batch.

        Args:
            personas: List of Persona Accounts

        Returns:
            Dictionary with:
                - successful: List of successful creation results with persona_id, email_id, primary_id
                - failed: List of failed attempts with error details
                - total: Total number of personas processed
                - success_count: Number of successful creations
                - failure_count: Number of failed creations
        """

        pass
    
    @abstractmethod
    async def get(
        self, 
        persona_id: Optional[str] = None, 
        email: Optional[str] = None,
        page_number: int = 0,
        page_size: int = 10
    ) -> List[Any]:
        """
        Get persona(s) from the system.
        
        Args:
            persona_id: Optional persona ID to fetch specific persona
            email: Optional email to search by
            page_number: Page number for pagination
            page_size: Number of results per page
            
        Returns:
            List of persona items
        """
        pass
    
    @abstractmethod
    async def update(
        self, 
        persona_id: str, 
        persona_payload: Dict[str, Any]
    ) -> Any:
        """
        Update an existing persona.
        
        Args:
            persona_id: ID of the persona to update
            persona_payload: Dictionary with updated persona data
        """
        pass
    
    @abstractmethod
    async def delete(self, persona_id: str) -> Any:
        """
        Delete a persona.
        
        Args:
            persona_id: ID of the persona to delete
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """
        Clean up resources and close connections.
        """
        pass

