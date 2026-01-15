from typing import Dict, Tuple, Type

from app.model.persona_account.persona_account_client_interface import PersonaAccountClient
from app.model.persona_account.persona_account_service_interface import PersonaAccountService
from app.service.ticketsuite.ts_persona_client import TicketSuitePersonaAccountClient
from app.service.ticketsuite.ts_persona_service_copy import TicketSuitePersonaAccountService


class PersonaAccountFactory:
    """
    The Factory that decides which Persona Service to return.
    
    Returns a service instance initialized with the client CLASS.
    The service will instantiate the client when needed with the api_key.
    """

    # Static Registry to map brand strings to (ClientClass, ServiceClass)
    _registry: Dict[str, Tuple[Type[PersonaAccountClient], Type[PersonaAccountService]]] = {
        'ticketsuite': (TicketSuitePersonaAccountClient, TicketSuitePersonaAccountService)
    }

    @classmethod
    def get_service(
        cls,
        brand: str
    ) -> PersonaAccountService:
        """
        The Factory Method.
        
        Returns a service instance with the client CLASS injected.
        The service instantiates clients when it has the api_key.
        """
        classes = cls._registry.get(brand)

        if not classes:
            raise ValueError(f"No persona services registered for brand: {brand}")

        client_class, service_class = classes

        if not client_class:
            raise ValueError(f"No persona client registered for brand: {brand}")

        if not service_class:
            raise ValueError(f"No persona service registered for brand: {brand}")

        # Return service initialized with the client CLASS
        return service_class(persona_account_client=client_class)