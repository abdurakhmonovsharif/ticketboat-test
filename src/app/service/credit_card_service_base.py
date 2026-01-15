from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from uuid import UUID

from app.model.ams_models import (
    AccountData, 
    OrderCreditCardResponse, 
    CreditCardProvider
)

logger = logging.getLogger(__name__)


class CreditCardServiceBase(ABC):
    """Abstract base class for credit card provider services"""
    
    def __init__(self, provider: CreditCardProvider):
        self.provider = provider
        self.logger = logging.getLogger(f"{__name__}.{provider.value}")
    
    @abstractmethod
    async def create_credit_card(
        self, 
        account_data: AccountData, 
        credit_limit: float,
        nickname: Optional[str] = None,
        additional_params: Dict[str, Any] = {}
    ) -> OrderCreditCardResponse:
        """
        Create a credit card for the given account
        
        Args:
            account_data: Account information from AMS database
            credit_limit: Credit limit for the card
            nickname: Optional nickname for the credit card
            additional_params: Provider-specific parameters
            
        Returns:
            CreditCardCreationResponse with card details or error information
        """
        pass
    
    @abstractmethod
    def validate_account_data(self, account_data: AccountData) -> bool:
        """
        Validate that the account data contains all required fields for this provider
        
        Args:
            account_data: Account information to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def _log_request(self, account_id: str, action: str, **kwargs):
        """Log service request for audit purposes"""
        self.logger.info(
            f"Credit card {action} request",
            extra={
                "provider": self.provider.value,
                "account_id": account_id,
                "action": action,
                **kwargs
            }
        )
    
    def _log_response(self, account_id: str, success: bool, **kwargs):
        """Log service response for audit purposes"""
        level = logging.INFO if success else logging.ERROR
        self.logger.log(
            level,
            f"Credit card creation {'succeeded' if success else 'failed'}",
            extra={
                "provider": self.provider.value,
                "account_id": account_id,
                "success": success,
                **kwargs
            }
        )
    
    def _create_error_response(
        self, 
        account_id: str, 
        error_message: str, 
        error_code: str = ""
    ) -> OrderCreditCardResponse:
        """Create a standardized error response"""
        from uuid import UUID
        return OrderCreditCardResponse(
            success=False,
            provider=self.provider,
            account_id=UUID(account_id),
            error_message=error_message,
            error_code=error_code
        )
    
    def _create_success_response(
        self,
        account_id: str,
        card_number: str,
        expiry_date: str,
        cvc: str,
        account_token: str = "",
        provider_data: Dict[str, Any] = {}
    ) -> OrderCreditCardResponse:
        """Create a standardized success response"""
        return OrderCreditCardResponse(
            success=True,
            provider=self.provider,
            account_id=UUID(account_id),
            card_number=card_number,
            expiry_date=expiry_date,
            cvc=cvc,
            account_token=account_token,
            provider_data=provider_data
        )
