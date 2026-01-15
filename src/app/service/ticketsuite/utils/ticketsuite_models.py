"""
TicketSuite API Models and Data Transfer Objects (DTOs)

This module contains all data models, configurations, and DTOs 
for interacting with the TicketSuite API.
"""
from enum import Enum
from typing import Dict, Any, List, Optional, TypeVar, Generic
from dataclasses import dataclass
from pydantic import BaseModel, Field


# ============================================================================
# Configuration Models
# ============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3  # DEFAULT_MAX_RETRIES + 1
    retry_delay: float = 10.0  # seconds
    retryable_status_codes: Optional[set[int]] = None
    
    def __post_init__(self):
        if self.retryable_status_codes is None:
            self.retryable_status_codes = {429}


# ============================================================================
# Enums
# ============================================================================

class TsResource(str, Enum):
    """Enum for TicketSuite API resources."""
    PERSONA = "personaAccount"


# ============================================================================
# Exception Models
# ============================================================================

class TsError(Exception):
    """Base exception for TicketSuite service errors."""
    pass


class TsClientError(TsError):
    """Exception for client errors (4xx) that should not be retried."""
    def __init__(self, message: str, status_code: int, response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


# ============================================================================
# Request DTOs
# ============================================================================

class TsProxyPayload(BaseModel):
    """Payload for updating TicketSuite persona proxy."""
    Host: str
    Port: int
    Username: str
    Password: str


# ============================================================================
# Response DTOs
# ============================================================================

class TsPersona(BaseModel):
    """
    Response model for a single persona from TicketSuite API.
    """
    # Core fields
    Id: Optional[str] = None
    Email: Optional[str] = None
    Password: Optional[str] = None
    AccessToken: Optional[str] = None
    Tags: Optional[str] = None
    
    StockTypePriority: Optional[str] = None
    InventoryTags: Optional[str] = None
    InternalNotes: Optional[str] = None
    RoleTags: Optional[str] = None
    SyncToAxsResale: Optional[bool] = None
    SyncToTradedesk: Optional[bool] = None
    Proxy: Optional[Dict[str, Any]] = None
    PhoneNumber: Optional[Dict[str, Any]] = None
    PosVendorId: Optional[str] = None
    CreatedDate: Optional[str] = None
    LastModifiedDate: Optional[str] = None
    
    # Allow extra fields from API that we might not have mapped
    class Config:
        extra = "allow"


# Generic type variable for response data
T = TypeVar('T')


class TsResponse(BaseModel, Generic[T]):
    """
    Generic response wrapper for TicketSuite API operations.
    
    This is a generic type that can be used for any TicketSuite API response:
    - TsResponse[TsPersona] - Single persona (create, update)
    - TsResponse[List[TsPersona]] - Multiple personas (get/search)
    
    Structure matches TicketSuite API format:
    {
        "Message": "Success",
        "Code": 200,
        "Result": <T>  # Generic type - can be single object or list
    }
    
    Usage:
        # For create (single persona)
        response: TsResponse[TsPersona] = TsResponse(**api_response)
        
        # For get (list of personas)
        response: TsResponse[List[TsPersona]] = TsResponse(**api_response)
    """
    Message: Optional[str] = None
    Code: Optional[int] = None
    Result: Optional[T] = None
    
    class Config:
        extra = "allow"


# Type aliases for common use cases
GetTsPersonaResponse = TsResponse[List[TsPersona]]
CreateTsPersonaResponse = TsResponse[TsPersona]
UpdateTsPersonaResponse = TsResponse[TsPersona]


class TsPersonaProxyUpdate(BaseModel):
    """
    Response model for persona proxy update from TicketSuite API.
    All fields are optional except Id.
    """
    Id: str  # Required
    Email: Optional[str] = None
    Password: Optional[str] = None
    AccessToken: Optional[str] = None
    Tags: Optional[str] = None
    StockTypePriority: Optional[str] = None
    InventoryTags: Optional[str] = None
    InternalNotes: Optional[str] = None
    SyncToAxsResale: Optional[bool] = None
    SyncToTradedesk: Optional[bool] = None
    Proxy: Optional[Dict[str, Any]] = None
    Nimble: Optional[Dict[str, Any]] = None
    
    class Config:
        extra = "allow"


# Type alias for proxy update response
UpdateTsPersonaProxyResponse = TsResponse[TsPersonaProxyUpdate]