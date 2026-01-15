"""
TicketSuite Persona Creation Utilities

This module contains utility functions for building and formatting
persona data for the TicketSuite API.
"""
import logging
from typing import Dict, Any, Optional

from app.model.ams_models import PhoneProviderType, TicketSuitePersonaPayload, TicketSuitePhone

logger = logging.getLogger(__name__)

def build_success_result(
    persona: TicketSuitePersonaPayload,
    persona_id: str,
    email: str,
    tags: str
) -> Dict[str, Any]:
    """
    Build success result dictionary with metadata from Pydantic model.
    
    Extracts metadata directly from the Pydantic model attributes because
    these fields may be excluded when serializing with exclude_none=True.
    
    Args:
        persona: The TicketSuite persona payload (Pydantic model)
        persona_id: The created persona ID from API response
        email: Email address
        tags: Tags/primary codes
        
    Returns:
        Dictionary with persona metadata for tracking
    """
    return {
        "persona_id": persona_id,
        "email_id": persona.email_id or None,
        "primary_id": persona.primary_id or None,
        "automator_id": persona.automator_id or None,
        "account_id": persona.account_id or None,
        "email": email,
        "tags": tags
    }


def build_failure_result(
    persona: TicketSuitePersonaPayload,
    email: str,
    tags: str,
    error_msg: str,
    error_type: str,
    status_code: Optional[int] = None,
    response: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build failure result dictionary with metadata from Pydantic model.
    
    Extracts metadata directly from the Pydantic model attributes because
    these fields may be excluded when serializing with exclude_none=True.
    
    Args:
        persona: The TicketSuite persona payload (Pydantic model)
        email: Email address
        tags: Tags/primary codes
        error_msg: Error message
        error_type: Type of error (missing_id, client_error, server_error, unexpected_error)
        status_code: HTTP status code (optional, for client errors)
        response: API response (optional, for client errors)
        
    Returns:
        Dictionary with error details and persona metadata for tracking
    """
    result = {
        "email_id": persona.email_id or None,
        "primary_id": persona.primary_id or None,
        "account_id": persona.account_id or None,
        "email": email,
        "tags": tags,
        "error": error_msg,
        "error_type": error_type
    }
    
    if status_code is not None:
        result["status_code"] = status_code
    if response is not None:
        result["response"] = response
        
    return result


def format_phone_number(phone: int | str) -> str:
    """
    Convert an integer or string phone number like 12694371155
    into formatted string like +1(269) 437-1155.
    Works for US-style numbers with optional country code.
    
    Args:
        phone: Phone number as int or string
        
    Returns:
        Formatted phone number string
        
    Raises:
        ValueError: If phone number format is invalid
    """
    digits = ''.join(filter(str.isdigit, str(phone)))
    
    if len(digits) == 11:
        country = f"+{digits[0]}"
        digits = digits[1:]
    elif len(digits) == 10:
        country = "+1"
    else:
        raise ValueError(f"Invalid phone number format: {phone}")
    
    area, first3, last4 = digits[:3], digits[3:6], digits[6:]
    return f"{country}({area}) {first3}-{last4}"


def build_persona_payload(
    account: dict,
    primary: dict,
    email_id: Optional[str] = '',
    primary_id: Optional[str] = '',
    automator_id: Optional[str] = '',
    account_id: Optional[str] = '',
) -> TicketSuitePersonaPayload:
    """
    Build a TicketSuite persona payload from account and primary data.
    Incorporates logic for Juiced accounts and Shadows accounts.
    
    Args:
        account: Account dictionary with fields like account_nickname, is_shadows, email_address, phone, proxy
        primary: Primary dictionary with fields like is_juiced, password, primary_code
        email_id: Used when result is successful
        primary_id: Used when result is successful
        automator_id: Used when result is successful
        account_id: Used when result is successful
    Returns:
        Dictionary ready to be used as TicketSuite persona payload
    """
    primary_code = primary["primary_code"]
    sync_to_tradedesk = False
    sync_to_axs_resale = primary_code == "axsmt"
    
    if primary.get("is_juiced", False):
        internal_notes = f"{account['account_nickname']} Juiced Account - Never Transfer/Crawl - Wallet Links Only - NOTM NOTP NOTND NOEVO"
        role_tags = "Juiced Acct"
        stock_type_priority = "Paperless"
        inventory_tags = f"{account['account_nickname']}, Juiced, no-tdmt, QCN"
    else:
        internal_notes = f"{account['account_nickname']} NOALL" if account.get("is_shadows", False) else ""
        role_tags = ""
        stock_type_priority = ""
        inventory_tags = f"{account['account_nickname']}, QCN"

    phone_data = build_phone_data(account.get("phone", {}))
     
    return TicketSuitePersonaPayload(
        Email=account["email_address"],
        Password=primary.get("password", ""),
        Tags=primary_code,
        InternalNotes=internal_notes,
        RoleTags=role_tags,
        AccessToken="",
        StockTypePriority=stock_type_priority,
        Proxy=account.get("proxy", ""),
        SyncToAxsResale=sync_to_axs_resale,
        SyncToTradedesk=sync_to_tradedesk,
        InventoryTags=inventory_tags,
        PhoneNumber=phone_data,
        PosVendorId="",
        email_id=email_id,
        primary_id=primary_id,
        automator_id=automator_id,
        account_id=account_id,
    )


def build_phone_data(phone_data: dict) -> Optional[TicketSuitePhone]:
    """
    Build a TicketSuite phone data dictionary from phone data.
    Returns None if phone data is missing or incomplete.
    """
    data = (phone_data or {}).copy()
    
    # If no phone data at all, return None
    if not data:
        return None
    
    phone_number = data.get("PhoneNumber")
    
    # If no phone number, return None
    if not phone_number:
        return None
    
    try:
        data["PhoneNumber"] = format_phone_number(phone_number)
    except ValueError as e:
        logger.warning(f"Invalid phone number format '{phone_number}': {e}")
        return None

    provider_map = {
        "WiredSMS": "Wired",
        "Text Chest": "TextChest"
    }
    provider_type = data.get("ProviderType")
    current_provider = data.get("Provider")

    if provider_type == PhoneProviderType.PHYSICAL_PHONE:
        data["Provider"] = "PhoneToEmail"
    elif current_provider in provider_map:
        data["Provider"] = provider_map[current_provider]
    
    # If provider is still missing, return None
    if not data.get("Provider"):
        logger.warning(f"Phone data missing Provider field")
        return None
    
    # Set defaults for boolean fields
    data.setdefault("IsRotation", False)
    data.setdefault("IsEnabled", True)

    return TicketSuitePhone(**data)

