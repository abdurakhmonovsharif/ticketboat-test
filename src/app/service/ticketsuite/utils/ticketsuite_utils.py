"""
TicketSuite API Utilities

This module contains general utility functions for working with the TicketSuite API,
such as response parsing, error handling, and common API operations.
"""
import logging
from typing import Dict, Any

import httpx

from app.service.ticketsuite.utils.ticketsuite_models import TsError

logger = logging.getLogger(__name__)


async def parse_json_response(response: httpx.Response) -> Dict[str, Any]:
    """
    Parse JSON response from TicketSuite API with error handling.
    
    Args:
        response: HTTP response object from httpx
        
    Returns:
        Parsed JSON as dictionary
        
    Raises:
        TsError: If JSON parsing fails

    """
    try:
        return response.json()
    except Exception as e:
        response_text = response.text[:200] if hasattr(response, 'text') else str(response.content[:200])
        logger.error(f"Failed to parse TicketSuite JSON response: {response_text}")
        raise TsError(f"Invalid JSON response from TicketSuite: {str(e)}") from e

# Default configuration
DEFAULT_TIMEOUT = 30.0  # seconds
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_DELAY = 10.0  # seconds
DEFAULT_RETRYABLE_STATUS_CODES = {429}