import os
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
import time

from app.database import get_pg_readonly_database
from app.model.ts_config_models import TSCredentials

logger = logging.getLogger(__name__)


async def get_automators_by_ids(automator_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Get automator details for multiple automators including API keys.
    
    Args:
        automator_ids: List of automator UUIDs
        
    Returns:
        List of dictionaries with automator details
    """
    try:
        query = """
            SELECT 
                id::varchar,
                name,
                api_key
            FROM ams.automator
            WHERE id = ANY(:automator_ids)
            ORDER BY name ASC
        """
        results = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"automator_ids": automator_ids}
        )
        return [dict(row) for row in results]
    except Exception as e:
        print(f"Error fetching automators: {str(e)}")
        return []


class TSCredentialManager:
    """
    Manages TicketSuite credentials from database.
    
    Credentials are stored in ams.automator table with columns:
    - api_key: TicketSuite API key
    """
    
    def __init__(self):
        self._cache = {}
        self._cache_ttl = 120  # seconds
        
    
    async def _load_from_database(self, automator_id: UUID) -> Optional[TSCredentials]:
        """Load credentials from database"""
        try:
            automator_id_str = str(automator_id)
            logger.info(f"Fetching TicketSuite credentials for automator {automator_id} from database")
            
            result = await get_automators_by_ids([automator_id_str])
            
            if not result:
                logger.warning(f"No TicketSuite credentials found for automator {automator_id}")
                return None
            
            automator = result[0]
            if not automator.get('api_key'):
                logger.error(f"Automator {automator_id} ({automator.get('name')}) has no api_key configured")
                return None
            
            credentials = TSCredentials(api_key=automator['api_key'])
            
            logger.info(f"Successfully loaded TicketSuite credentials for automator {automator_id}")
            return credentials
            
        except Exception as e:
            logger.error(f"Unexpected error loading credentials for automator {automator_id}: {str(e)}")
            return None
        
    
    async def get_credentials_for_automator(self, automator_id: UUID) -> Optional[TSCredentials]:
        """
        Get TicketSuite credentials for a specific automator from database.
        
        Args:
            automator_id: UUID of the automator
            
        Returns:
            TSCredentials object, or None if not found
        """
        cache_key = str(automator_id)
        if cache_key in self._cache:
            cached_creds, cached_time = self._cache[cache_key]
            if time.time() - cached_time < self._cache_ttl:
                logger.debug(f"Using cached credentials for automator {automator_id}")
                return cached_creds
        
        credentials = await self._load_from_database(automator_id)
        if credentials:
            self._cache[cache_key] = (credentials, time.time())
        
        return credentials

    
    def clear_cache(self):
        """Clear the credentials cache"""
        self._cache.clear()
        logger.info("Cleared TicketSuite credentials cache")


ts_credential_manager = TSCredentialManager()

