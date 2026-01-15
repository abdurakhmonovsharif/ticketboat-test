"""
Persona Orchestrator - Coordinates persona creation across multiple brands/automators.

This module is brand-agnostic and delegates all brand-specific work to services
obtained from PersonaAccountFactory.
"""
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.model.ams_models import AccountPrimaryPayload
from app.model.persona_account.persona_account_factory import PersonaAccountFactory
from app.model.persona_account.persona_account_service_interface import PersonaSyncResult
from app.db.ams_db import get_accounts_with_primaries


logger = logging.getLogger(__name__)


async def get_accounts_automators_with_api_key(account_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get automators for accounts including api_key.
    
    Returns:
        Dict mapping account_id to list of automators with {id, name, brand, api_key}
    """
    from app.database import get_pg_readonly_database
    
    query = """
        SELECT
            a.id as account_id,
            am.id::varchar as automator_id,
            am.name as automator_name,
            am.brand,
            am.api_key
        FROM ams.ams_account a
        LEFT JOIN ams.account_automator_mapping aam ON aam.account_id = a.id
        LEFT JOIN ams.automator am ON am.id = aam.automator_id
        WHERE a.id = ANY(:account_ids)
    """
    rows = await get_pg_readonly_database().fetch_all(
        query=query, values={"account_ids": account_ids}
    )
    
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        account_id = str(row["account_id"])
        grouped.setdefault(account_id, [])
        if row["automator_id"]:
            grouped[account_id].append({
                "id": row["automator_id"],
                "name": row["automator_name"],
                "brand": row["brand"],
                "api_key": row["api_key"]
            })
    
    return grouped


class PersonaOrchestrator:
    """
    Orchestrates persona creation across accounts and automators.
    
    This class is brand-agnostic. For each account:
      - Gets automators (with brand, api_key)
      - For each automator:
        - Gets service from factory by brand
        - Calls service to sync primaries
      - Aggregates all results
    """
    
    async def create_personas(
        self,
        payload: List[AccountPrimaryPayload]
    ) -> List[Dict[str, Any]]:
        """
        Main entry point for persona creation.
        
        Args:
            payload: List of {account_id, primary_ids} pairs
            
        Returns:
            List of sync response items grouped by account
        """
        if not payload:
            return []
        
        # Collect all account IDs
        account_ids = [item.account_id for item in payload]
        
        # Build account -> primary_ids mapping
        account_primary_map = {
            item.account_id: item.primary_ids
            for item in payload
        }
        
        # Get automators for all accounts (with api_key)
        accounts_automators = await get_accounts_automators_with_api_key(account_ids)
        
        # Get account data with primaries for response transformation
        account_primaries = await get_accounts_with_primaries(account_ids)
        account_data_map = {acc.account_id: acc for acc in account_primaries}
        
        all_results: List[PersonaSyncResult] = []
        accounts_without_automator: List[str] = []
        
        # Process each account
        for account_id, primary_ids in account_primary_map.items():
            automators = accounts_automators.get(account_id, [])
            
            if not automators:
                accounts_without_automator.append(account_id)
                logger.warning(f"Account {account_id} has no automators assigned")
                # Add error result for this account
                all_results.append(PersonaSyncResult(
                    automator_id="",
                    automator_name="",
                    account_id=account_id,
                    successful=[],
                    failed=[{
                        "account_id": account_id,
                        "error": "No automators assigned to this account"
                    }],
                    error="No automators assigned"
                ))
                continue
            
            # Sync to each automator
            for automator in automators:
                result = await self._sync_to_automator(
                    account_id=account_id,
                    primary_ids=primary_ids,
                    automator=automator
                )
                all_results.append(result)
        
        # Transform results to response format
        return self._transform_to_response(all_results, account_data_map)
    
    async def _sync_to_automator(
        self,
        account_id: str,
        primary_ids: List[str],
        automator: Dict[str, Any]
    ) -> PersonaSyncResult:
        """
        Sync account's primaries to a single automator using appropriate service.
        
        Gets the service from factory based on automator's brand.
        """
        brand = automator.get("brand")
        automator_id = automator.get("id", "")
        automator_name = automator.get("name", automator_id)
        
        if not brand:
            error_msg = f"Automator {automator_name} has no brand configured"
            logger.error(error_msg)
            return PersonaSyncResult(
                automator_id=automator_id,
                automator_name=automator_name,
                account_id=account_id,
                successful=[],
                failed=[{"account_id": account_id, "error": error_msg}],
                error=error_msg
            )
        
        try:
            # Get the right service for this brand
            service = PersonaAccountFactory.get_service(brand)
            
            # Delegate to brand-specific service
            result = await service.sync_account_to_automator(
                account_id=account_id,
                primary_ids=primary_ids,
                automator=automator
            )
            return result
            
        except ValueError as e:
            # Brand not registered in factory
            error_msg = f"No service registered for brand: {brand}"
            logger.error(error_msg)
            return PersonaSyncResult(
                automator_id=automator_id,
                automator_name=automator_name,
                account_id=account_id,
                successful=[],
                failed=[{"account_id": account_id, "error": error_msg}],
                error=error_msg
            )
        except Exception as e:
            error_msg = f"Error syncing to automator {automator_name}: {str(e)}"
            logger.error(error_msg)
            return PersonaSyncResult(
                automator_id=automator_id,
                automator_name=automator_name,
                account_id=account_id,
                successful=[],
                failed=[{"account_id": account_id, "error": error_msg}],
                error=error_msg
            )
    
    def _transform_to_response(
        self,
        results: List[PersonaSyncResult],
        account_data_map: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Transform PersonaSyncResults to API response format.
        
        Groups results by account and formats as:
        [
            {
                "account_id": "...",
                "email_address": "...",
                "sync_results": [
                    {"primary_name": "...", "status": "success/error", ...}
                ]
            }
        ]
        """
        # Build primary_id -> primary_name mapping
        primary_name_map: Dict[str, str] = {}
        for account in account_data_map.values():
            for primary in getattr(account, 'primaries', []) or []:
                if primary.id:
                    primary_name_map[primary.id] = primary.primary_name
        
        # Group results by account
        account_results: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        for result in results:
            account_id = result.account_id
            
            # Process successful
            for persona in result.successful:
                primary_id = persona.get("primary_id")
                account_results[account_id].append({
                    "primary_name": primary_name_map.get(primary_id),
                    "status": "success",
                    "status_code": 200,
                    "response": persona.get("response", "Persona created successfully"),
                    "error": None
                })
            
            # Process failed
            for persona in result.failed:
                primary_id = persona.get("primary_id")
                # Use primary_name from persona if available, otherwise lookup from map
                primary_name = persona.get("primary_name") or primary_name_map.get(primary_id)
                account_results[account_id].append({
                    "primary_name": primary_name,
                    "status": "error",
                    "status_code": None,
                    "response": None,
                    "error": persona.get("error", "Unknown error")
                })
        
        # Build final response
        response = []
        for account_id, sync_results in account_results.items():
            account_data = account_data_map.get(account_id)
            email_address = getattr(account_data, 'email_address', "") if account_data else ""
            
            response.append({
                "account_id": account_id,
                "email_address": email_address or "",
                "sync_results": sync_results
            })
        
        return response


def get_persona_orchestrator() -> PersonaOrchestrator:
    """Get a PersonaOrchestrator instance."""
    return PersonaOrchestrator()

