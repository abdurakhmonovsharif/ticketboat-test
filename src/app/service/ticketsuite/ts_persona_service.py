import asyncio
import logging
import traceback
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum

import httpx

from app.db.ams_db import (
    get_accounts_with_primaries,
    get_automators_by_ids,
    validate_accounts_for_ts_sync,
    get_primary_ids_by_code,
    create_primary_account_mapping,
    get_accounts_data_by_ids,
)
from app.model.ams_models import TicketSuitePersonaPayload, AccountPrimary
from app.model.persona_account.persona_account_client_interface import PersonaAccountClient
from app.service.ticketsuite.ts_persona_client import get_ticketsuite_persona_client, TicketSuitePersonaAccountClient
from app.service.ticketsuite.utils.persona_creation_utils import build_persona_payload
from app.db.ams_db import update_ticketsuite_persona_ids


logger = logging.getLogger(__name__)


class AutomatorBrand(str, Enum):
    """Enum for different automator brands."""
    TICKETSUITE = "ticketsuite"
    TACIYON = "taciyon"


class CompanyType(str, Enum):
    """Company types for different business entities."""
    TICKETBOAT = "ticketboat"
    TICKETBOAT_INTL = "ticketboat_intl"
    SHADOWS = "shadows"
    SHADOWS_INTL = "shadows_intl"


TICKETBOAT_COMPANIES = ["Ticket Boat", "Ticket Boat Intl"]
SHADOWS_COMPANIES = ["Shadows", "Shadows Intl"]

# Default primaries for Ticketboat accounts
DEFAULT_PRIMARIES = [
    {"primary_code": "98159c6e-f86e-4c50-a6d7-901677c59c3b", "password": "Ticketboat1234!"},
    {"primary_code": "axsmt", "password": ""},
    {"primary_code": "tmmt", "password": ""},
    {"primary_code": "sgmt", "password": ""}
]

# Initial creation primaries (only "All But Ticketmaster")
INITIAL_CREATION_PRIMARIES = [
    {"primary_code": "98159c6e-f86e-4c50-a6d7-901677c59c3b", "password": "Ticketboat1234!"}
]


@dataclass
class PersonaCreationResult:
    """Result of persona creation for a single automator."""
    automator_id: str
    automator_name: str
    successful: List[Dict[str, Any]]
    failed: List[Dict[str, Any]]
    error: Optional[str] = None


@dataclass
class PersonaCreationSummary:
    """Overall summary of persona creation across all automators."""
    total_successful: int
    total_failed: int
    successful_personas: List[Dict[str, Any]]
    failed_personas: List[Dict[str, Any]]
    accounts_without_automator: List[str]
    automator_errors: List[Dict[str, Any]]

# TsPersonaAccountService
class PersonaCreationService:
    """
    Service for creating personas across multiple automator systems.
    
    Supports multiple automator brands (TicketSuite, Taciyon, etc.) with
    different API implementations for each brand. Handles batch creation,
    error tracking, and database persistence.
    
    Architecture:
    - Each account can have multiple automators
    - Each automator belongs to a unique brand per account
    - Personas are created in ALL automators tied to an account
    - API keys are fetched from automator.api_key column in database
    """
    # , persona_account_client: PersonaAccountClient
    def __init__(self):
        self.timeout = httpx.Timeout(timeout=60)
        # self.api_client = persona_account_client
        
    def _account_label(self, account: Dict[str, Any]) -> str:
        """Get human-friendly account identifier for logging."""
        return (
            account.get("account_nickname")
            or account.get("nickname")
            or account.get("email_address")
            or account.get("account_id")
            or "unknown account"
        )

    def _is_ticketboat_company(self, company_name: str) -> bool:
        """Check if company is a Ticketboat company."""
        return company_name in TICKETBOAT_COMPANIES

    def _is_shadows_company(self, company_name: str) -> bool:
        """Check if company is a Shadows company."""
        return company_name in SHADOWS_COMPANIES

    def _should_skip_primary(self, primary: Any) -> bool:
        """Check if primary should be skipped based on validation rules."""
        if primary.added_to_ts or primary.missing_fields:
            return True
        if not primary.id:
            logger.warning(f"Primary {primary.primary_name} has no id, skipping")
            return True
        return False

    def _build_account_dict(self, account: Any) -> Dict[str, Any]:
        """Build account dictionary with required fields."""
        account_dict = account.model_dump()
        account_dict["account_nickname"] = account.account_nickname
        account_dict["is_shadows"] = account.is_shadows
        account_dict["phone"] = account.phone
        account_dict["proxy"] = account.proxy
        return account_dict

    def _build_primary_dict(self, primary: Any) -> Dict[str, Any]:
        """Build primary dictionary with required fields."""
        primary_dict = primary.model_dump()
        primary_dict["is_juiced"] = primary.is_juiced
        return primary_dict

    def _group_personas_by_automator(
        self,
        account_primaries: List[Any],
        account_automator_map: Dict[str, List[str]],
        primary_ids_filter: Optional[Set[str]] = None
    ) -> tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
        """
        Group personas by their automator IDs.
        
        Creates personas in ALL automators assigned to an account.
        Each persona (email + primary code) will be created in EACH automator.
        
        Args:
            account_primaries: List of accounts with their primaries
            account_automator_map: Mapping of account_id to automator_ids
            primary_ids_filter: Optional set of specific primary IDs to include
        
        Returns:
            Tuple of (personas_by_automator, accounts_without_automator)
        """
        personas_by_automator = {}
        accounts_without_automator = []
        
        for account in account_primaries:
            if not account.email_id:
                logger.warning(f"Account {account.account_id} has no email_id, skipping")
                continue
            
            automator_ids = account_automator_map.get(account.account_id, [])
            if not automator_ids:
                accounts_without_automator.append(account.account_id)
                logger.warning(f"Account {account.account_id} has no automator assigned, skipping")
                continue
            
            # Create personas in ALL automators assigned to this account
            for automator_id in automator_ids:
                self._add_account_personas_to_automator(
                    account=account,
                    automator_id=automator_id,
                    personas_by_automator=personas_by_automator,
                    primary_ids_filter=primary_ids_filter
                )
            
            if len(automator_ids) > 1:
                logger.info(
                    f"Account {account.account_id} has {len(automator_ids)} automators. "
                    f"Creating personas in all {len(automator_ids)} automators."
                )

        return personas_by_automator, accounts_without_automator

    def _add_account_personas_to_automator(
        self,
        account: Any,
        automator_id: str,
        personas_by_automator: Dict[str, List[Dict[str, Any]]],
        primary_ids_filter: Optional[Set[str]] = None
    ) -> None:
        """
        Add account primaries to persona groups for a single automator.
        
        Args:
            account: Account object with primaries
            automator_id: Single automator ID to create personas in
            personas_by_automator: Dictionary to populate with persona data
            primary_ids_filter: Optional set of specific primary IDs to include
        """
        account_dict = self._build_account_dict(account)
        
        for primary in account.primaries:
            # Skip primaries that don't pass basic validation
            if self._should_skip_primary(primary):
                continue
            
            # Apply primary filtering if provided
            if primary_ids_filter and primary.id not in primary_ids_filter:
                continue
            
            if automator_id not in personas_by_automator:
                personas_by_automator[automator_id] = []
            
            personas_by_automator[automator_id].append({
                "account": account_dict,
                "primary": self._build_primary_dict(primary),
                "email_id": account.email_id,
                "primary_id": primary.id,
                "automator_id": automator_id,
                "account_id": account.account_id  # Add account_id for response grouping
            })

    async def _create_personas_in_ticketsuite(
        self,
        automator_id: str,
        automator_name: str,
        api_key: str,
        persona_data_list: List[Dict[str, Any]]
    ) -> PersonaCreationResult:
        """
        Create personas in TicketSuite for a specific automator.
        
        Args:
            automator_id: Automator UUID
            automator_name: Human-readable automator name
            api_key: API key for this automator
            persona_data_list: List of persona data to create
        
        Returns:
            PersonaCreationResult with success/failure details
        """
        try:
            ts_service = get_ticketsuite_persona_client(api_key=api_key)
            
            personas_to_create = [
                self._build_ticketsuite_persona_payload(persona_data, automator_id)
                for persona_data in persona_data_list
            ]
            
            if not personas_to_create:
                return PersonaCreationResult(
                    automator_id=automator_id,
                    automator_name=automator_name,
                    successful=[],
                    failed=[]
                )
            
            logger.info(
                f"Creating {len(personas_to_create)} personas in TicketSuite "
                f"automator {automator_name} ({automator_id})"
            )
            
            async with ts_service:
                batch_results = await ts_service.create_in_batch(personas_to_create)


                if batch_results["successful"]:
                    await update_ticketsuite_persona_ids(batch_results["successful"])
                
                if batch_results["failed"]:
                    logger.warning(
                        f"{len(batch_results['failed'])} personas failed for "
                        f"automator {automator_name}"
                    )
                
                return PersonaCreationResult(
                    automator_id=automator_id,
                    automator_name=automator_name,
                    successful=batch_results["successful"],
                    failed=batch_results["failed"]
                )
                
        except Exception as e:
            error_msg = f"Error creating personas in TicketSuite: {str(e)}"
            logger.error(error_msg)
            traceback.print_exc()
            
            return PersonaCreationResult(
                automator_id=automator_id,
                automator_name=automator_name,
                successful=[],
                failed=[
                    {
                        "email_id": pd["email_id"],
                        "primary_id": pd["primary_id"],
                        "error": error_msg
                    }
                    for pd in persona_data_list
                ],
                error=error_msg
            )

    def _build_ticketsuite_persona_payload(
        self,
        persona_data: Dict[str, Any],
        automator_id: str
    ) -> TicketSuitePersonaPayload:
        """
        Build TicketSuite persona payload with metadata.
        
        Args:
            ts_service: TicketSuite service instance (kept for backward compatibility)
            persona_data: Dict containing account, primary, email_id, primary_id, account_id data
            automator_id: Automator ID to include in metadata
        
        Returns:
            Persona payload ready for TicketSuite API
        """
        payload = build_persona_payload(
            account=persona_data["account"],
            primary=persona_data["primary"],
            email_id=persona_data["email_id"],
            primary_id=persona_data["primary_id"],
            automator_id=automator_id or persona_data["automator_id"],
            account_id=persona_data.get("account_id"),  # Include account_id
        )

        return payload

    async def _create_personas_in_automator(
        self,
        automator: Dict[str, Any],
        persona_data_list: List[Dict[str, Any]]
    ) -> PersonaCreationResult:
        """
        Create personas in a specific automator based on its brand.
        
        Args:
            automator: Automator dict with id, name, api_key, brand
            persona_data_list: List of persona data to create
        
        Returns:
            PersonaCreationResult with success/failure details
        """
        automator_id = automator["id"]
        automator_name = automator.get("name", automator_id)
        api_key = automator.get("api_key")

        if not api_key:
            error_msg = f"No API key configured for automator {automator_name}"
            logger.error(error_msg)
            return PersonaCreationResult(
                automator_id=automator_id,
                automator_name=automator_name,
                successful=[],
                failed=[
                    {
                        "email_id": pd["email_id"],
                        "primary_id": pd["primary_id"],
                        "error": error_msg
                    }
                    for pd in persona_data_list
                ],
                error=error_msg
            )

        # For now, only TicketSuite is implemented
        # Future: Add support for Taciyon and other brands
        return await self._create_personas_in_ticketsuite(
            automator_id=automator_id,
            automator_name=automator_name,
            api_key=api_key,
            persona_data_list=persona_data_list
        )

    async def _process_personas_by_automators(
        self,
        personas_by_automator: Dict[str, List[Dict[str, Any]]],
        accounts_without_automator: List[str]
    ) -> PersonaCreationSummary:
        """
        Process persona creation for all automators and aggregate results.
        
        Args:
            personas_by_automator: Mapping of automator_id to persona data lists
            accounts_without_automator: List of account IDs without automators
        
        Returns:
            PersonaCreationSummary with aggregated results
        """
        # Fetch all automator details
        automator_ids = list(personas_by_automator.keys())
        automators_data = await get_automators_by_ids(automator_ids)
        automators_dict = {auto["id"]: auto for auto in automators_data}
        
        all_successful = []
        all_failed = []
        automator_errors = []
        
        # Process each automator
        for automator_id, persona_data_list in personas_by_automator.items():
            automator = automators_dict.get(automator_id)
            
            if not automator:
                error_msg = f"Automator {automator_id} not found in database"
                logger.error(error_msg)
                automator_errors.append({
                    "automator_id": automator_id,
                    "error": error_msg
                })
                all_failed.extend([
                    {
                        "email_id": pd["email_id"],
                        "primary_id": pd["primary_id"],
                        "error": error_msg
                    }
                    for pd in persona_data_list
                ])
                continue
            
            result = await self._create_personas_in_automator(automator, persona_data_list)
            
            all_successful.extend(result.successful)
            all_failed.extend(result.failed)
            
            if result.error:
                automator_errors.append({
                    "automator_id": result.automator_id,
                    "automator_name": result.automator_name,
                    "error": result.error
                })
        
        return PersonaCreationSummary(
            total_successful=len(all_successful),
            total_failed=len(all_failed),
            successful_personas=all_successful,
            failed_personas=all_failed,
            accounts_without_automator=accounts_without_automator,
            automator_errors=automator_errors
        )

    def _transform_summary_to_sync_response(
        self,
        summary: PersonaCreationSummary,
        account_primaries: List[AccountPrimary]
    ) -> List[Dict[str, Any]]:
        """
        Transform PersonaCreationSummary to TicketSuiteSyncResponse format.

        Args:
            summary: The persona creation summary with all results
            account_primaries: Original account data with email addresses

        Returns:
            List of sync response items grouped by account
        """
        from collections import defaultdict

        # Build account_id -> email_address mapping
        # Note: account_primaries are Pydantic models, use attribute access (.)
        account_email_map = {
            acc.account_id: acc.email_address or ""
            for acc in account_primaries
        }

        # Build primary_id -> primary_name mapping
        primary_name_map = {}
        for acc in account_primaries:
            for primary in acc.primaries or []:  # Access .primaries attribute
                if primary.id:
                    primary_name_map[primary.id] = primary.primary_name

        # Group results by account_id
        account_results = defaultdict(list)

        # Process successful personas
        for persona in summary.successful_personas:
            email_id = persona.get("email_id")  # No underscore
            primary_id = persona.get("primary_id")  # No underscore
            account_id = persona.get("account_id")

            # Find account_id from account_primaries if not directly available
            if not account_id:
                for acc in account_primaries:
                    if acc.email_id == email_id:  # Pydantic attribute access
                        account_id = acc.account_id
                        break

            if account_id:
                account_results[account_id].append({
                    "primary_name": primary_name_map.get(primary_id),
                    "status": "success",
                    "status_code": 200,
                    "response": persona.get("response", "Persona created successfully")
                })

        # Process failed personas
        for persona in summary.failed_personas:
            email_id = persona.get("email_id")
            primary_id = persona.get("primary_id")
            error = persona.get("error")
            account_id = persona.get("account_id")

            # Find account_id from account_primaries if not directly available
            if not account_id and email_id:
                for acc in account_primaries:
                    if acc.email_id == email_id:
                        account_id = acc.account_id
                        break

            if account_id:
                account_results[account_id].append({
                    "primary_name": primary_name_map.get(primary_id),
                    "status": "error",
                    "status_code": None,
                    "error": error
                })

        # Build final response
        response = []
        for account_id, sync_results in account_results.items():
            email_address = account_email_map.get(account_id, "")
            response.append({
                "account_id": account_id,
                "email_address": email_address,
                "sync_results": sync_results
            })

        return response

    async def create_personas_for_accounts(
        self,
        account_ids: List[str],
        primary_ids: Optional[List[str]] = None,
        all_primaries: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Create personas for accounts across their assigned automators.
        
        This is the main entry point for persona creation. It validates accounts,
        groups personas by automator, and creates them in each automator system.
        
        Args:
            account_ids: List of account IDs to create personas for
            primary_ids: List of specific primary IDs to create personas for
            all_primaries: If True, creates personas for ALL available primaries;
                          if False, only creates for specified primary_ids
        
        Returns:
            List of TicketSuiteSyncResponseItem with account_id, email_address, and sync_results
        """
        try:
            # Validate accounts have required fields (company, automator)
            validation_result = await validate_accounts_for_ts_sync(account_ids)
            
            if validation_result["invalid_accounts"]:
                # For invalid accounts, return error format
                invalid_response = []
                for invalid_acc in validation_result["invalid_accounts"]:
                    invalid_response.append({
                        "account_id": invalid_acc["account_id"],
                        "email_address": "",
                        "sync_results": [{
                            "primary_name": None,
                            "status": "error",
                            "status_code": None,
                            "error": f"Account validation failed: {', '.join(invalid_acc['missing_fields'])}"
                        }]
                    })
                return invalid_response
            
            # Get accounts with their primaries
            valid_accounts = validation_result["valid_accounts"]
            account_automator_map = {
                acc["account_id"]: acc.get("automator_ids", [])
                for acc in valid_accounts
            }
            
            account_ids_list = [acc["account_id"] for acc in valid_accounts]
            account_primaries = await get_accounts_with_primaries(account_ids_list)
            
            # Build primary filter set
            primary_ids_filter = set(primary_ids) if not all_primaries and primary_ids else None
            
            # Group personas by automator
            personas_by_automator, accounts_without_automator = self._group_personas_by_automator(
                account_primaries=account_primaries,
                account_automator_map=account_automator_map,
                primary_ids_filter=primary_ids_filter
            )
            
            # Process persona creation for all automators
            summary = await self._process_personas_by_automators(
                personas_by_automator,
                accounts_without_automator
            )

            logger.info(
                f"{ {'result_of_create_personas_for_accounts': summary} }"
            )
            
            # Transform to new response format
            return self._transform_summary_to_sync_response(summary, account_primaries)
            
        except Exception as e:
            logger.error(f"Error in create_personas_for_accounts: {e}")
            traceback.print_exc()
            raise

    async def create_personas_for_new_accounts(
        self,
        account_ids: List[str]
    ) -> None:
        """
        Create initial personas for newly created accounts.
        
        Rules:
        - Only creates "All But Ticketmaster" primary for Ticketboat accounts
        - Skips Shadows company accounts (no initial personas)
        - Uses automator-specific API keys from automator.api_key column
        - Creates personas in ALL automators assigned to each account
        
        Args:
            account_ids: List of account IDs for new accounts
        """
        try:
            logger.info(f"Creating initial personas for {len(account_ids)} new accounts")
            
            # Create initial mappings for Ticketboat accounts only
            await self._create_default_primary_mappings_for_ticketboat(account_ids)
            
            # Get the initial primary IDs to filter
            initial_primary_codes = [p["primary_code"] for p in INITIAL_CREATION_PRIMARIES]
            primary_ids_map = await get_primary_ids_by_code(initial_primary_codes)
            initial_primary_ids = list(primary_ids_map.values())
            
            # Validate accounts have automators and company
            validation_result = await validate_accounts_for_ts_sync(account_ids)
            
            if validation_result["invalid_accounts"]:
                logger.warning(
                    f"{len(validation_result['invalid_accounts'])} accounts missing "
                    "automator/company:"
                )
                for invalid in validation_result["invalid_accounts"]:
                    logger.warning(
                        f"  - {invalid['account_nickname']}: "
                        f"{', '.join(invalid['missing_fields'])}"
                    )
            
            if not validation_result["valid_accounts"]:
                logger.info("No valid accounts to create personas for")
                return
            
            # Create personas using automator-specific API keys
            result = await self.create_personas_for_accounts(
                account_ids=account_ids,
                primary_ids=initial_primary_ids,
                all_primaries=False
            )
            
            result_data = result.get("result", {})
            if result_data.get("successful_count", 0) > 0:
                logger.info(
                    f"Successfully created {result_data['successful_count']} "
                    "personas for new accounts"
                )
            if result_data.get("failed_count", 0) > 0:
                logger.warning(
                    f"{result_data['failed_count']} personas failed to create"
                )
            if result_data.get("automator_credential_errors"):
                logger.warning(
                    f"{len(result_data['automator_credential_errors'])} automators "
                    "had credential errors"
                )
                
        except Exception as e:
            logger.error(f"Error in create_personas_for_new_accounts: {e}")
            traceback.print_exc()

    async def _create_default_primary_mappings_for_ticketboat(
        self,
        account_ids: List[str]
    ) -> None:
        """
        Create default primary account mappings for Ticketboat accounts.
        
        Args:
            account_ids: List of account IDs to process
            use_initial_creation_config: If True, only creates "All But Ticketmaster"
                                        If False, uses full DEFAULT_PRIMARIES list
        """
        try:
            tb_accounts = await self._filter_ticketboat_accounts(account_ids)
            if not tb_accounts:
                return
            
            primary_codes = [p["primary_code"] for p in INITIAL_CREATION_PRIMARIES]
            primary_ids = await get_primary_ids_by_code(primary_codes)
            
            # Create mappings using the selected config
            await self._create_mappings_for_accounts(tb_accounts, primary_ids, INITIAL_CREATION_PRIMARIES)
            
        except Exception as e:
            logger.error(f"Error creating default primary mappings: {e}")
            traceback.print_exc()

    async def _filter_ticketboat_accounts(self, account_ids: List[str]) -> List[str]:
        """Filter accounts to only Ticketboat accounts. Shadows accounts are skipped."""
        import json
        accounts_data = await get_accounts_data_by_ids(account_ids)
        tb_accounts = []
        
        for account in accounts_data:
            account_dict = dict(account)
            company = json.loads(account_dict["company"]) if account_dict["company"] else None
            
            if not company:
                logger.warning(
                    f"Account {account_dict['id']} has no company assigned, skipping"
                )
                continue
            
            if self._is_ticketboat_company(company["name"]):
                tb_accounts.append(account_dict["id"])
            elif self._is_shadows_company(company["name"]):
                logger.info(
                    f"Skipping Shadows company account {account_dict['id']} - "
                    "no initial personas"
                )
        
        return tb_accounts

    async def _create_mappings_for_accounts(
        self,
        tb_accounts: List[str],
        primary_ids: Dict[str, str],
        primaries_config: List[Dict[str, str]]
    ) -> None:
        """Create primary account mappings for Ticketboat accounts."""
        for account_id in tb_accounts:
            for primary_cfg in primaries_config:
                code = primary_cfg["primary_code"]
                password = primary_cfg["password"]
                primary_id = primary_ids.get(code)
                
                if not primary_id:
                    logger.warning(f"Primary code not found: {code}")
                    continue
                
                try:
                    await create_primary_account_mapping(
                        account_id,
                        primary_id,
                        password=password
                    )
                except Exception as e:
                    logger.error(
                        f"Error creating mapping for account {account_id} with {code}: {e}"
                    )

    def _format_validation_error(self, invalid_accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Format validation error response."""
        invalid_list = []
        for invalid_acc in invalid_accounts:
            missing = ", ".join(invalid_acc["missing_fields"])
            invalid_list.append(
                f"{invalid_acc['account_nickname']} (ID: {invalid_acc['account_id']}) - "
                f"Missing: {missing}"
            )
        
        return {
            "success": False,
            "error": "Some accounts are missing required fields",
            "invalid_accounts": invalid_accounts,
            "message": (
                "The following accounts cannot be synced:\n" +
                "\n".join(invalid_list)
            )
        }


# Global service instance
_persona_creation_service: Optional[PersonaCreationService] = None


def get_persona_creation_service() -> PersonaCreationService:
    """
    Get the global PersonaCreationService instance.
    
    Returns a fresh instance per call to avoid shared mutable state
    between concurrent operations.
    """
    return PersonaCreationService()

