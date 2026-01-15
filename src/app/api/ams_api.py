import os
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Literal, Annotated, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Body
from fastapi.responses import StreamingResponse
from logging import getLogger

from app.auth.auth_system import get_current_user_with_roles
from app.db import ams_db, ams_cc_db
from app.model.ams_models import (
    AccountRequestPayloadV2Model,
    CreateCreditCardIssuerRequest,
    CreateStep,
    CreditCardProvider,
    PersonRequestModel,
    AddressRequestModel,
    EmailRequestModel,
    AccountRequestModel,
    PhoneNumberRequestModel,
    AcctStepRequestModel,
    ProxyRequestModel,
    EncryptionKeyResponse,
    EncryptedCreditCardDataWithKeyId,
    FilteredAddressRequestModel,
    FilteredAccountsRequestModel,
    FilteredAccountsCSVRequestModel,
    FilteredProxiesRequestModel,
    FilteredCCsRequestModel,
    UpdatePrimaryAccountMappingRequest,
    UpdateStageLink,
    UpdateStageOrder,
    UpdateStep,
    UpdateStepDependencies,
    UpdateStepOrder,
    AddPrimaryAccountMappingRequest,
    EmailTwoFARequestModel,
    EmailTwoFAResponseModel,
    EmailCommonFieldsRequest,
    OrderCreditCardRequest,
    OrderCreditCardResponse,
    AccountCreditCardOrders,
    BulkOrderCreditCardResponse,
    CreateTicketSuitePersonasRequest,
    RebuildAccountsRequest,
    AccountPrimaryPayload
)
from app.service.credit_card_factory import get_credit_card_factory
from app.service.persona_orchestrator import get_persona_orchestrator
from app.model.user import User
from app.service.encryption_key_service import encryption_key_service
from app.service.ticketsuite.ts_persona_client import get_ticketsuite_persona_client
from app.tasks.create_mlx_profiles import create_mlx_profiles_for_accounts
from app.service.ticketsuite.ts_persona_service import get_persona_creation_service
from app.db.ams_cc_db import get_account_data_for_credit_card
from app.utils import get_ses_client

router = APIRouter(prefix="/ams")


@router.get("/persons")
async def get_persons(
        page: int = Query(default=1, description="Page number to return"),
        page_size: int = Query(default=10, description="Number of results to return per page"),
        sort_field: str | None = Query(default=None, description="Field to sort by"),
        sort_order: str = Query(default='desc', description="Sort order"),
        search_query: str = Query(default="", description="Search term"),
        metro: Literal["all", "yes", "no"] = Query(default="all", description="Metro count"),
        name_quality: Literal["all", "Employee", "Non-Employee", "Contractor", "Department"] = Query(
            default="all", description="Filter by name quality"),
        status: Literal["all", "Active", "Inactive", "Active - No New Accts"] = Query(
            default="all", description="Filter by status"),
        timezone: str = Query(default="America/Chicago", description="Valid IANA timezone name like 'America/Chicago'"),
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_persons_viewer",
                    "ams_persons_editor",
                    "ams_card_editor"
                ]
            )
        ),
):
    try:
        return await ams_db.get_searched_persons(page, page_size, sort_field, sort_order, search_query, metro,
                                                 name_quality, status, timezone)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/persons/all")
async def get_all_persons(
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin", "ams_all_viewer", "ams_persons_editor",
                    "ams_persons_viewer", "ams_email_pw_editor"
                ]
            )
        ),
):
    try:
        return await ams_db.get_all_persons()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/recovery-emails/all")
async def get_all_recovery_emails(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_recovery_emails()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/recovery-phones/all")
async def get_all_recovery_phones(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_recovery_phones()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/forwarding-emails/all")
async def get_all_forwarding_emails(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_forwarding_emails()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/catchall-emails/all")
async def get_all_catchall_emails(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_catchall_emails()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/phone-numbers/all")
async def get_all_phone_numbers(
        statuses: List[str] = Query(["Active"]),
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_phone_numbers(status_list=statuses)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

@router.get("/persons/{person_id}")
async def get_person(
        person_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_persons_editor", "ams_persons_viewer"])),
):
    try:
        return await ams_db.get_single_person(person_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/persons/status")
async def update_person_status(
        updated_data: dict,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_persons_editor"])),
):
    try:
        return await ams_db.update_persons_status(updated_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/persons/{person_id}/star")
async def update_person_star(
        person_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_persons_editor"])),
):
    try:
        return await ams_db.update_persons_star(person_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/persons/{person_id}")
async def update_person(
        person_id: str,
        request: PersonRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_persons_editor"])),
):
    try:
        return await ams_db.update_person(person_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/persons")
async def create_persons(
        request: list[PersonRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_persons_editor"])),
):
    try:
        return await ams_db.create_persons(request)
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/metro-areas")
async def get_metro_areas(
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin", "ams_all_viewer", "ams_accounts_viewer", "ams_accounts_editor",
                    "ams_card_editor", "ams_cc_admin", "ams_card_viewer", "ams_address_editor", "ams_address_viewer",
                    "ams_proxies_viewer", "ams_proxies_editor"
                ]
            )
        ),
):
    try:
        return await ams_db.get_all_metro_areas()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/metro-areas/with-available-addresses",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_viewer",
                    "ams_accounts_editor",
                    "ams_card_editor",
                    "ams_cc_admin",
                    "ams_card_viewer",
                ]
            )
        )
    ],
)
async def get_metro_areas_with_available_addresses():
    try:
        return await ams_db.get_all_metro_areas_with_available_addresses()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/timezones")
async def get_timezones(
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin", "ams_all_viewer", "ams_card_editor", "ams_cc_admin", "ams_card_viewer",
                    "ams_address_editor", "ams_address_viewer", "ams_proxies_viewer", "ams_proxies_editor",
                    "ams_accounts_editor", "ams_accounts_viewer"
                ]
            )
        ),
):
    try:
        return await ams_db.get_all_timezones()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/states")
async def get_states(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_address_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_states()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/companies")
async def get_companies(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_accounts_viewer", "ams_accounts_editor",
                 "ams_card_editor", "ams_cc_admin", "ams_card_viewer", "captain"])),
):
    try:
        return await ams_db.get_all_companies()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/addresses/filtered")
async def get_filtered_addresses(
        request_data: FilteredAddressRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_address_editor", "ams_address_viewer"])),
):
    try:
        return await ams_db.get_searched_addresses(
            page=request_data.page,
            page_size=request_data.page_size,
            sort_field=request_data.sort_field,
            sort_order=request_data.sort_order,
            search_query=request_data.search_query,
            metro_area_ids=request_data.metro_area_ids,
            assigned_to_account=request_data.assigned_to_account,
            timezone=request_data.timezone,
            address_type=request_data.address_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/addresses/all")
async def get_all_addresses(
        address_type: Optional[str] = Query(None,
                                            description="Filter by address type: 'Account Address' or 'Billing Address'"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_address_editor", "ams_address_viewer"])),
):
    """
    Get all addresses with optional filtering by address type.
    
    Args:
        address_type: Optional filter for address type ('Account Address' or 'Billing Address')
        user: Authenticated user with appropriate roles
    
    Returns:
        List of addresses matching the filter criteria
    """
    try:
        return await ams_db.get_addresses_with_filter(address_type=address_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/addresses/{address_id}")
async def get_address(
        address_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_address_viewer", "ams_address_editor"])),
):
    try:
        return await ams_db.get_single_address(address_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/addresses")
async def create_addresses(
        request: list[AddressRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_address_editor"])),
):
    try:
        return await ams_db.create_addresses(request)
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.put("/addresses/{address_id}")
async def update_address(
        address_id: str,
        request: AddressRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_address_editor"])),
):
    try:
        return await ams_db.update_address(address_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/addresses/{address_id}/star")
async def update_address_star(
        address_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_address_editor"])),
):
    try:
        return await ams_db.update_address_star(address_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails")
async def get_emails(
        page: int = Query(default=1, description="Page number to return"),
        page_size: int = Query(default=10, description="Number of results to return per page"),
        sort_field: str | None = Query(default=None, description="Field to sort by"),
        sort_order: str = Query(default='desc', description="Sort order"),
        search_query: str = Query(default="", description="Search term"),
        assigned_to_account: Literal["all", "yes", "no"] = Query(default="all", description="Assigned to account"),
        paid_account: Literal["all", "yes", "no"] = Query(default="all", description="Paid account"),
        status: Optional[Annotated[str, Literal['AVAILABLE', 'IN USE', 'SUSPENDED', 'RETIRED', 'In Use - Mgmt']]] =
        Query(
            default=None,
            description="Filter by status"
        ),
        user: User = Depends(
            get_current_user_with_roles(["ams_admin", "ams_all_viewer", "ams_email_pw_editor", "ams_email_pw_viewer"])),
):
    try:
        return await ams_db.get_searched_emails(page, page_size, sort_field, sort_order, search_query,
                                                assigned_to_account, paid_account, status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/{email_id}")
async def get_email(
        email_id: str,
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_editor", "ams_email_pw_viewer"]
            )
        ),
):
    try:
        return await ams_db.get_single_email(email_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/emails")
async def create_emails(
        request: list[EmailRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_email_pw_editor"])),
):
    try:
        return await ams_db.create_emails(request)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.put("/emails/{email_id}")
async def update_email(
        email_id: str,
        request: EmailRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_email_pw_editor"])),
):
    try:
        return await ams_db.update_email(email_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/emails/{email_id}/star")
async def update_email_star(
        email_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_email_pw_editor"])),
):
    try:
        return await ams_db.update_email_star(email_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/emails/common-fields")
async def update_email_common_fields(
        updated_data: EmailCommonFieldsRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_email_pw_editor"])),
):
    try:
        return await ams_db.update_email_common_fields(updated_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/{email_id}/two-fa")
async def get_email_two_fa(
        email_id: str,
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_email_pw_viewer", "ams_email_pw_editor"]
            )
        ),
) -> list[EmailTwoFAResponseModel]:
    """
    Get all 2FA methods for a specific email.
    """
    try:
        return await ams_db.get_email_two_fa(email_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/emails/{email_id}/two-fa")
async def update_email_two_fa(
        email_id: str,
        two_fa_data: List[EmailTwoFARequestModel] = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_email_pw_editor"])),
) -> list[EmailTwoFAResponseModel]:
    """
    Update 2FA methods for a specific email.
    This replaces all existing 2FA methods with the new ones.
    """
    try:
        return await ams_db.update_email_two_fa(email_id, two_fa_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/accounts/filtered")
async def get_filtered_accounts(
        request_data: FilteredAccountsRequestModel = Body(...),
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                    "ams_card_editor"
                ]
            )
        ),
):
    try:
        return await ams_db.get_searched_accounts(
            page=request_data.page,
            page_size=request_data.page_size,
            sort_field=request_data.sort_field,
            sort_order=request_data.sort_order,
            search_query=request_data.search_query,
            metro_area_ids=request_data.metro_area_ids,
            company_ids=request_data.company_ids,
            created_at=request_data.created_at,
            incomplete_steps=request_data.incomplete_steps,
            status_code=request_data.status_code,
            address_search_query=request_data.address_search_query,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def generate_accounts_csv_stream(csv_data: str):
    """Generator function to stream pre-formatted CSV data in chunks"""
    if not csv_data:
        return

    # Stream the CSV data in chunks
    chunk_size = 8192  # 8KB chunks
    for i in range(0, len(csv_data), chunk_size):
        yield csv_data[i:i + chunk_size]


@router.post("/accounts/filtered/export")
async def export_filtered_accounts_csv(
        request_data: FilteredAccountsCSVRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer"])),
):
    """Export filtered AMS accounts to CSV. Limited to 20,000 records for performance."""
    try:
        # Get CSV data directly from the database function
        csv_data = await ams_db.get_searched_accounts_for_csv_export(
            sort_field=request_data.sort_field,
            sort_order=request_data.sort_order,
            search_query=request_data.search_query,
            metro_area_ids=request_data.metro_area_ids,
            company_ids=request_data.company_ids,
            created_at=request_data.created_at,
            incomplete_steps=request_data.incomplete_steps,
            status_code=request_data.status_code,
            address_search_query=request_data.address_search_query,
            limit=20000
        )

        # Use streaming response with generator for better memory efficiency
        return StreamingResponse(
            generate_accounts_csv_stream(csv_data),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=ams_accounts_export.csv"
            }
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred while exporting accounts: {str(e)}")


@router.get("/accounts/{account_id}/single")
async def get_single_account(
        account_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_single_account(account_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/accounts/by-ids")
async def get_accounts_data_by_ids(
        account_ids: list[str] | None = Query(..., description="List of account IDs"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer",
                                                          "ams_email_pw_viewer", "ams_email_pw_editor"])),
):
    if account_ids is None or len(account_ids) == 0:
        raise HTTPException(status_code=400, detail="account_ids query parameter is required and cannot be empty.")

    try:
        return await ams_db.get_accounts_data_by_ids(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/accounts/null-proxy-id")
async def get_accounts_with_null_proxy_id(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_proxies_editor"]
            )
        ),
):
    try:
        return await ams_db.get_accounts_with_null_proxy_id()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/accounts/{account_id}/single")
async def update_account(
        account_id: str,
        updated_data: dict[str, str | None] = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_account(account_id, updated_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/bulk-update")
async def update_account_status(
        update_data: dict,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_accounts_bulk(update_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/star")
async def update_account_star(
        account_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_account_star(account_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/nickname")
async def update_account_nickname(
        account_id: str,
        update_data: dict[Literal['nickname'], str],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_accounts_nickname(account_id, update_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/accounts/steps")
async def update_accounts_steps(
        request: list[AcctStepRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_accounts_steps(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/notes")
async def update_account_notes(
        account_id: str,
        update_data: dict[Literal['notes'], str],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_accounts_notes(account_id, update_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/accounts/tags")
async def get_account_tags(
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_account_tags()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/accounts/status-options",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                ]
            )
        )
    ],
)
async def get_account_status_options():
    try:
        return await ams_db.get_account_status_options()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/accounts/tags-by-account-ids",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                ]
            )
        )
    ],
)
async def get_account_tags_for_account(
        account_ids: list[str] | None = Query(..., description="List of account IDs")
):
    if account_ids is None or len(account_ids) == 0:
        raise HTTPException(
            status_code=400,
            detail="account_ids query parameter is required and cannot be empty.",
        )
    try:
        return await ams_db.get_account_tags_for_account(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/accounts/tags")
async def create_account_tag(
        body: dict[Literal['name'], str],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.create_account_tag(body["name"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/tags")
async def update_account_tags(
        account_id: str,
        update_data: dict[Literal['tags'], list[str]],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_accounts_tags(account_id, update_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/account-items-to-create")
async def get_account_items_by_metro_id(
        quantity: int = Query(default=1, description="Number of account items to retrieve"),
        metro_area_id: str = Query(default="", description="Selected Metro area id"),
        account_id: str | None = Query(default=None,
                                       description="Account id to include the current items of the account"),
        has_dob: bool = Query(default=False, description="Filter persons who have date of birth"),
        has_ssn: bool = Query(default=False,
                              description="Filter persons who have social security number (last 4 digits)"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_account_items_by_metro_id(quantity, metro_area_id, account_id, has_dob, has_ssn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/v2/account-items-to-create",
    dependencies=[Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor", "ams_accounts_viewer"]))],
)
async def get_account_items_by_metro_id_v2(
        metro_area_id: str = Query(default="", description="Selected Metro area id"),
        timezone_id: str | None = Query(
            default=None, description="Timezone ID to filter account addresses"
        ),
        has_dob: bool = Query(
            default=False, description="Filter persons who have date of birth"
        ),
        has_ssn: bool = Query(
            default=False,
            description="Filter persons who have social security number (last 4 digits)",
        ),
        create_mlx_profile: bool = Query(
            default=False, description="Whether to create MXL profiles for new accounts"),
):
    """
    Retrieve account creation building-block data for a specific metro (v2).

    This endpoint aggregates people, emails, addresses, phones, proxies, tags,
    and the next nickname increment needed to build new accounts in a given
    metro area. Optional filters restrict persons to those that have a DOB and/or
    last 4 SSN. If the special sentinel value 'any_metro' is provided, a future
    implementation will return a cross-metro aggregation (currently not implemented).
    If the special sentinel value 'NONE' is provided, returns all available resources
    (all addresses regardless of metro, all persons, phones, and proxies).

    Args:
        metro_area_id (str): The target metro area UUID. Use an actual metro UUID.
            The value 'any_metro' is a placeholder for a not-yet-implemented
            multi-metro variant. The value 'NONE' returns all resources without metro
            area filtering (all addresses, all persons).
        timezone_id (str | None): (Reserved for future use in v2 "any_metro"
            implementation) Timezone UUID to constrain addresses/persons. Ignored
            for specific metro lookups.
        has_dob (bool): When True, only include persons whose date_of_birth is not NULL.
            When False, DOB presence is not enforced.
        has_ssn (bool): When True, only include persons whose last_4_ssn is not NULL.
            When False, SSN presence is not enforced.
        create_mxl_profile (bool): Whether to create MXL profiles for new accounts.

    Returns:
        dict: A dictionary containing (for a specific metro):
            metro (dict | None): The metro metadata for the requested metro_area_id.
                None when metro_area_id is 'NONE'.
            ready_count (int): Minimum count of person-with-email records and available
                addresses (i.e., how many complete account "slots" can be formed).
            persons (list[dict]): Raw person records (may include those without emails).
            persons_with_emails (list[dict]): Persons enriched with a non-empty 'emails' list.
            addresses (list[dict]): Available addresses in the metro (not already used).
                For 'NONE', returns all addresses regardless of metro area.
            phones (list[dict]): Available phone numbers.
            proxies (list[dict]): Available proxies.
            grouped_proxies (list[dict]): Proxies grouped by metro (and unassigned group).
            tags (list[dict]): Account tags metadata.
            next_increment (dict | None): Next nickname increment info (shortname + suffix),
                or None if not computable. Always None for 'NONE' metro.

    Raises:
        HTTPException: 500 if an unexpected error occurs during data retrieval.
    """
    none_metro_id = "9687698a-8753-4875-b763-88fb7536dc5f"
    try:
        if metro_area_id == "any_metro":
            pass
            # TODO: implement any_metro at later date
            # return await ams_db.get_account_items_in_any_metro_v2(
            #     timezone_id, has_dob, has_ssn
                # )
        elif metro_area_id == none_metro_id:
            return await ams_db.get_account_items_for_none_metro(
                none_metro_id, has_dob, has_ssn, create_mlx_profile
            )
        else:
            return await ams_db.get_account_items_by_metro_id_v2(
                metro_area_id, has_dob, has_ssn, create_mlx_profile
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/v2/accounts/check-nickname",
    dependencies=[Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                       "ams_accounts_editor", "ams_accounts_viewer"]))],
)
async def check_account_nickname_unique(
        nickname: str = Query(..., description="Nickname to check for existence"),
):
    """
    Check whether an account nickname already exists.

    This endpoint validates the uniqueness of a proposed account nickname
    following the naming convention used for account creation (e.g., a metro
    shortname followed by a zero-padded numeric suffix). It delegates the
    lookup to the data-access layer.

    Args:
        nickname (str): The exact nickname string to check.

    Returns:
        dict: A dictionary with:
            is_unique (bool): True if the nickname does not exist, False otherwise.

    Raises:
        HTTPException: 500 if an unexpected error occurs during the lookup.
    """

    try:
        is_unique = await ams_db.check_account_nickname_unique(nickname)
        return {"is_unique": is_unique}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/v2/accounts/check-ts-persona-existence",
    dependencies=[Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                       "ams_accounts_editor", "ams_accounts_viewer"]))],
)
async def check_ts_persona_existence(
    email: str = Query(..., description="email address to check"),
    automator_id: str = Query(None, description="specific automator ID to check (optional)")
):
    """
    Check whether a TicketSuite persona exists for a single email address.
    
    If automator_id is provided, checks only that specific automator.
    If automator_id is not provided, checks ALL TicketSuite automators.

    Args:
        email (str): Email address to verify.
        automator_id (str, optional): Specific automator ID to check. If not provided,
            checks all TicketSuite automators.

    Returns:
        dict: {
            "is_unique": bool - True if no persona exists in any checked automator,
            "found_in_automators": list - List of automator names where persona was found (if any)
        }

    Raises:
        HTTPException: 500 if an unexpected error occurs during the lookup.
    """
    try:
        found_in_automators = []
        
        # Get automators to check
        if automator_id:
            # Check specific automator
            automators = await ams_db.get_automators_by_ids([automator_id])
        else:
            # Get all TicketSuite automators
            all_automators = await ams_db.get_all_automators_with_api_key()
            automators = [a for a in all_automators if a.get("brand") == "ticketsuite"]
        
        if not automators:
            # No automators to check, consider it unique
            return {
                "is_unique": True,
                "found_in_automators": []
            }
        
        # Check each automator
        for automator in automators:
            api_key = automator.get("api_key")
            if not api_key:
                continue
                
            ts_service = get_ticketsuite_persona_client(api_key=api_key)
            async with ts_service:
                exists = await ts_service.get(email=email)
                if exists and len(exists) > 0:
                    found_in_automators.append(automator.get("name", "Unknown"))
        
        return {
            "is_unique": len(found_in_automators) == 0,
            "found_in_automators": found_in_automators
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/account-items-to-create-any-metro")
async def get_account_items_in_any_metro(
        quantity: int = Query(default=1, description="Number of account items to retrieve"),
        timezone_id: str | None = Query(default=None, description="Timezone ID to filter account addresses"),
        has_dob: bool = Query(default=False, description="Filter persons who have date of birth"),
        has_ssn: bool = Query(default=False,
                              description="Filter persons who have social security number (last 4 digits)"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_account_items_in_any_metro(quantity, timezone_id, has_dob, has_ssn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/accounts")
async def create_accounts(
        request: List[AccountRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.create_accounts(request)
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/v2/accounts")
async def create_accounts_v2(
        payload: AccountRequestPayloadV2Model,
        background_tasks: BackgroundTasks,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    """
    Bulk create v2 AMS accounts.

    Accepts a list of account payloads (v2 schema) and delegates persistence to
    the data-access layer. Each item typically includes references to a person,
    email, address, phone, proxy, company, optional tags, and nickname metadata.

    Args:
        request (AccountRequestPayloadV2Model): List of account creation objects.
            Each must satisfy validation rules defined in AccountRequestPayloadV2Model.

    Returns:
        Any: The result from ams_db.create_accounts_v2, usually a list of created
            account records (with generated IDs / nicknames) or a structured
            response describing successes and failures.

    Raises:
        HTTPException:
            400: If validation / value errors occur in input data.
            500: For unexpected server or database errors.
    """
    try:
        result = await ams_db.create_accounts_v2(payload.accounts)
        environment = os.getenv("ENVIRONMENT", "staging")
        if environment == "prod":
            background_tasks.add_task(send_account_creation_email, [acct for acct in result], user.email)
        if payload.create_mlx_profile:
            task_name = f"create_mlx_profiles:{len(result)}_accounts"
            background_tasks.add_task(
                create_mlx_profiles_for_accounts,
                [acct["id"] for acct in result],
                task_name=task_name,
                user_email=user.email,
            )
            
        background_tasks.add_task(
            get_persona_creation_service().create_personas_for_new_accounts,
            [acct["id"] for acct in result]
        )
        return result
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/accounts/rebuild")
async def rebuild_accounts(
        payload: RebuildAccountsRequest,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.rebuild_accounts(payload.accounts)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def send_account_creation_email(
    accounts: List[dict], requested_by_email: Optional[str] = None
) -> None:
    """
    Send a consolidated SES notification for newly created AMS accounts.
    """
    logger = getLogger(__name__)

    if not accounts:
        logger.info("No accounts provided for account creation email; skipping send")
        return

    try:
        account_rows = []
        for account in accounts:
            try:
                account_id = account["id"]
            except Exception:
                logger.warning(
                    "Skipping account entry with no id in account creation email payload"
                )
                continue
            if not account_id:
                logger.warning(
                    "Skipping account entry with empty id in account creation email payload"
                )
                continue

            account_data = await get_account_data_for_credit_card(account_id)
            if not account_data:
                logger.warning(
                    f"Account data not found for account_id {account_id} when sending account creation email"
                )
                continue

            person_name = f"{account_data.person_first_name} {account_data.person_last_name}".strip()
            account_rows.append(
                {
                    "account": account_data.nickname or str(account_data.id),
                    "company": account_data.company_name or "-",
                    "person_name": person_name,
                    "email": account_data.email_address or "-",
                }
            )

        if not account_rows:
            logger.info(
                "No account details resolved for account creation email; skipping send"
            )
            return

        table_rows = []
        for row in account_rows:
            table_rows.append(
                f"""
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">{row["account"]}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{row["company"]}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{row["person_name"]}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{row["email"]}</td>
                </tr>
                """
            )

        ses_client = get_ses_client()
        if not ses_client:
            logger.error("Failed to initialize SES client for account creation email")
            return

        account_count = len(account_rows)
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th {{ background-color: #f8f9fa; padding: 12px; border: 1px solid #ddd; text-align: left; }}
                td {{ padding: 10px; border: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <h2>Account Creation Summary</h2>
            <p>
                <strong>Date:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
                <strong>Total Accounts:</strong> {account_count}<br>
                <strong>Requested By:</strong> {requested_by_email or "-"}
            </p>
            <table>
                <thead>
                    <tr>
                        <th>Account</th>
                        <th>Company</th>
                        <th>Person Name</th>
                        <th>Email</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(table_rows)}
                </tbody>
            </table>
        </body>
        </html>
        """

        msg = MIMEMultipart("alternative")
        msg["From"] = "forwarder@tb-portal.com"
        recipients = ["ap@ticketboat.com"]

        # Validate requested_by_email before adding
        if requested_by_email and isinstance(requested_by_email, str):
            clean_email = requested_by_email.strip()
            # Basic validation to ensure it looks like an email and isn't a duplicate
            if "@" in clean_email and clean_email not in recipients:
                recipients.append(clean_email)

        msg["To"] = ", ".join(recipients)
        msg["Subject"] = (
            f"Account Creation Summary - {datetime.now().strftime('%Y-%m-%d')}"
        )
        msg.attach(MIMEText(html_body, "html"))

        ses_client.send_raw_email(
            Source=msg["From"],
            Destinations=recipients,
            RawMessage={"Data": msg.as_string()},
        )

        logger.info(
            f"Account creation email sent successfully for {account_count} account(s)"
        )
    except Exception as e:
        logger.error(f"Failed to send account creation email: {str(e)}")


@router.post("/accounts/sync-vaultwarden")
async def handle_accounts_vaultwarden_sync(
        account_ids: List[str] = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    results = await ams_db.sync_vaultwarden_accounts(account_ids)
    return {"results": results}

    
@router.post("/accounts/ticketsuite-personas")
async def handle_create_ticketsuite_personas_for_accounts(
        payload: List[AccountPrimaryPayload],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    """
    Create personas for account-primary combinations across all automators.
    
    Automatically routes to the correct service based on each automator's brand
    (TicketSuite, Taciyon, etc.). Each account can have multiple automators,
    and personas will be synced to all of them.
    
    Args:
        payload: List of AccountPrimaryPayload with:
            - account_id: str - Account ID to create personas for
            - primary_ids: List[str] - Primary IDs to sync for this account
    
    Returns:
        List[dict]: List of sync response items with:
            - account_id: str
            - email_address: str
            - sync_results: List of sync results with:
                - primary_name: str | None
                - status: "success" | "error"
                - status_code: int | None
                - response: dict (success message)
                - error: str (error message)
    
    Example:
        [
            {
                "account_id": "acc-uuid-1",
                "primary_ids": ["primary-1", "primary-2"]
            },
            {
                "account_id": "acc-uuid-2", 
                "primary_ids": ["primary-3"]
            }
        ]
    """
    try:
        orchestrator = get_persona_orchestrator()
        return await orchestrator.create_personas(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/accounts/by-ids/primaries")
async def get_accounts_with_primaries_by_ids(
        account_ids: list[str] | None = Query(..., description="List of account IDs"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_accounts_editor", "ams_accounts_viewer"])),
):
    if account_ids is None or len(account_ids) == 0:
        raise HTTPException(status_code=400, detail="account_ids query parameter is required and cannot be empty.")

    try:
        return await ams_db.get_accounts_with_primaries(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get(
    "/accounts/{account_id}/persona-mappings",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                ]
            )
        )
    ],
)
async def get_account_persona_mappings(account_id: str):
    """
    Get all persona mappings for an account across different automators.
    
    This endpoint returns which personas exist in which automator systems
    (TicketSuite, Taciyon, etc.) for each primary associated with the account.
    
    Returns:
        Dictionary mapping primary names to lists of persona mappings:
        {
            "Ticketmaster": [
                {
                    "automator_id": "abc-123",
                    "automator_name": "TicketSuite US",
                    "ams_automator_id": "ts-persona-456",
                    "created_at": "2024-01-15T10:30:00Z",
                    "updated_at": "2024-01-15T10:30:00Z"
                }
            ],
            "AXS": [...]
        }
    """
    try:
        return await ams_db.get_all_persona_mappings_for_account(account_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get(
    "/accounts/by-ids/pos",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                ]
            )
        )
    ],
)
async def get_accounts_pos_by_ids(
        account_ids: list[str] | None = Query(..., description="List of account IDs")
):
    if account_ids is None or len(account_ids) == 0:
        raise HTTPException(
            status_code=400,
            detail="account_ids query parameter is required and cannot be empty.",
        )

    try:
        return await ams_db.get_accounts_pos(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get(
    "/accounts/by-ids/automators",
    dependencies=[
        Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_accounts_editor",
                    "ams_accounts_viewer",
                ]
            )
        )
    ],
)
async def get_accounts_automators_by_ids(
        account_ids: list[str] | None = Query(..., description="List of account IDs")
):
    if account_ids is None or len(account_ids) == 0:
        raise HTTPException(
            status_code=400,
            detail="account_ids query parameter is required and cannot be empty.",
        )

    try:
        return await ams_db.get_accounts_automators(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/accounts/{account_id}/primaries")
async def create_primary_account(
        account_id: str,
        request: AddPrimaryAccountMappingRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    """
    Create a primary account mapping for a given account.
    Args:
        account_id: The ID of the account to create the primary account mapping for.
        primary_id: The ID of the primary to create the account mapping for.
        password: The password for the primary. If generate_password is true, the password field is ignored and a secure password is generated automatically.
        generate_password: whether to generate a password for the primary.
    Returns:
        dict: The result from the database creation.
    """
    try:
        return await ams_db.create_primary_account_mapping(
            account_id, request.primary_id, request.password, request.generate_password
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.patch("/accounts/{account_id}/primaries")
async def update_primary_account(
        account_id: str,
        request: UpdatePrimaryAccountMappingRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    """
    Update a primary password for a given account.

    Args:
        account_id (str): The ID of the account to update.
        request (dict): The request body containing the email_id, primary_id, and password.

    Returns:
        dict: The result from the database update.

    Raises:
        HTTPException:
            500: If an error occurs while updating the primary password.
    """
    try:
        return await ams_db.update_primary_account_mapping(account_id, request.primary_id, request.password)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.patch(
    "/accounts/{account_id}/proxy",
    dependencies=[
        Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"]))
    ],
)
async def update_proxy_and_reason(
        account_id: str,
        request: dict = Body(...),
):
    try:
        return await ams_db.update_proxy_and_reason(account_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/primaries")
async def get_primaries(
        search_query: str = Query(default="", description="Search term"),
        user: User = Depends(get_current_user_with_roles(["ams_admin"])),
):
    try:
        return await ams_db.get_searched_primaries(search_query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/primaries")
async def create_primary(
        request: dict = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin"])),
):
    try:
        return await ams_db.create_primary(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/phone-numbers")
async def get_phone_numbers(
        page: int = Query(default=1, description="Page number to return"),
        page_size: int = Query(default=10, description="Number of results to return per page"),
        sort_field: str | None = Query(default=None, description="Field to sort by"),
        sort_order: str = Query(default='desc', description="Sort order"),
        search_query: str = Query(default="", description="Search term"),
        assigned_to_account: Literal["all", "yes", "no"] = Query(default="all", description="Assigned to account"),
        provider: Literal[
            "All", "Text Chest", "Verizon", "T-Mobile", "US Mobile", "Tello", "Personal", "WiredSMS"] = Query(
            default="All", description="Phone provider"),
        timezone: str = Query(default='America/Chicago', description="Valid IANA timezone name like 'Asia/Tashkent'"),
        status: Optional[Annotated[str, Literal['Active', 'Cancelled', 'Special']]] = Query(
            default=None,
            description="Filter by status"
        ),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_phones_editor", "ams_phones_viewer"])),
):
    try:
        return await ams_db.get_searched_phone_numbers(page, page_size, sort_field, sort_order,
                                                       search_query, assigned_to_account, provider, timezone, status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/phone-numbers/check-unique",
    dependencies=[Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                       "ams_phones_editor", "ams_phones_viewer"]))]
)
async def check_phone_number_unique(
        phone_number: str = Query(..., description="Phone number to check for existence")
):
    try:
        result = await ams_db.check_phone_number_unique(phone_number)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/phone-numbers")
async def create_phone_numbers(
        request: list[PhoneNumberRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_phones_editor"])),
):
    try:
        return await ams_db.create_phone_numbers(request)
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.put("/phone-numbers/{phone_id}")
async def update_phone_number(
        phone_id: str,
        request: PhoneNumberRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_phones_editor"])),
):
    try:
        return await ams_db.update_phone_number(phone_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/phone-numbers/{phone_id}")
async def get_phone_number(
        phone_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_phones_editor", "ams_phones_viewer"])),
):
    try:
        return await ams_db.get_single_phone_number(phone_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/phone-providers")
async def get_phone_providers(
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_phones_editor", "ams_phones_viewer"])),
):
    try:
        return await ams_db.get_phone_providers()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/phone-numbers/status")
async def update_phone_numbers_status(
        updated_data: dict,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_phones_editor"])),
):
    try:
        return await ams_db.update_phone_numbers_status(updated_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/phone-numbers/{phone_id}/star")
async def update_phone_star(
        phone_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_phones_editor"])),
):
    try:
        return await ams_db.update_phone_star(phone_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/email-addresses/all")
async def get_all_email_addresses(
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_email_pw_editor", "ams_email_pw_viewer"])),
):
    try:
        return await ams_db.get_all_email_addresses()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/credit-cards/filtered")
async def get_filtered_credit_cards(
        request_data: FilteredCCsRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(
            ["ams_admin", "ams_all_viewer", "ams_card_editor", "ams_cc_admin", "ams_card_viewer"])),
):
    """
    Get filtered credit cards.
    """
    try:
        return await ams_cc_db.get_credit_cards_by_filters(
            page=request_data.page,
            page_size=request_data.page_size,
            sort_field=request_data.sort_field,
            sort_order=request_data.sort_order,
            search_query=request_data.search_query,
            metro_area_ids=request_data.metro_area_ids,
            company_ids=request_data.company_ids,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while creating the credit card: {e}")


@router.post("/credit-cards/encrypted", response_model=dict)
async def create_credit_card_encrypted(
        encrypted_request: EncryptedCreditCardDataWithKeyId = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_card_editor", "ams_cc_admin"])),
):
    """
    Create a new credit card record from encrypted data.
    The backend will decrypt the data, validate the card number, and store it encrypted.
    """
    try:
        credit_card_id = await ams_cc_db.create_credit_card_encrypted(encrypted_request, user.user_id)
        return {"id": str(credit_card_id), "message": "Credit card created successfully from encrypted data."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while creating the credit card: {e}")


@router.patch("/credit-cards/bulk-update")
async def bulk_update_cards(
        update_data: dict,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_cc_admin", "ams_card_editor"]))
):
    try:
        return await ams_cc_db.update_credit_cards(update_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/credit-cards/{card_id}", response_model=EncryptedCreditCardDataWithKeyId)
async def get_single_credit_card(
        card_id: str,
        user: User = Depends(
            get_current_user_with_roles(["ams_admin", "ams_cc_admin", "ams_card_viewer", "ams_card_editor"])),
):
    try:
        return await ams_cc_db.get_single_credit_card(card_id, user.user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while getting the credit card data by id: {e}")


@router.get("/credit-cards/{card_id}/edit")
async def get_credit_card_for_edit(
        card_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_cc_admin", "ams_card_editor"])),
):
    """
    Get full credit card data for editing in the edit modal.
    Returns comprehensive information including all addresses, account, person, and company details.
    """
    try:
        return await ams_cc_db.get_credit_card_for_edit(card_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while getting credit card for edit: {e}")


@router.put("/credit-cards/{card_id}/encrypted")
async def update_credit_card(
        card_id: str,
        encrypted_request: EncryptedCreditCardDataWithKeyId = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_cc_admin", "ams_card_editor"])),
):
    """
    Update a single credit card.
    Updates the specified credit card with the provided data.
    Only non-null fields will be updated.
    """
    # credit_card_id = await ams_cc_db.create_credit_card_encrypted(encrypted_request, user.user_id)
    # return {"id": str(credit_card_id), "message": "Credit card created successfully from encrypted data."}
    try:
        return await ams_cc_db.update_credit_card_encrypted(card_id, user.user_id, encrypted_request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while updating credit card: {e}")


@router.post("/credit-cards/encryption-key", response_model=EncryptionKeyResponse)
async def get_encryption_key(
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_cc_admin",
                    "ams_card_editor",
                    "ams_card_viewer",
                    "ams_all_viewer"
                ]
            )
        ),
):
    """
    Get a short-lived encryption key for credit card data encryption.
    The key expires in 7 minutes and can only be used once.
    """
    try:
        key_data = await encryption_key_service.create_encryption_key(
            user_id=user.user_id,
        )
        return EncryptionKeyResponse(**key_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create encryption key: {e}")


@router.get("/credit-cards/encryption-key/{key_id}", response_model=EncryptionKeyResponse)
async def get_encryption_key_by_id(
        key_id: str,
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_cc_admin",
                    "ams_card_editor",
                    "ams_card_viewer",
                    "ams_all_viewer"
                ]
            )
        ),
):
    """
    Get the short-lived encryption key created.
    Returns the key if it exists and is not expired.
    """
    try:
        key_data = await encryption_key_service.get_encryption_key(
            key_id=key_id,
            user_id=user.user_id,
        )
        return EncryptionKeyResponse(**key_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get encryption key by id: {e}")


@router.post("/credit-cards/check-card-number")
async def check_card_number(
        card_number: str = Body(..., embed=True),
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_cc_admin",
                    "ams_card_editor"
                ]
            )
        ),
):
    """
    Check if a credit card number already exists in the system.
    Returns only existence status for security.
    """
    try:
        from app.db.ams_cc_db import check_card_number_exists
        exists, _ = await check_card_number_exists(card_number)
        return {"exists": exists}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to check card number: {e}")


@router.get("/credit-card-issuers")
async def get_credit_card_issuers(
        user: User = Depends(
            get_current_user_with_roles(
                [
                    "ams_admin",
                    "ams_all_viewer",
                    "ams_card_editor",
                    "ams_cc_admin",
                    "ams_card_viewer"
                ]
            )
        ),
):
    """
    Get all credit card issuers from the database.
    Returns a list of issuers with id and label.
    """
    try:
        return await ams_cc_db.get_all_credit_card_issuers()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/credit-card-issuers")
async def create_credit_card_issuer(
    request: CreateCreditCardIssuerRequest = Body(...),
    user: User = Depends(
        get_current_user_with_roles(
            [
                "ams_admin",
                "ams_all_viewer",
                "ams_card_editor",
                "ams_cc_admin",
                "ams_card_viewer",
            ]
        )
    ),
):
    """
    Create a new credit card issuer in the database.
    """
    try:
        return await ams_cc_db.create_credit_card_issuer(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/credit-cards/order", response_model=OrderCreditCardResponse)
async def order_credit_cards(
        request: OrderCreditCardRequest = Body(...),
        user: User = Depends(
            get_current_user_with_roles([
                "ams_admin",
                "ams_accounts_editor"
            ])
        )
):
    """
    Create a new credit card for an AMS account
    
    Requires one of the following roles:
    - ams_admin: Full AMS administrative access
    - ams_card_editor: Can edit credit cards
    - credit_card_creator: Can create credit cards
    """
    try:
        logger = getLogger(__name__)
        logger.info(f"Credit card order requested by user {user.email} for account {request.account_id}")

        factory = get_credit_card_factory()

        # Validate provider
        if not factory.is_provider_available(request.provider):
            available_providers = [p.value for p in factory.get_available_providers()]
            raise HTTPException(
                status_code=400,
                detail=f"Provider '{request.provider.value}' is not available. Available providers: {available_providers}"
            )

        # Create the credit card
        response = await factory.create_credit_card(request)

        # Log the result
        if response.success:
            logger.info(f"Credit card created successfully for account {request.account_id}")
        else:
            logger.warning(f"Credit card creation failed for account {request.account_id}: {response.error_message}")

        return response

    except HTTPException:
        raise
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Unexpected error in credit card creation endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/accounts/by-ids/credit-cards")
async def get_existing_credit_cards_by_accounts(
        account_ids: List[str] = Query(..., description="List of account IDs to get existing credit cards for"),
        user: User = Depends(
            get_current_user_with_roles([
                "ams_admin",
                "ams_all_viewer",
                "ams_accounts_viewer",
                "ams_accounts_editor",
                "ams_card_viewer",
                "ams_card_editor",
                "ams_cc_admin"
            ])
        )
):
    """
    Get existing credit cards for multiple AMS accounts
    
    Returns a mapping of account_id to list of existing credit cards.
    This helps determine which providers are already used by each account.
    
    Requires one of the following roles:
    - ams_admin: Full AMS administrative access
    - ams_all_viewer: Can view all AMS data
    - ams_accounts_viewer: Can view AMS accounts
    - ams_accounts_editor: Can edit AMS accounts
    - ams_card_viewer: Can view credit cards
    - ams_card_editor: Can edit credit cards
    - ams_cc_admin: Credit card admin access
    """
    try:
        if not account_ids:
            raise HTTPException(status_code=400, detail="account_ids parameter is required and cannot be empty")

        logger = getLogger(__name__)
        logger.info(f"Fetching existing credit cards for {len(account_ids)} accounts")

        existing_cards = await ams_cc_db.get_existing_credit_cards_by_accounts(account_ids)

        logger.info(f"Found existing credit cards for accounts: {list(existing_cards.keys())}")
        return existing_cards

    except HTTPException:
        raise
    except Exception as e:
        logger = getLogger(__name__)
        logger.error(f"Error fetching existing credit cards: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching existing credit cards: {str(e)}")


@router.post("/credit-cards/bulk-order", response_model=BulkOrderCreditCardResponse)
async def bulk_order_credit_cards(
        account_orders: List[AccountCreditCardOrders] = Body(...),
        user: User = Depends(
            get_current_user_with_roles([
                "admin",
                "ams_admin",
                "ams_accounts_editor"
            ])
        )
):
    """
    Create multiple credit cards for multiple AMS accounts
    
    Requires one of the following roles:
    - ams_admin: Full AMS administrative access
    - ams_accounts_editor: Can edit AMS accounts and credit cards
    """
    environment = os.getenv("ENVIRONMENT", "staging")
    try:
        if environment == "prod":
            logger = getLogger(__name__)
            logger.info(
                f"Bulk credit card order requested by user {user.email} for {len(account_orders)} accounts"
            )
            factory = get_credit_card_factory()
            return await factory.bulk_order_credit_cards(account_orders)
        else:
            # return mock response in non-production environments
            logger = getLogger(__name__)
            logger.info(
                f"Bulk credit card order requested in non-prod environment '{environment}'. Returning mock response."
            )
            mock_results = []
            for account in account_orders:
                for _ in account.orders:
                    mock_results.append(
                        OrderCreditCardResponse(
                            success=True,
                            provider=CreditCardProvider.WEX,
                            account_id=account.account_id,
                            card_number="****8095",
                            expiry_date="2030-11-13T00:00:00.000Z",
                            cvc=None,
                            account_token="mock_token_12345",
                            error_message=None,
                            error_code=None,
                            provider_data={
                                "credit_rating": "AC",
                                "status_code": "AC",
                                "description": "Account successfully retrieved.",
                            },
                        )
                    )
            mock_response = BulkOrderCreditCardResponse(
                overall_success=True,
                results=mock_results,
                total_requested=len(mock_results),
                total_successful=len(mock_results),
                total_failed=0,
            )
            return mock_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in bulk credit card ordering: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred during bulk credit card ordering: {e}.")


@router.post("/proxies/by-ids")
async def get_proxies_by_ids(
        ids: List[str] = Body(...),
        user: User = Depends(get_current_user_with_roles(
            ["ams_admin", "ams_all_viewer", "ams_proxies_editor", "ams_proxies_viewer", "ams_accounts_editor"]
        )
        ),
):
    try:
        return await ams_db.get_proxies_by_ids(ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/proxies/filtered")
async def get_filtered_proxies(
        request_data: FilteredProxiesRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_proxies_editor", "ams_proxies_viewer"])),
):
    try:
        return await ams_db.get_searched_proxies(
            page=request_data.page,
            page_size=request_data.page_size,
            sort_field=request_data.sort_field,
            sort_order=request_data.sort_order,
            search_query=request_data.search_query,
            metro_area_ids=request_data.metro_area_ids,
            provider_id=request_data.provider_id,
            timezone=request_data.timezone,
            status=request_data.status
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/proxies/status-options", dependencies=[
    Depends(
        get_current_user_with_roles(
            [
                "ams_admin",
                "ams_all_viewer",
                "ams_accounts_editor",
                "ams_accounts_viewer",
            ]
        )
    )
], )
async def get_proxy_status_options():
    try:
        return await ams_db.get_proxy_status_options()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/proxy-providers")
async def get_proxy_providers(
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_proxies_editor", "ams_proxies_viewer"])),
):
    try:
        return await ams_db.get_all_proxy_providers()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/proxy-change-reasons")
async def get_proxy_change_reasons(
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_all_viewer",
                                                          "ams_proxies_editor", "ams_proxies_viewer"])),
):
    try:
        return await ams_db.get_proxy_change_reasons()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/profiles")
async def get_profiles(
        user: User = Depends(get_current_user_with_roles(["ams_admin"])),
):
    try:
        return await ams_db.get_all_profiles()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/automators")
async def get_automators(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_accounts_editor"]
            )
        ),
):
    try:
        return await ams_db.get_all_automators()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/pos")
async def get_point_of_sale(
        user: User = Depends(get_current_user_with_roles(
            ["ams_admin", "ams_all_viewer", "ams_accounts_editor"]
        )
        ),
):
    try:
        return await ams_db.get_all_point_of_sale()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/automators")
async def update_account_automators(
        account_id: str,
        update_data: dict[Literal['automatorIds'], list[str]],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_account_automators(account_id, update_data["automatorIds"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/accounts/{account_id}/point-of-sale")
async def update_account_point_of_sale(
        account_id: str,
        update_data: dict[Literal['posIds'], list[str]],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_account_point_of_sale(account_id, update_data["posIds"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/proxies/{proxy_id}/star")
async def update_proxy_star(
        proxy_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_proxies_editor"])),
):
    try:
        return await ams_db.update_proxy_star(proxy_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/proxies")
async def create_proxies(
        request: list[ProxyRequestModel],
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_proxies_editor"])),
):
    try:
        return await ams_db.create_proxies(request)
    except ValueError as ve:  # Handle specific value errors
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.put("/proxies/{proxy_id}")
async def update_proxy(
        proxy_id: str,
        request: ProxyRequestModel = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_proxies_editor"])),
):
    try:
        return await ams_db.update_proxy(proxy_id, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/proxies/{proxy_id}")
async def get_proxy(
        proxy_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_proxies_editor", "ams_proxies_viewer"])),
):
    try:
        return await ams_db.get_single_proxy(proxy_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/proxies/status")
async def update_proxies_status(
        updated_data: dict,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_proxies_editor"])),
):
    try:
        return await ams_db.update_proxies_status(updated_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps")
async def get_stages_steps(
        link: str | None = Query(default=None, description="Filter stages by link"),
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"]
            )
        )
):
    try:
        return await ams_db.get_stages_steps(link)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/stages-steps")
async def create_stages(
        stage_name: str = Body(..., description="Name of the stage to create"),
        stage_order: int = Body(..., description="Order index for the new stage"),
        stage_link: str | None = Body(None, description="Optional link for the stage"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.create_stages(stage_name, stage_order, stage_link)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/stages-steps")
async def update_stage_order(
        data: UpdateStageOrder | UpdateStageLink = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    if isinstance(data, UpdateStageOrder):
        try:
            return await ams_db.update_stage_order(data.stage_id, data.new_order_index)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
    elif isinstance(data, UpdateStageLink):
        try:
            return await ams_db.update_stage_link(data.stage_id, data.stage_link)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps/filter-data")
async def get_stages_steps_filter_data(
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"]
            )
        )
):
    try:
        return await ams_db.get_stages_steps_filter_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/stages-steps/{stage_id}")
async def delete_stage(
        stage_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.delete_stage(stage_id)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/stages-steps/{stage_id}")
async def update_stage_name(
        stage_id: str,
        new_name: str = Body(..., description="New name for the stage"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_stage_name(stage_id, new_name)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps/steps")
async def get_steps(
        account_ids: list[str] | None = Query(default=None, description="Account ID"),
        user: User = Depends(
            get_current_user_with_roles(
                ["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"]
            )
        )
):
    try:
        if account_ids:
            return await ams_db.get_steps_for_account(account_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/stages-steps/{account_id}/steps/complete")
async def complete_steps_for_account(
        account_id: str,
        steps: List[str] = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.complete_steps_for_account(account_id, steps)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/stages-steps/{account_id}/steps/uncomplete")
async def uncomplete_step_for_account(
        account_id: str,
        steps: list[str] = Body("steps", embed=True, description="List of step IDs to uncomplete"),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.uncomplete_step_for_account(account_id, steps)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/stages-steps/{stage_id}/steps")
async def create_step(
        stage_id: str,
        step_data: CreateStep = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.create_step(stage_id, step_data)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch("/stages-steps/{stage_id}/steps")
async def update_step_order(
        stage_id: str,
        data: UpdateStepOrder = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_step_order(data.step_id, stage_id, data.new_order_index)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/stages-steps/steps/{step_id}")
async def delete_step(
        step_id: str,
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.delete_step(step_id)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps/{stage_id}/steps/{step_id}/dependencies")
async def get_step_dependencies(
        stage_id: str,
        step_id: str,
        user: User = Depends(
            get_current_user_with_roles(["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_step_dependencies(step_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps/{stage_id}/steps/{step_id}/available-dependencies")
async def get_available_dependencies(
        stage_id: str,
        step_id: str,
        user: User = Depends(
            get_current_user_with_roles(["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_steps_for_dependencies(stage_id, step_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stages-steps/{stage_id}/steps/{step_id}/dependent-steps")
async def get_dependent_steps(
        stage_id: str,
        step_id: str,
        user: User = Depends(
            get_current_user_with_roles(["ams_admin", "ams_all_viewer", "ams_accounts_editor", "ams_accounts_viewer"])),
):
    try:
        return await ams_db.get_dependent_steps(stage_id, step_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/stages-steps/{stage_id}/steps/{step_id}")
async def update_step(
        stage_id: str,
        step_id: str,
        data: UpdateStep = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_step(step_id, data.name, data.type, data.api_details)
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/stages-steps/{stage_id}/steps/{step_id}/dependencies")
async def update_step_dependencies(
        stage_id: str,
        step_id: str,
        data: UpdateStepDependencies = Body(...),
        user: User = Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"])),
):
    try:
        return await ams_db.update_step_dependencies(step_id, data.prerequisite_step_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post(
    "/accounts/sync-ticketsuite-proxy",
    dependencies=[
        Depends(get_current_user_with_roles(["ams_admin", "ams_accounts_editor"]))
    ],)
async def sync_ticketsuite_proxy(
        
        request: dict = Body(...),
):
    results = await ams_db.sync_ticketsuite_proxy(request)
    return {"results": results}
