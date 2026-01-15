from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from app.db.seatgeek_account_db import (
    get_seat_geek_account_data,
    send_seat_geek_purge_message,
    get_all_seat_geek_accounts,
    update_seat_geek_account_status,
    insert_manage_accounts_history
)
from app.model.shadows_seatgeek import SeatGeekAccountListResponse, SeatGeekAccount, SeatGeekPurgeRequest, SeatGeekPauseRequest
from starlette.concurrency import run_in_threadpool
import logging
from datetime import datetime, timezone
from app.model.user import User
from app.auth.auth_system import (
    get_current_user
)

router = APIRouter(
    prefix="/seatgeek",
    tags=["Seat Geek"],
)


def preprocess_seat_geek_account(acc: dict) -> dict:
    """
    Preprocess a DynamoDB SeatGeek account item to ensure it matches the Pydantic model requirements.
    - Converts lists to comma-separated strings for string fields.
    - Converts float/Decimal to int for account_created_at if needed.
    - Returns the processed dictionary.
    """
    # Convert lists to comma-separated strings for string fields
    if isinstance(acc.get('account_allowed_countries'), list):
        acc['account_allowed_countries'] = ','.join(acc['account_allowed_countries'])
    if isinstance(acc.get('account_marketplaces'), list):
        acc['account_marketplaces'] = ','.join(acc['account_marketplaces'])
    if isinstance(acc.get('account_created_at'), float):
        acc['account_created_at'] = int(acc['account_created_at'])
    if hasattr(acc.get('account_created_at'), 'to_integral_value'):
        acc['account_created_at'] = int(acc['account_created_at'].to_integral_value())
    return acc


@router.post("/purge/listings/")
async def purge_seat_geek_listings(
        payload: SeatGeekPurgeRequest,
        user: User = Depends(get_current_user()),
):
    """
    Purge SeatGeek listings for a given account.
    Expects a JSON body with account_id and sub_id fields.
    """
    try:
        user.assert_is_admin()
        sub_id = payload.sub_id
        queue_message_id = None
        if sub_id is None:
            raise HTTPException(status_code=400, detail="Account id is required fields.")
        account_data = await run_in_threadpool(get_seat_geek_account_data, sub_id)
        if not account_data.get("account_token"):
            raise HTTPException(status_code=404, detail="SeatGeek account not found.")
        queue_message_id = send_seat_geek_purge_message(account_data.get("account_token"))
        if not queue_message_id:
            raise HTTPException(status_code=500, detail="Failed to send purge request.")
        user_roles_str = ','.join(user.roles) if isinstance(user.roles, (list, tuple)) else str(user.roles)
        await insert_manage_accounts_history(
            account_id="seatgeek_account",
            account_name=sub_id or "",
            user_name=user.email,
            user_role=user_roles_str,
            change_event_type="PURGE_LISTINGS"
        )
        return JSONResponse(content={"success": True, "queue_id": queue_message_id }, status_code=200)
    except Exception as e:
        logging.error(f"PURGE SEAT GEEK LISTINGS ERROR => {str(e)}")
        raise HTTPException(status_code=500, detail="Something went wrong.")


@router.get("/accounts/", response_model=SeatGeekAccountListResponse)
async def get_seat_geek_accounts(
        user: User = Depends(get_current_user()),
):
    """
    Get all SeatGeek accounts from DynamoDB.
    This endpoint fetches all SeatGeek account records from the DynamoDB table and returns them
    as a list of dictionaries to the frontend, using a Pydantic response model for validation.
    """
    try:
        user.assert_is_admin()
        accounts = await run_in_threadpool(get_all_seat_geek_accounts)
        if not accounts:
            raise HTTPException(status_code=404, detail="Failed to get SeatGeek accounts.")
        account_models = [SeatGeekAccount(**preprocess_seat_geek_account(acc)) for acc in accounts if isinstance(acc, dict)]
        return {"accounts": account_models}
    except Exception as e:
        logging.error(f"GET SEAT GEEK ACCOUNTS ERROR => {str(e)}")
        raise HTTPException(status_code=500, detail="Something went wrong.")



@router.post("/pause/listings/")
async def pause_seat_geek_account(
        payload: SeatGeekPauseRequest,
        user: User = Depends(get_current_user()),
):
    """
    Pause or unpause a SeatGeek account by setting blocked_status based on the pause flag.
    Expects a JSON body with sub_id and pause fields.
    """
    try:
        user.assert_is_admin()
        sub_id = payload.sub_id
        pause = payload.pause
        if not sub_id:
            raise HTTPException(status_code=400, detail="sub_id is a required field.")
        blocked_status = pause
        blocked_at = datetime.now(timezone.utc).isoformat() if pause else None
        await run_in_threadpool(update_seat_geek_account_status, sub_id, blocked_status, blocked_at)
        user_roles_str = ','.join(user.roles) if isinstance(user.roles, (list, tuple)) else str(user.roles)
        await insert_manage_accounts_history(
            account_id="seatgeek_account",
            account_name=sub_id or "",
            user_name=user.email,
            user_role=user_roles_str,
            change_event_type="PAUSE_LISTINGS"
        )
        return JSONResponse(content={"success": True, "blocked_status": blocked_status}, status_code=200)
    except Exception as e:
        logging.error(f"PAUSE SEAT GEEK ACCOUNT ERROR => {str(e)}")
        raise HTTPException(status_code=500, detail="Something went wrong.")
