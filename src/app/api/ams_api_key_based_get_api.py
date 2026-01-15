from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import validate_ams_api_key
from app.db import ams_db

from app.service.ticketmaster_account_creation import (
    TMOTP,
    get_ticketmaster_email_auth_code,
    get_ticketmaster_sms_auth_code,
)

router = APIRouter(prefix="/ams-local")


@router.get("/accounts/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_accounts():
    try:
        return await ams_db.get_all_accounts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_emails():
    try:
        return await ams_db.get_all_emails()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/addresses/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_addresses():
    try:
        return await ams_db.get_all_addresses()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/persons/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_persons():
    try:
        return await ams_db.get_all_persons()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/metro-areas/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_metro_areas():
    try:
        return await ams_db.get_all_metro_areas()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/states/all", dependencies=[Depends(validate_ams_api_key)])
async def get_all_states():
    try:
        return await ams_db.get_all_states()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.patch(
    "/account/{account_id}/multilogin", dependencies=[Depends(validate_ams_api_key)]
)
async def update_account(account_id: str, body: dict):
    try:
        await ams_db.update_ams_account_multilogin_id(account_id, body["multilogin_id"])
        return {"message": "Account updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get(
    "/tm-setup/sms-otp",
    dependencies=[Depends(validate_ams_api_key)],
    response_model=TMOTP,
)
async def get_tm_auth_sms_otp(
    phone: str = Query(..., description="The phone number to search"),
) -> TMOTP:
    try:
        return await get_ticketmaster_sms_auth_code(phone)
    except HTTPException:
        raise


@router.get(
    "/tm-setup/email-otp",
    dependencies=[Depends(validate_ams_api_key)],
    response_model=TMOTP,
)
async def get_tm_auth_email_otp(
    email: str = Query(..., description="The email address to search"),
) -> TMOTP:
    try:
        return await get_ticketmaster_email_auth_code(email)
    except HTTPException:
        raise
