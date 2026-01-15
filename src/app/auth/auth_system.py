import os
import traceback

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
from firebase_admin import auth

from app.db import user_db
from app.model.user import User
from app.service.email_combined_user_retriever import EmailCombinedUserRetriever

security = HTTPBearer()


class AllowedRolesListCannotBeEmpty(Exception):
    pass


def get_current_user_with_roles(allowed_roles: list[str]):
    async def dependency(
            token: HTTPAuthorizationCredentials = Depends(security),
    ) -> User:
        return await _get_current_user_with_roles(allowed_roles, token)

    return dependency


async def _get_current_user_with_roles(
        allowed_roles: list[str], token: HTTPAuthorizationCredentials
) -> User:
    if not allowed_roles:
        raise AllowedRolesListCannotBeEmpty()

    user = await _get_current_user(token)
    if _has_overlap(user.roles, allowed_roles):
        return user
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"User must have at least one of the following roles: {', '.join(allowed_roles)}",
        )


def _has_overlap(list1, list2):
    return bool(set(list1) & set(list2))


def get_current_user():
    async def dependency(
            token: HTTPAuthorizationCredentials = Depends(security),
    ) -> User:
        return await _get_current_user(token)

    return dependency


async def _get_current_user(token: HTTPAuthorizationCredentials) -> User:
    if not token:
        raise HTTPException(status_code=500, detail="Missing authentication token")

    try:
        decoded_token = auth.verify_id_token(token.credentials)
        firebase_user = auth.get_user(decoded_token["uid"])

        email = User.get_email_for_firebase_user(firebase_user)
        provider = User.get_provider_for_firebase_user(firebase_user)

        await user_db.upsert_firebase_user_ids_for_email(email, [firebase_user.uid])
        await user_db.upsert_providers_for_email(email, [provider])

        email_combined_user = (
            await EmailCombinedUserRetriever().get_email_combined_user(email)
        )
        user: User = await User.create_user_from_firebase_user_record(firebase_user)
        user.roles = email_combined_user.roles
        user.providers = email_combined_user.providers

        return user
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=403, detail=f"Invalid authentication credentials: {e}"
        )
    except:
        traceback.print_exc()
        raise HTTPException(
            status_code=403, detail=f"Invalid authentication credentials"
        )


load_dotenv()

AMS_API_KEY = os.getenv("AMS_API_KEY")
AMS_API_KEY_NAME = "X-AMS-API-Key"
ams_api_key_header = APIKeyHeader(name=AMS_API_KEY_NAME, auto_error=True)


def validate_ams_api_key(ams_api_key: str = Security(ams_api_key_header)):
    if ams_api_key != AMS_API_KEY:
        raise HTTPException(status_code=403, detail=f"Invalid API Key")
    return ams_api_key
