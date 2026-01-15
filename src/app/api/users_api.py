import json
import os
import traceback
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from firebase_admin import auth
from firebase_admin._user_mgt import ExportedUserRecord
from pydantic import BaseModel

from app.auth.auth_system import (
    get_current_user,
    get_current_user_with_roles,
)
from app.db import role_db, user_db
from app.model.create_user_input import CreateUserInput, UpdateUserInput
from app.model.email_combined_user import EmailCombinedUserDto, EmailCombinedUser
from app.model.user import User
from app.service.email_combined_user_retriever import EmailCombinedUserRetriever

router = APIRouter()


class BulkRoleInput(BaseModel):
    email: str
    roles: list[str]


class RoleInput(BaseModel):
    email: str
    role: str


DEFAULT_ROLES = ["admin", "user", "public"]


@router.get("/users/bootstrap", include_in_schema=False)
async def bootstrap_default_roles():
    existing_roles = await role_db.get_all_roles()

    if not existing_roles:
        await role_db.set_all_roles(DEFAULT_ROLES)
    else:
        existing_roles = list(existing_roles)
        for default_role in DEFAULT_ROLES:
            if default_role not in existing_roles:
                existing_roles.append(default_role)
        await role_db.set_all_roles(existing_roles)

    await _upsert_roles(os.environ["DEFAULT_ADMIN_EMAIL"], DEFAULT_ROLES)


@router.post("/users/init")
async def init_user(
        user: User = Depends(get_current_user()),
):
    await bootstrap_default_roles()
    if user.email_verified:
        new_roles = ["public"]
        if _is_default_admin_user(user):
            new_roles.append("admin")
        await _upsert_roles(user.email, new_roles)
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Email not verified yet"
        )


@router.post("/users/refresh_and_sync_roles")
async def refresh_and_sync_roles(
        data: dict,
        user: User = Depends(get_current_user()),
):
    email = data["email"]
    firebase_user_id = data["firebase_user_id"]
    pg_user = await user_db.get_user_for_email(email)
    custom_claims = {
        "roles": pg_user["roles"],
        "pg_uid": pg_user["id"]
    }
    auth.set_custom_user_claims(firebase_user_id, custom_claims)


@router.get("/users")
async def get_users(
        email: Optional[str] = Query(None, description="Users whose email contains this text will be returned"),
        roles: Optional[str] = Query("", description="Comma-separated list of roles to filter users by"),
        page: Optional[int] = Query(None, description="The page number to retrieve users from"),
        page_size: Optional[int] = Query(None, description="The number of users to retrieve per page"),
        sort_by: Optional[str] = Query(None, description="Column to sort by (name or email)"),
        sort_order: Optional[str] = Query(None, description="Sort order (asc or desc)"),
        current_user: User = Depends(get_current_user()),
) -> EmailCombinedUserDto:
    try:
        _assert_user_retrieval_permissions(email, current_user)
        return await EmailCombinedUserRetriever().get_email_combined_users_by_pagination(
            page,
            page_size,
            email=email,
            roles=roles.split(",") if roles else [],
            sort_by=sort_by,
            sort_order=sort_order
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500,
                            detail=f"Error retrieving users: {str(e)} user :  {json.dumps(current_user)}")


def _assert_user_retrieval_permissions(
        requested_email: Optional[str], current_user: User
):
    if current_user.has_role("admin"):
        return
    if requested_email and current_user.email == requested_email:
        return
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"Only admin can retrieve all users",
    )


@router.delete("/users")
async def delete_user(
        email: str = Query(..., description="The email of the user to retrieve roles for"),
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        # List all users
        page = auth.list_users()
        users_to_delete = []

        while page:
            for firebase_user in page.users:
                if User.get_email_for_firebase_user(firebase_user) == email:
                    users_to_delete.append(firebase_user.uid)
            page = page.get_next_page()

        if not users_to_delete:
            raise HTTPException(
                status_code=404, detail=f"No users found with email {email}"
            )

        # Delete all users with the specified email
        for user_id in users_to_delete:
            auth.delete_user(user_id)

        await user_db.delete_user(email)

        return {"message": f"Deleted {len(users_to_delete)} user(s) with email {email}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting user: {str(e)}")


@router.post("/users")
async def create_user(
        create_user_input: CreateUserInput,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    # Extract data from the request
    email = create_user_input.email
    password = create_user_input.password
    force_change_password = create_user_input.force_user_to_change_password

    try:
        # Create the user in Firebase Authentication
        new_user = auth.create_user(
            email=email,
            password=(
                password if password else None
            ),  # Set the provided password or omit it if None
        )

        # Optionally set a flag to force the user to change the password
        if force_change_password:
            auth.update_user(
                new_user.uid,
                password=password,
                custom_claims={"force_password_change": True},
            )

        return {
            "message": "User created successfully",
            "user_id": new_user.uid,
            "email": new_user.email,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")


@router.put("/users/{firebase_user_id}")
async def update_user(
        firebase_user_id: str,
        update_user_input: UpdateUserInput,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await _update_user_info(firebase_user_id, update_user_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating user: {str(e)}")


@router.get("/users/roles")
async def get_user_roles(
        email: str = Query(..., description="The email of the user to retrieve roles for"),
        user: User = Depends(get_current_user()),
) -> list[str]:
    if email != user.email:
        user.assert_is_admin()
    try:
        return await user_db.get_roles_for_email(email)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving user roles: {str(e)}"
        )


@router.get("/users/buyer")
async def get_buyers(
        user: User = Depends(get_current_user_with_roles(["captain", "admin"])),
) -> list[dict]:
    try:
        return await user_db.get_buyers()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving user roles: {str(e)}"
        )


@router.post("/users/set_roles")
async def set_roles(
        role_input: BulkRoleInput,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        await _set_roles(role_input.email, role_input.roles)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to add role")


@router.post("/users/bulk_role_update")
async def set_bulk_roles_update(
        role_input: List[BulkRoleInput],
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        await _set_bulk_roles(role_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to add role")


@router.post("/users/add_role")
async def add_role(
        role_input: RoleInput,
        user: User = Depends(get_current_user()),
):
    try:
        if user.has_role("admin") or role_input.role == "public":
            await _upsert_role(role_input.email, role_input.role)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to add role")


@router.post("/users/delete_role")
async def delete_role(
        role_input: RoleInput,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        await _delete_role(role_input.email, role_input.role)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to add role")


async def _set_roles(user_email: str, roles: list[str]):
    # Ensure the default admin user retains admin privileges
    if _is_email_default_admin_user(user_email) and "admin" not in roles:
        roles.append("admin")
    await user_db.set_roles_for_email(user_email, roles)


async def _set_bulk_roles(bulk_roles_input: List[BulkRoleInput]):
    for role_input in bulk_roles_input:
        await _set_roles(role_input.email, role_input.roles)


async def _upsert_role(user_email: str, new_role: str):
    await _upsert_roles(user_email, [new_role])


async def _upsert_roles(user_email: str, new_roles: list[str]):
    current_roles = await user_db.get_roles_for_email(user_email)

    # Merge new roles with current roles
    new_roles_copy = new_roles.copy()
    for current_role in current_roles:
        if current_role.lower() not in new_roles_copy:
            new_roles_copy.append(current_role)

    await user_db.set_roles_for_email(user_email, new_roles_copy)


async def _update_user_info(firebase_user_id: str, update_user_input: UpdateUserInput) -> dict:
    new_name = update_user_input.name.strip()
    new_email = update_user_input.email.strip()

    user_retriever = EmailCombinedUserRetriever()
    users_with_new_email: list[EmailCombinedUser] = await user_retriever.get_requested_email_combined_users(new_email)
    existing_user = auth.get_user(firebase_user_id)
    user_email = User.get_email_for_firebase_user(existing_user)
    is_email_updated = (user_email != new_email)

    if is_email_updated and not is_available_email(new_email, users_with_new_email):
        raise HTTPException(status_code=400, detail="A user with the provided email already exists")

    update_data = {}
    if new_name:
        update_data['display_name'] = new_name
    if new_email and is_email_updated:
        update_data['email'] = new_email

    all_users_from_firebase: list[ExportedUserRecord] = user_retriever._query_all_users_from_firebase()
    try:
        if update_data:
            for firebase_user in all_users_from_firebase:
                firebase_user_email = User.get_email_for_firebase_user(firebase_user)

                if firebase_user_email == user_email and firebase_user.uid == existing_user.uid:
                    auth.update_user(
                        uid=firebase_user.uid,
                        **update_data
                    )
                    print("Successfully updated user data in Firebase")
                elif firebase_user_email == user_email:
                    auth.delete_user(firebase_user.uid)
                    print("Successfully deleted user data in Firebase")
    except auth.EmailAlreadyExistsError:
        raise HTTPException(status_code=400, detail="Email already in use")
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error updating user in Firebase")

    try:
        await user_db.update_user_email_and_name_for_email(user_email, new_email, new_name)
        print("Successfully updated user data in PostgreSQL")
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error updating user in PostgreSQL")

    return {
        "message": "User updated successfully"
    }


def is_available_email(email: str, users: list[EmailCombinedUser]) -> bool:
    for user in users:
        if email == user.email:
            return False
    return True


async def verify_user_exists_by_email(email: str) -> bool:
    user = await user_db.get_user_for_email(email)
    return True if user else False


async def _delete_role(user_email: str, role_to_delete: str):
    await _delete_roles(user_email, [role_to_delete])


async def _delete_roles(user_email: str, roles_to_delete: list[str]):
    current_roles = await user_db.get_roles_for_email(user_email)
    roles_to_keep = [
        role for role in current_roles if role.lower() not in roles_to_delete
    ]
    await user_db.set_roles_for_email(user_email, roles_to_keep)


def _is_default_admin_user(user: User):
    return _is_email_default_admin_user(user.email)


def _is_email_default_admin_user(user_email: str):
    return user_email == os.environ["DEFAULT_ADMIN_EMAIL"]
