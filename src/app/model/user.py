import os
from typing import Optional
from pydantic import BaseModel, root_validator
from fastapi import HTTPException, status

from firebase_admin._user_mgt import UserRecord
from firebase_admin import db

from app.db import user_db

TRUSTED_PROVIDERS = ["google.com", "microsoft.com"]


class MissingEmailForFirebaseUser(Exception):
    pass


class MissingProviderForFirebaseUser(Exception):
    pass


class User(BaseModel):
    user_id: str
    name: Optional[str] = None
    email: str
    email_verified: bool
    roles: list[str] = []
    providers: list[str] = []

    @root_validator(pre=True)
    def default_name(cls, values):
        if "name" not in values or not values["name"]:
            values["name"] = "Unknown Person"
        return values

    def has_role(self, role_name: str):
        return self.roles and role_name in self.roles

    @staticmethod
    async def create_user_from_firebase_user_record(
        firebase_user: UserRecord,
    ):
        email = User.get_email_for_firebase_user(firebase_user)
        roles = await user_db.get_roles_for_email(email)
        providers = await user_db.get_providers_for_email(email)

        user = User(
            user_id=firebase_user.uid,
            name=User.get_display_name_for_firebase_user(firebase_user),
            email=email,
            email_verified=firebase_user.email_verified,
            roles=roles,
            providers=providers,
        )

        if (
            user.email_verified
            or firebase_user.provider_data[0].provider_id in TRUSTED_PROVIDERS
        ):
            if "public" not in user.roles:
                user.roles.append("public")

            if (
                user.email.lower().endswith("@ticketboat.com")
                and "user" not in user.roles
            ):
                user.roles.append("user")

            if (
                user.email.lower() == os.environ["DEFAULT_ADMIN_EMAIL"].lower()
                and "admin" not in user.roles
            ):
                user.roles.append("admin")

        await user_db.upsert_user(user)

        return user

    def assert_is_admin(self):
        if not self.has_role("admin"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    def assert_user_has_role(self, role: str):
        if not self.has_role(role):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    @staticmethod
    def get_provider_for_firebase_user(firebase_user: UserRecord) -> str:
        if firebase_user.provider_data and firebase_user.provider_data[0].provider_id:
            return firebase_user.provider_data[0].provider_id
        raise MissingProviderForFirebaseUser(
            f"Cannot determine provider for firebase user: {firebase_user.uid}"
        )

    @staticmethod
    def get_email_for_firebase_user(firebase_user: UserRecord) -> str:
        if firebase_user.email:
            return firebase_user.email
        if firebase_user.provider_data and firebase_user.provider_data[0].email:
            return firebase_user.provider_data[0].email
        raise MissingEmailForFirebaseUser(
            f"Cannot determine email for firebase user: {firebase_user.uid}"
        )

    @staticmethod
    def get_display_name_for_firebase_user(firebase_user: UserRecord) -> str:
        if firebase_user.display_name:
            return firebase_user.display_name
        elif firebase_user.provider_data:
            return firebase_user.provider_data[0].display_name or ""
        return ""

    @staticmethod
    def get_email_key(email: str) -> str:
        return email.replace(".", ",")
