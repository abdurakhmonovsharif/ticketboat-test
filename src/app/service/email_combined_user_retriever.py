from collections import defaultdict
from typing import List, Optional

from firebase_admin import auth as firebase_auth

from app.db import user_db
from app.model.email_combined_user import EmailCombinedUser, EmailCombinedUserDto
from app.model.user import User


class FailedToRetrieveEmailCombinedUser(Exception):
    pass


class EmailCombinedUserRetriever:
    def __init__(self):
        # Note: We need to pull in all the users from firebase in order to construct EmailCombinedUser
        #       because firebase does not provide an email filter on their auth system
        self._all_users_from_firebase = self._query_all_users_from_firebase()
        self._providers_per_email_address = (
            self._construct_providers_per_email_address()
        )
        self._user_ids_per_email_address = self._construct_user_ids_per_email_address()

    def _query_all_users_from_firebase(self) -> list:
        all_firebase_users = []
        page = firebase_auth.list_users()
        while page:
            all_firebase_users.extend(page.users)
            page = page.get_next_page()
        return all_firebase_users

    def _construct_providers_per_email_address(self) -> dict[str, list[str]]:
        providers_per_email_address: defaultdict[str, list[str]] = defaultdict(list)
        for firebase_user in self._all_users_from_firebase:
            email: str = User.get_email_for_firebase_user(firebase_user)
            providers_per_email_address[email].extend(
                [provider.provider_id for provider in firebase_user.provider_data]
            )
        return providers_per_email_address

    def _construct_user_ids_per_email_address(self) -> dict[str, list[str]]:
        user_ids_per_email_address: defaultdict[str, list[str]] = defaultdict(list)
        for firebase_user in self._all_users_from_firebase:
            email: str = User.get_email_for_firebase_user(firebase_user)
            user_ids_per_email_address[email].append(firebase_user.uid)
        return user_ids_per_email_address

    async def get_email_combined_user(self, requested_email: str) -> EmailCombinedUser:
        for email_combined_user in await self.get_all_email_combined_users():
            if email_combined_user.email == requested_email:
                return email_combined_user
        raise FailedToRetrieveEmailCombinedUser()

    async def get_all_email_combined_users(self) -> list[EmailCombinedUser]:
        all_users: dict[str, EmailCombinedUser] = {}
        all_users_from_db: dict[str, dict] = await user_db.get_all_users()
        firebase_users = []
        for firebase_user in self._all_users_from_firebase:
            firebase_users.append(firebase_user.__dict__)
            email: str = User.get_email_for_firebase_user(firebase_user)
            if email in all_users:
                continue
            all_users[email] = (
                await self._create_email_combined_user_from_firebase_user(
                    firebase_user, email, all_users_from_db
                )
            )

        return list(all_users.values() or [])

    async def get_requested_email_combined_users(
            self,
            email: Optional[str] = None,
            roles: Optional[List[str]] = None
    ) -> list[EmailCombinedUser]:
        requested_users = []
        all_users: list[EmailCombinedUser] = await self.get_all_email_combined_users()

        for user in all_users:
            email_match = True if not email else email in user.email
            roles_match = True if not roles else all(role in user.roles for role in roles)

            if email_match and roles_match:
                requested_users.append(user)

        return requested_users

    async def get_email_combined_users_by_pagination(
            self,
            page: Optional[int] = None,
            page_size: Optional[int] = None,
            email: Optional[str] = None,
            roles: Optional[List[str]] = None,
            sort_by: Optional[str] = None,
            sort_order: Optional[str] = None
    ) -> EmailCombinedUserDto:

        all_users: list[EmailCombinedUser] = await self.get_requested_email_combined_users(email, roles)
        total_users = len(all_users)
        if sort_by in ['display_name', 'email']:
            reverse = sort_order.lower() == 'desc' if sort_order else False
            if sort_by == 'display_name':
                all_users.sort(key=lambda user: user.display_name.lower(), reverse=reverse)
            else:
                all_users.sort(key=lambda user: user.email.lower(), reverse=reverse)

        if page and page_size:
            slice_start: int = (page - 1) * page_size
            slice_end: int = page * page_size
            all_users = all_users[slice_start: slice_end]

        return EmailCombinedUserDto(email_combined_users=all_users, total=total_users)

    async def _create_email_combined_user_from_firebase_user(
            self, firebase_user, email: str, all_users_from_db: dict[str, dict]
    ):
        return EmailCombinedUser(
            email=email,
            display_name=User.get_display_name_for_firebase_user(firebase_user),
            roles=all_users_from_db.get(email, {}).get("roles", []),
            user_ids=[firebase_user.uid],
            providers=all_users_from_db.get(email, {}).get("providers", []),
        )
