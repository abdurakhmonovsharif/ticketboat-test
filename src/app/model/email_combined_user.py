from typing import List

from pydantic import BaseModel


class EmailCombinedUser(BaseModel):
    email: str
    display_name: str
    user_ids: list[str]
    roles: list[str]
    providers: list[str]


class EmailCombinedUserDto(BaseModel):
    email_combined_users: List[EmailCombinedUser]
    total: int
