from typing import Optional

from pydantic import BaseModel, EmailStr


class UserEmailCreateRequest(BaseModel):
    company: str
    nickname: str
    gmail_login: EmailStr


class UserEmailUpdateRequest(BaseModel):
    company: Optional[str]
    nickname: Optional[str]
    gmail_login: Optional[EmailStr]
