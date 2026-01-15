from typing import Optional
from pydantic import BaseModel


class CreateUserInput(BaseModel):
    email: str
    password: Optional[str] = None
    force_user_to_change_password: bool


class UpdateUserInput(BaseModel):
    name: str
    email: str
