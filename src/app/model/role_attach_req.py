from typing import List

from pydantic import BaseModel, EmailStr


class RoleAttachReq(BaseModel):
    uid: str
    roles: List[str]
