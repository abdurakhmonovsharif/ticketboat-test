from pydantic import BaseModel
from typing import List


class EmailCommentCreateRequest(BaseModel):
    email_id: str
    text: str

class GroupCommentCreateRequest(BaseModel):
    group_id: str
    text: str

class EmailForwardRequest(BaseModel):
    email_ids: List[str]
    forward_to: List[str]
