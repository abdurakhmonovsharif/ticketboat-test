from datetime import datetime
from pydantic import BaseModel


class Comment(BaseModel):
    id: str
    group_id: str
    text: str
    current_user_email: str
    created_at: str
