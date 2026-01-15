from pydantic import BaseModel


class Post(BaseModel):
    id: str
    user_id: int
    title: str
    body: str
