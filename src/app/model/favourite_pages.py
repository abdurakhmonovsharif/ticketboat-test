from pydantic import BaseModel
from typing import Optional


class FavouritePageCreate(BaseModel):
    page_url: str
    page_label: Optional[str] = None