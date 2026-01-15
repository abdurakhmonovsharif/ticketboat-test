from typing import List, Optional

from pydantic import BaseModel


class ReportPayload(BaseModel):
    title: str
    link: str
    category_id: str
    roles: List[str]
    description: Optional[str] = None


class CategoryPayload(BaseModel):
    title: str
    description: Optional[str] = None