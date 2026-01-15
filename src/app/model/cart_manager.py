from typing import Optional, List

from pydantic import BaseModel, Field


class CartManagerUpdate(BaseModel):
    sub_id: str
    status: str
    comment: Optional[str] = None


class CartManagerUpdateNew(BaseModel):
    id: str
    status: str
    comment: Optional[str] = None


class BulkCartManagerUpdate(BaseModel):
    cart_ids: List[str]
    status: str = Field(...)
    comment: Optional[str] = None


class AutoApprovePayload(BaseModel):
    event_name: str
    venue: str
    event_date_time: str
    match_section: Optional[str] = None
    match_price: Optional[float] = None
    rule_action: str
    created_by: str


class UpdateAutoApprovePayload(BaseModel):
    event_name: str
    venue: str
    event_date_time: str
    match_section: Optional[str] = None
    match_price: Optional[float] = None
    rule_action: str
    is_active: Optional[bool] = True
