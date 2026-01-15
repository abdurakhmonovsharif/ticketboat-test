from pydantic import BaseModel
from typing import List, Optional, Union

class SeatGeekPurgeRequest(BaseModel):
    account_id: Optional[str]
    sub_id: str

class SeatGeekAccount(BaseModel):
    id: str
    sub_id: str
    account_allowed_countries: Optional[Union[str, List[str]]] = None
    account_bucket_range_end: Optional[int] = None
    account_bucket_range_start: Optional[int] = None
    account_created_at: Optional[Union[int, float]] = None
    account_created_at_str: Optional[str]
    account_id: str
    account_marketplaces: Optional[Union[str, List[str]]] = None
    account_max_listings: Optional[int] = None
    account_token: str
    fee_pct: Optional[float] = None
    margin_pct: Optional[float] = None
    blocked_status: Optional[bool] = False
    blocked_at: Optional[str] = None

class SeatGeekAccountListResponse(BaseModel):
    accounts: List[SeatGeekAccount]

class SeatGeekPauseRequest(BaseModel):
    sub_id: str
    pause: bool
