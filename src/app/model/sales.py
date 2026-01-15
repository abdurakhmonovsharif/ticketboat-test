from datetime import datetime
from typing import Optional, List, Dict

from pydantic import BaseModel


class UnclaimedSalesSerializer(BaseModel):
    id: str
    topic: Optional[str] = None
    status: Optional[str] = None
    nocharge_price: Optional[float] = None
    amount: Optional[float] = None
    event_name: Optional[str] = None
    event_state: Optional[str] = None
    start_date: Optional[datetime] = None
    currency_code: Optional[str] = None
    country: Optional[str] = None
    created_at: Optional[datetime] = None
    account_id: Optional[str] = None
    event_id: Optional[int] = None
    section: Optional[str] = None
    row: Optional[str] = None
    orig_section: Optional[str] = None
    orig_row: Optional[str] = None
    quantity: Optional[int] = None
    venue: Optional[str] = None
    city: Optional[str] = None
    external_id: Optional[str] = None
    delivery_method: Optional[str] = None
    link: Optional[str] = None
    listing_notes: Optional[str] = None
    exchange: Optional[str] = None
    sales_source: Optional[str] = None
    event_code: Optional[str] = None
    performer_id: Optional[str] = None
    venue_id: Optional[str] = None
    has_discounts: bool = False
    has_potential_discount: bool = False


class ClaimSalesRequest(BaseModel):
    id: str
    event_state: Optional[str] = None
    account_id: str
    exchange: str
    created_at: str
    event_name: str
    start_date: str
    section: Optional[str] = None
    orig_section: Optional[str] = None
    row: Optional[str] = None
    orig_row: Optional[str] = None
    quantity: int
    venue: str
    city: str
    nocharge_price: Optional[float] = None
    amount: float
    listing_notes: Optional[str] = None
    sales_source: str
    link: Optional[str] = None
    currency_code:  Optional[str] = None
    event_code:  Optional[str] = None
    performer_id: Optional[str] = None
    venue_id: Optional[str] = None
    primary_status: Optional[str] = None


class ClaimSalesResponse(BaseModel):
    claimed_sales: List[str]
    already_claimed_sales: List[str]
    failed_sales: List[str]
