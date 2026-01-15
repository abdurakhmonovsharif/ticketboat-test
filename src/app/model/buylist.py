from pydantic import BaseModel, field_validator, Field
from typing import Optional, List
from datetime import datetime


class BuyListItemSerializer(BaseModel):
    id: str
    event_state: Optional[str]
    account_id: Optional[str]
    exchange: Optional[str]
    transaction_date: Optional[datetime]
    event_name: Optional[str]
    event_date: Optional[datetime]
    section: Optional[str]
    row: Optional[str]
    orig_section: Optional[str] = None
    orig_row: Optional[str] = None
    quantity: Optional[int]
    venue: Optional[str]
    venue_city: Optional[str]
    buylist_status: Optional[str]
    link: Optional[str]
    subs: Optional[int]
    viagogo_order_status: Optional[str]
    card: Optional[str]
    amount: Optional[float]
    confirmation_number: Optional[str]
    buyer: Optional[str]
    delivery_method: Optional[str]
    discount: Optional[str]
    was_offer_extended: Optional[int]
    nih: Optional[int]
    mismapped: Optional[int]
    was_discount_code_used: Optional[int]
    date_last_checked: Optional[datetime]
    date_tickets_available: Optional[datetime]
    bar_code: Optional[str]
    notes: Optional[str]
    buylist_order_status: Optional[str]
    escalated_to: Optional[str]
    listing_notes: Optional[str]
    sales_source: Optional[str]
    created_at: Optional[datetime]
    nocharge_price: Optional[float]
    purchase_confirmation_created_at: Optional[datetime]
    order_claimed_created_at: Optional[datetime]
    buyer_email: Optional[str]
    currency_code: Optional[str]
    grabbed_at: Optional[datetime]
    has_discounts: bool
    event_code: Optional[str]
    performer_id: Optional[str]
    venue_id: Optional[str]
    has_potential_discount: bool
    is_hardstock: Optional[bool] = False
    primary_status: Optional[str]


class UpdateBuyListItemRequest(BaseModel):
    card: Optional[str] = None
    confirmation_number: Optional[str] = None
    discount: Optional[str] = None
    date_last_checked: Optional[datetime] = None
    date_tickets_available: Optional[datetime] = None
    was_discount_code_used: Optional[int] = Field(None, description="1 for Yes, 0 for No")
    was_offer_extended: Optional[int] = Field(None, description="1 for Yes, 0 for No")
    nih: Optional[int] = Field(None, description="1 for Yes, 0 for No")
    subs: Optional[int] = Field(None, description="1 for Yes, 0 for No")
    mismapped: Optional[int] = Field(None, description="1 for Yes, 0 for No")
    bar_code: Optional[str] = None
    notes: Optional[str] = None
    buylist_order_status: Optional[str] = Field(
        None,
        description="Status of the order",
        pattern="^(Fulfilled|Invoiced|Rejected|Cancelled|Pending|Unbought)$"
    )
    escalated_to: Optional[str] = Field(
        None,
        description="Escalation level",
        pattern="^(Tier 1|Tier 2|Tier 3|Tier 4)$"
    )
    is_hardstock: Optional[bool] = Field(None, description="Whether this is hard stock")

    @field_validator("date_last_checked", "date_tickets_available")
    def ensure_date(cls, value):
        if isinstance(value, datetime):
            return value.date()
        return value

    @field_validator("was_discount_code_used", "was_offer_extended", "nih", "subs")
    def validate_binary_fields(cls, value):
        if value not in {0, 1, None}:
            raise ValueError("This field must be either 1 (Yes) or 0 (No).")
        return value

    @field_validator("buylist_order_status")
    def validate_buylist_order_status(cls, value):
        allowed_statuses = {"Fulfilled", "Invoiced", "Rejected", "Cancelled", "Pending", "Unbought"}
        if value and value not in allowed_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(allowed_statuses)}")
        return value

    @field_validator("escalated_to")
    def validate_escalated_to(cls, value):
        allowed_tiers = {"Tier 1", "Tier 2", "Tier 3", "Tier 4"}
        if value and value not in allowed_tiers:
            raise ValueError(f"Invalid tier. Must be one of: {', '.join(allowed_tiers)}")
        return value

class BatchUpdateBuylistRequest(BaseModel):
    item_ids: List[str]
    buylist_order_status: str

    @field_validator("buylist_order_status")
    def validate_buylist_order_status(cls, value):
        allowed_statuses = {"Fulfilled", "Invoiced", "Rejected", "Cancelled", "Pending", "Unbought"}
        if value not in allowed_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(allowed_statuses)}")
        return value

class SaveErrorReportRequest(BaseModel):
    error: str
    account_id: str
    buylist_id: str

class UnclaimSalesRequest(BaseModel):
    ids: List[str]

class UnclaimSalesResponse(BaseModel):
    ids: List[str]
    blocked_ids: List[str] = []

class SaleHistoryModel(BaseModel):
    id: str
    operation: str
    module: str
    user: str
    created: Optional[datetime] = None
    email: str


class SuggestionsRequest(BaseModel):
    item_ids: List[str]


class SuggestionsResponse(BaseModel):
    id: str
    suggested_accounts: List[str]
    created_at: datetime
    suggestions: List[dict]
