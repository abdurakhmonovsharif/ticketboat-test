import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from app.enums.po_queue_enums import POStatus


class POUpdatePayload(BaseModel):
    status: Optional[POStatus] = None


class POCreateRequest(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    email_id: Optional[str] = None
    account: Optional[str] = None
    card: Optional[str] = None
    event: Optional[str] = None
    opponent: Optional[str] = None
    venue: Optional[str] = None
    date: Optional[str] = None
    time: Optional[str] = None
    tba: Optional[bool] = None
    shipping_method: Optional[str] = None
    quantity: Optional[int] = None
    section: Optional[str] = None
    row: Optional[str] = None
    start_seat: Optional[str] = None
    end_seat: Optional[str] = None
    total_cost: Optional[float] = None
    conf_number: Optional[str] = None
    consecutive: Optional[bool] = None
    internal_note: Optional[str] = None
    external_note: Optional[str] = None
    po_note: Optional[str] = None
    po_number: Optional[str] = None
    created: str = Field(default_factory=lambda: datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    status: Optional[str] = Field(default="Unclaimed")
