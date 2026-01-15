from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime


class ShadowsTicketmasterEvents(BaseModel):
    id: str
    ticket_number: str
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state: str
    country: str
    geoloc: str
    event_code: str
    is_cancelled: int
    is_sold_out: int
    status: str
    available_facet_status: Optional[str]
    general_facet_status: Optional[str]
    url: str
    created_at: datetime
    visited_at: datetime
    genre: Optional[str] = None
    latitude: str
    longitude: str
    collection_session_ts: datetime
    collection_type: Optional[str]
    price_override: int
    tpo_created_at: Optional[datetime] = None

class ShadowsTicketmasterEventsResponse(BaseModel):
    items: List[ShadowsTicketmasterEvents] = Field(default_factory=list)

class ShadowsTicketmasterSeating(BaseModel):
    id: str
    event_code: str
    event_ticketnumber: str
    offer_code: str
    section: Optional[str] = ''
    row: Optional[str] = ''
    seats: Optional[str] = ''
    seat_from: Optional[str] = ''
    seat_to: Optional[str] = ''
    quantity: Optional[int] = 0
    updated_at: Optional[datetime] = None

class ShadowsTicketmasterSeatingResponse(BaseModel):
    items: List[ShadowsTicketmasterSeating] = Field(default_factory=list)

class ShadowsTicketmasterSearchQuery(BaseModel):
    event_code: Optional[str] = None
    event_name: Optional[str] = None
    start_date: Optional[str] = None
    venue: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
