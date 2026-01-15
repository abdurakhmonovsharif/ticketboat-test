from datetime import datetime
from typing import Literal, Annotated, List, Optional
from uuid import UUID

from pydantic import BaseModel


class ShowDetailsModel(BaseModel):
    id: UUID
    show_name: str
    description: str
    mapped_events_count: int
    show_start: datetime | None
    show_end: datetime | None
    average_markup: float
    total_listings: int
    active_listings_count: int
    synced_events_count: int
    sales_count: int
    sales_total_price: float
    status: Annotated[str, Literal["Active", "Inactive", "Unmapped"]]
    map_info: str


class EventDetailsModel(BaseModel):
    id: str
    title: str
    event_start: datetime
    total_listings_count: int
    total_seats_count: int
    event_markup: float
    markup_override_value: Optional[float]
    markup_override_timing_type: Optional[Annotated[str, Literal["entire_event", "hours_before_event"]]]
    markup_override_timing_from_hours: Optional[int]
    markup_override_timing_to_hours: Optional[int]
    active_listings_count: int
    active_seats_count: int
    shipped_event_sales_orders_count: int
    shipped_event_sales_orders_seats_count: int
    event_sales_total_price: float
    event_status: Annotated[str, Literal["Active", "Inactive", "Unmapped"]]
    source: Annotated[str, Literal["TradeDesk", "Stubhub"]]
    source_event_id: int
    event_name: Optional[str]
    delayed_orders_count: Optional[int]


class ShowModel(BaseModel):
    id: UUID
    title: str
    description: str
    created_at: datetime
    updated_at: Optional[datetime]


class UnmappedEventModel(BaseModel):
    id: str
    title: str
    event_start: datetime
    show: ShowModel
    created_at: datetime
    updated_at: Optional[datetime]
    is_unmapped_in_trade_desk: bool
    is_unmapped_in_stubhub: bool
    mapped_to: str  # "TradeDesk", "StubHub", "TradeDesk, StubHub", or "Unmapped"


class SyncActiveUpdateRequest(BaseModel):
    sync_active: bool


class BulkUpdateRequest(BaseModel):
    outbox_event_ids: List[str]
    sync_active: Optional[bool]
    markup_percent: Optional[float]


class TradeDeskEventCityModel(BaseModel):
    id: int
    name: str
    state: str
    state_code: str


class TradeDeskEventVenueModel(BaseModel):
    id: int
    name: str
    also_called: str
    country: str
    city: TradeDeskEventCityModel


class TradeDeskEventModel(BaseModel):
    id: int
    name: str
    date: str
    venue: TradeDeskEventVenueModel
    timezone: str = "UTC"  # IANA timezone identifier (e.g., "America/Chicago"), defaults to UTC


class StubhubEventCityModel(BaseModel):
    id: int
    name: str
    state: str
    state_code: str


class StubhubEventVenueModel(BaseModel):
    id: int
    name: str
    also_called: str
    country: str
    city: StubhubEventCityModel


class StubhubEventModel(BaseModel):
    id: int
    name: str
    date: str
    venue: StubhubEventVenueModel
    timezone: str = "UTC"  # IANA timezone identifier (e.g., "America/Chicago"), defaults to UTC


class MapEventRequest(BaseModel):
    outbox_event_id: str
    trade_desk_event_id: int
    event_date: str
    timezone: str
    outbox_show_id: str
    trade_desk_event_name: str


class MapStubhubEventRequest(BaseModel):
    outbox_event_id: str
    stubhub_event_id: int
    event_date: str
    timezone: str
    outbox_show_id: str
    stubhub_event_name: str


# Rule Override Models
class RuleOverrideModel(BaseModel):
    id: UUID
    priority_order: int
    show_filter_type: Literal["all", "specific_shows", "specific_events"]
    show_ids: Optional[List[UUID]]
    event_ids: Optional[List[str]]
    seat_filter_type: Literal["all", "first_1_row", "first_2_rows", "first_3_rows", "first_4_rows"]
    action_type: Literal["put_up", "mark_override", "pull_down"]
    action_value: Optional[float]
    timing_type: Literal["entire_event", "hours_before_event"]
    timing_from_hours: Optional[int]
    timing_to_hours: Optional[int]
    is_active: bool
    notes: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]


class RuleOverrideCreateRequest(BaseModel):
    show_filter_type: Literal["all", "specific_shows", "specific_events"]
    show_ids: Optional[List[str]] = None
    event_ids: Optional[List[str]] = None
    seat_filter_type: Literal["all", "first_1_row", "first_2_rows", "first_3_rows", "first_4_rows"]
    action_type: Literal["put_up", "mark_override", "pull_down"]
    action_value: Optional[float] = None
    timing_type: Literal["entire_event", "hours_before_event"]
    timing_from_hours: Optional[int] = None
    timing_to_hours: Optional[int] = None
    is_active: bool = True
    notes: Optional[str] = None


class RuleOverrideUpdateRequest(BaseModel):
    show_filter_type: Optional[Literal["all", "specific_shows", "specific_events"]] = None
    show_ids: Optional[List[str]] = None
    event_ids: Optional[List[str]] = None
    seat_filter_type: Optional[Literal["all", "first_1_row", "first_2_rows", "first_3_rows", "first_4_rows"]] = None
    action_type: Optional[Literal["put_up", "mark_override", "pull_down"]] = None
    action_value: Optional[float] = None
    timing_type: Optional[Literal["entire_event", "hours_before_event"]] = None
    timing_from_hours: Optional[int] = None
    timing_to_hours: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None


class RuleReorderRequest(BaseModel):
    rule_orders: List[dict]  # [{id: UUID, priority_order: int}]


class ShowDropdownModel(BaseModel):
    id: UUID
    show_name: str


class EventDropdownModel(BaseModel):
    id: str
    title: str
    event_start: datetime
    show_id: UUID
    show_name: str
    trade_desk_event_id: Optional[int] = None


class RulePreviewRequest(BaseModel):
    show_filter_type: Literal["all", "specific_shows", "specific_events"]
    show_ids: Optional[List[str]] = None
    event_ids: Optional[List[str]] = None
    seat_filter_type: Literal["all", "first_1_row", "first_2_rows", "first_3_rows", "first_4_rows"]
    action_type: Literal["put_up", "mark_override", "pull_down"]


class RulePreviewResponse(BaseModel):
    affected_shows_count: int
    affected_events_count: int
    affected_listings_count: int
    estimated_impact: str


# Outbox PDF Models
class OutboxPdfRequest(BaseModel):
    document_name: str


class OutboxPdfResponse(BaseModel):
    document_name: str
    pdf_url: Optional[str] = None
    pdf_base64: Optional[str] = None
    status: str
    error_message: Optional[str] = None
