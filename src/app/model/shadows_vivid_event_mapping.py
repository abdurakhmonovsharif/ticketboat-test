from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime, timezone


class ShadowsUnmappedVividEventsModel(BaseModel):
    exchange: str
    event_code: str
    event_name: str
    start_date: datetime
    venue: str
    city: str
    url: str
    ignore: Optional[str]
    primary: Optional[str]

class ShadowsVividEventMappingViewModel(BaseModel):
    id: Optional[str]
    ticketmaster_event_code: str
    vivid_event_id: int
    datetime_added: Optional[datetime]
    datetime_updated: Optional[datetime]
    ticketmaster_id: str
    ticket_number: str
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state: str
    country: str
    event_code: str
    is_cancelled: Optional[int]
    is_sold_out: Optional[int]
    status: str
    available_facet_status: Optional[str]
    general_facet_status: Optional[str]
    url: Optional[str]
    vivideventid: Optional[int]
    vivideventname: Optional[str]
    eventdate: Optional[datetime]
    weburi: Optional[str]
    primary: Optional[str]

class ShadowsVividSearchMappedEventModel(BaseModel):
    ticketmaster_event_code: Optional[str]
    event_name: Optional[str]

class ShadowsUpdateEventModel(BaseModel):
    ticketmaster_event_code: str
    vivid_event_id: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

class ShadowsUpdateIgnoreModel(BaseModel):
    ticketmaster_event_code: str
    ignore: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

class ShadowsRemoveEventModel(BaseModel):
    ticketmaster_event_code: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
