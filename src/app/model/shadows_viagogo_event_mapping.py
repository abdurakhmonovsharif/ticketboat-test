from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime, timezone


class ShadowsViagogoUnmappedEventsModel(BaseModel):
    event_name: str
    start_date: datetime
    datetime_added: datetime
    datetime_updated: datetime
    venue: str
    event_code: str
    url: str
    available_seats: Optional[float]
    ignore: Optional[str]
    primary: Optional[str]

class ShadowsViagogoMappedEventsModel(BaseModel):
    id: int
    ticketmaster_event_code: str
    viagogo_event_id: str
    datetime_added: datetime
    datetime_updated: datetime
    ticketmaster_id: str
    ticket_number: str
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state: str
    country: str
    event_code: str
    is_cancelled: int
    is_sold_out: int
    status: str
    available_facet_status: Optional[str]
    general_facet_status: Optional[str]
    url: str
    stubhubeventid: Optional[int]
    stubhubeventname: Optional[str]
    eventdate: Optional[datetime]
    eventtime: Optional[datetime]
    catalog_id: Optional[int]
    weburi: Optional[str]
    primary: Optional[str]

class ShadowsViagogoSearchMappedEventModel(BaseModel):
    ticketmaster_event_code: Optional[str]
    event_name: Optional[str]

class ShadowsUpdateEventModel(BaseModel):
    ticketmaster_event_code: str
    viagogo_event_id: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

class ShadowsUpdateIgnoreModel(BaseModel):
    ticketmaster_event_code: str
    ignore: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

class ShadowsRemoveEventModel(BaseModel):
    ticketmaster_event_code: str
    datetime_updated: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

class ShadowsUpdateSuccessModel(BaseModel):
    pass

class ShadowsUpdateErrorModel(BaseModel):
    pass
