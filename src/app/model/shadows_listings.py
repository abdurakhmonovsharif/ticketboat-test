from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime


class ShadowsViagogoListingsModel(BaseModel):
    id: str
    event_id: str
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state_province: Optional[str]
    country: str
    market: str = "Viagogo"

class ShadowsVividListingsModel(BaseModel):
    id: int
    event_id: int
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state: str
    country: str = "US"
    market: str = "Vivid"

class ShadowsGoTicketsListingsModel(BaseModel):
    id: str
    event_id: int
    event_name: str
    start_date: datetime
    venue: str
    city: str
    state_province: Optional[str]
    country: str
    market: str = "Gotickets"

class ShadowsSeatGeekListingsModel(BaseModel):
    id: str
    event_id: int
    event_name: str
    start_date: datetime
    venue: str
    city: Optional[str]
    state_province: Optional[str]
    country: Optional[str]
    market: str = "Seatgeek"

class ShadowsListingsModel(BaseModel):
    viagogo: List[ShadowsViagogoListingsModel] = Field(default_factory=list)
    vivid: List[ShadowsVividListingsModel] = Field(default_factory=list)
    gotickets: List[ShadowsGoTicketsListingsModel] = Field(default_factory=list)
    seatgeek: List[ShadowsSeatGeekListingsModel] = Field(default_factory=list)

    def to_items_format(self) -> dict:
        combined_items = self.seatgeek + self.viagogo + self.vivid + self.gotickets
        items_dict = {
            "items": [item.model_dump() for item in combined_items],
            "total": len(combined_items),
        }
        return items_dict

class ShadowsListingSearchModel(BaseModel):
    event_name: Optional[str]
    start_date: Optional[str]
    venue: Optional[str]
    market: Optional[List]

class ViagogoListingModel(BaseModel):
    id: str
    event_id: str
    section: str
    row: Optional[str]
    ticket_price: float
    account: str
    section_id: str

class VividListingsModel(BaseModel):
    id: int
    event_id: int
    section: str
    row: Optional[str]
    ticket_price: float
    account: str
    section_id: str

class GoTicketsListingsModel(BaseModel):
    id: str
    event_id: str
    section: str
    row: Optional[str]
    ticket_price: float
    account: str
    section_id: str

class SeatGeekListingsModel(BaseModel):
    id: str
    event_id: int
    section: str
    row: Optional[str]
    ticket_price: float
    account: str
    section_id: Optional[str]
