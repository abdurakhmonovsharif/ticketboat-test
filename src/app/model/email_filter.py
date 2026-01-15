from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class EmailFilterCreate(BaseModel):
    archive: bool = Field(default=False)
    mark_as_read: bool = Field(default=False)
    star: bool = Field(default=False)
    add_comment: bool = Field(default=False)
    flags: Optional[List[str]] = Field(default_factory=list)  # Array of strings
    users: Optional[List[str]] = Field(default_factory=list)  # Array of strings
    from_: Optional[str] = Field(default="", alias="from")  # Alias for 'from' due to it being a reserved word
    to: Optional[str] = Field(default="")
    subject: Optional[str] = Field(default="")
    does_not_have: Optional[str] = Field(default="")
    search_term: Optional[str] = Field(default="")
    forward_to: Optional[List[str]] = Field(default_factory=list)  # Array of strings


class TagUpdateRequest(BaseModel):
    tags: list[str]


class FlagUpdateRequest(BaseModel):
    flag_ids: List[str]
    has_archived: bool

class OpenSearchFlagUpdateRequest(BaseModel):
    flags: List[str]

class FlagBulkUpdateRequest(BaseModel):
    flag_ids: List[str]
    email_ids: List[str]

class OpenSearchFlagBulkUpdateRequest(BaseModel):
    flags: List[str]
    email_group_ids: List[str]


class UsersUpdateRequest(BaseModel):
    user_ids: List[str]



class UsersBulkUpdateRequest(BaseModel):
    email_group_ids: List[str]
    user_ids: List[str]


class OnsaleIgnoreRequest(BaseModel):
    is_ignored: bool = Field(..., description="Whether to set the onsale as ignored (true) or not ignored (false)")


class OnsaleMarkAddedRequest(BaseModel):
    is_added: bool = Field(..., description="Whether to set the onsale as added (true) or not added (false)")


class OnsaleEmailItem(BaseModel):
    id: str = Field(..., description="Unique identifier for the onsale record")
    venue: Optional[str] = Field(None, description="Venue name")
    performer: Optional[str] = Field(None, description="Performer name")
    promoter: Optional[str] = Field(None, description="Promoter name")
    discount_code: Optional[str] = Field(None, description="Comma-separated discount codes")
    presale_code: Optional[str] = Field(None, description="Comma-separated presale codes")
    price: Optional[float] = Field(None, description="Minimum price")
    email_id: Optional[str] = Field(None, description="Associated email ID")
    last_received: Optional[datetime] = Field(None, description="Last received timestamp in specified timezone")
    event_name: Optional[str] = Field(None, description="Event name")
    event_datetime: Optional[datetime] = Field(None, description="Event date and time")
    onsale_or_presale_ts: Optional[datetime] = Field(None, description="Onsale or presale timestamp")
    discovery_date: Optional[datetime] = Field(None, description="Discovery date in specified timezone")
    event_url: Optional[str] = Field(None, description="Event URL")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State")
    is_matched: bool = Field(False, description="Whether the event is matched in daily onsales")
    is_added: bool = Field(False, description="Whether the onsale has been added")
    added_at: Optional[datetime] = Field(None, description="When it was added")
    added_by: Optional[str] = Field(None, description="Who added it")
    is_ignored: bool = Field(False, description="Whether the onsale has been ignored")
    ignored_at: Optional[datetime] = Field(None, description="When it was ignored")
    ignored_by: Optional[str] = Field(None, description="Who ignored it")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")


class OnsaleEmailResponse(BaseModel):
    items: List[OnsaleEmailItem] = Field(..., description="List of onsale email items")
    total: int = Field(..., description="Total count of items matching the criteria")
