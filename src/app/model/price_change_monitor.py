from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class PriceChangeMonitorCreate(BaseModel):
    event_id: str
    event_name: str
    increase_threshold: Optional[int] = None
    decrease_threshold: Optional[int] = None
    increase_monitored: bool = False
    decrease_monitored: bool = False
    email_recipients: List[str] = []


class PriceChangeMonitorUpdate(BaseModel):
    event_name: Optional[str] = None
    increase_threshold: Optional[int] = None
    decrease_threshold: Optional[int] = None
    increase_monitored: Optional[bool] = None
    decrease_monitored: Optional[bool] = None
    email_recipients: Optional[List[str]] = None
    status: Optional[str] = None


class PriceChangeMonitorResponse(BaseModel):
    id: int
    event_id: str
    event_name: Optional[str]
    increase_threshold: Optional[int]
    decrease_threshold: Optional[int]
    increase_monitored: bool
    decrease_monitored: bool
    email_recipients: List[str]
    status: str
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str]

    class Config:
        from_attributes = True





