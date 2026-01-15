from typing import Optional

from pydantic import BaseModel


class SuperPriorityEventRequest(BaseModel):
    event_code: str
    event_url: Optional[str]
    start_time: str
