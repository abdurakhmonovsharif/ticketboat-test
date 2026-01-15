from typing import List

from pydantic import BaseModel


class MarkHandledResponse(BaseModel):
    handled_events: List[str]
    failed_events: List[str]
