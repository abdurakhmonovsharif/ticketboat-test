from pydantic import BaseModel, field_validator, Field
from typing import Optional, List
from datetime import datetime
from uuid import UUID


class ShadowsWildcardBlacklist(BaseModel):
    id: UUID
    event_name_like: str
    reason: str
    added_by: str
    created_at: datetime
    market: Optional[str]
    city: Optional[str]
    similarity: Optional[str]
    field: Optional[str]

class ShadowsWildcardBlacklistRequest(BaseModel):
    event_name_like: str
    reason: str
    market: Optional[str]
    city: Optional[str]

class ShadowsWildcardBlacklistChangeLog(BaseModel):
    operation: str
    event_name_like: str
    reason: str
    added_by: str
    market: Optional[str]
