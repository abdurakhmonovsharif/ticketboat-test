from pydantic import BaseModel, field_validator, Field
from typing import List, Optional
from datetime import datetime, timezone
from uuid import uuid4


class ShadowsUserTrackerModel(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex)
    operation: str
    module: str
    user: str
    data: str
    created: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
