from pydantic import BaseModel, field_validator, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from uuid import UUID


class ReviewStatusItem(BaseModel):
    event_code: str
    review_status: str | None = None
    created_at: datetime
    updated_at: datetime

class ReviewStatusInput(BaseModel):
    event_code: str
    review_status: Optional[str] = ''
    reviewed_by: Optional[str] = ''

class ReviewStatusRequest(BaseModel):
    items: List[str]

class EventCodesReviewStatusInput(BaseModel):
    review_status: List[str]
    page_size: int = 50
    page: int = 1

class SortConfig(BaseModel):
    columnKey: Optional[str] = None
    order: Optional[str] = None  # 'ascend' | 'descend' | null

class ExtendedFilters(BaseModel):
    rangeFilters: Dict[str, Any]
    sortedColumns: List[str]
    sortConfig: Optional[SortConfig] = None

class CustomViewPayload(BaseModel):
    username: str
    view_name: str
    days_range: int = Field(..., ge=1, le=365)
    filters: ExtendedFilters

class CustomViewResponse(BaseModel):
    id: str
    username: str
    view_name: str
    filters: ExtendedFilters
    days_range: int
    created_at: datetime

class DeleteCustomViewPayload(BaseModel):
    id: UUID
