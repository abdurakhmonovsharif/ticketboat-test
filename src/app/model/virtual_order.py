from typing import Optional

from pydantic import BaseModel

from app.enums.virtual_order_enums import VirtualOrderStatus


class VirtualOrderDto(BaseModel):
    event_url: str
    section_list: str
    restrictions: str
    max_buyers: Optional[int] = None
    status: Optional[VirtualOrderStatus] = VirtualOrderStatus.ACTIVE
    priority_level: int
    assigned_buyers: Optional[list] = None
