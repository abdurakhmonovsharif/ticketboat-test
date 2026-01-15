from enum import Enum


class VirtualOrderXUserAssignedStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    REJECTED = "REJECTED"


class VirtualOrderStatus(Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
