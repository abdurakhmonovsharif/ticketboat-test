from enum import Enum


class POStatus(str, Enum):
    UNCLAIMED = 'UNCLAIMED'
    PENDING = 'PENDING'
    COMPLETE = 'COMPLETE'
    PROBLEM = 'PROBLEM'

    @classmethod
    def _missing_(cls, value: str):
        # Case-insensitive match, return in uppercase
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return None
