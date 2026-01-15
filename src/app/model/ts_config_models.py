from pydantic import BaseModel


class TSCredentials(BaseModel):
    """TicketSuite API credentials for an automator"""
    api_key: str

