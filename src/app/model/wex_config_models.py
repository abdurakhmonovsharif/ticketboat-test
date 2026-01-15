from typing import Dict, Optional
from pydantic import BaseModel, Field


class WEXCredentials(BaseModel):
    """WEX API credentials configuration"""
    org_group_login_id: str = Field(..., description="WEX organization group login ID")
    username: str = Field(..., description="WEX API username")
    password: str = Field(..., description="WEX API password")
    bank_number: str = Field(default="0010", description="WEX bank number")
    company_number: str = Field(..., description="WEX company number")


class WEXCredentialSet(BaseModel):
    """A set of WEX credentials with an identifier"""
    credential_key: str = Field(..., description="Unique key for this credential set (e.g., 'TB', 'OTHER')")
    credentials: WEXCredentials = Field(..., description="The actual credentials")
    company_names: list[str] = Field(default_factory=list, description="Company names that use these credentials")

