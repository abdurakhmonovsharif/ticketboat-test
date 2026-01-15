from typing import Optional

from pydantic import BaseModel


class ShadowsOfferTypesModel(BaseModel):
    name: str
    url: Optional[str] = None
    valid: Optional[bool] = None
    offer_hash: str

