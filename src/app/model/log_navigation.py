from pydantic import BaseModel


class LogNavigation(BaseModel):
    page_url: str
    page_label: str