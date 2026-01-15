from typing import List
from pydantic import BaseModel, validator


class DailyEmailsRequest(BaseModel):
    emails: List[str]
    
    @validator('emails')
    def validate_emails(cls, v):
        if not v:
            raise ValueError('Emails list cannot be empty')
        
        # Basic email format validation
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for email in v:
            if not re.match(email_pattern, email):
                raise ValueError(f'Invalid email format: {email}')
        
        return v
