from datetime import datetime, timedelta
import re
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from fastapi import HTTPException
from pydantic import BaseModel

from app.db.incoming_texts_db import get_ticketmaster_otp_message
from app.service.email_service import EmailService


class OTP(BaseModel):
    auth_code: str


class TMOTP(BaseModel):
    detail: OTP | str


def extract_html_auth_code(html: str) -> str | None:
    """
    Extract the authentication code from the given HTML email content.

    Args:
        html (str): The HTML content of the email.

    Returns:
        Optional[str]: The extracted authentication code, or None if not found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) Prefer the cell that follows the "Your Authentication Code:" label
    label = soup.find(string=re.compile(r"Your\s+Authentication\s+Code", re.I))
    if label:
        td_with_label = label.find_parent("td")
        if td_with_label:
            td_with_code = td_with_label.find_next("td")
            if td_with_code:
                m = re.search(r"\b(\d{4,8})\b", td_with_code.get_text(" ", strip=True))
                if m:
                    return m.group(1)

    # 2) Fallback: look for the styled code cell
    for td in soup.select("td.buttonblock, td.innertd"):
        m = re.search(r"\b(\d{4,8})\b", td.get_text(" ", strip=True))
        if m:
            return m.group(1)

    return None


async def get_ticketmaster_email_auth_code(email_address: str) -> TMOTP:
    """
    Fetches the most reccent Ticketmaster auth code email content, and extracts the auth code.

    Args:
        email_address (str): The email address the auth code was sent to.

    Returns:
        TMOTP: The auth code details.
    """
    now = datetime.now(ZoneInfo("America/Chicago"))
    today = now.strftime("%Y-%m-%d")
    auth_email_list = await EmailService().get_email_list(
        page_size=1,
        timezone="America/Chicago",
        start_date=today,
        end_date=today,
        to_email=email_address,
        subject="Here's Your Authentication Code",
    )
    if auth_email_list and auth_email_list["total"] < 1:
        raise HTTPException(status_code=404, detail="No matching email found")
    else:
        last_received_raw = auth_email_list["items"][0]["last_received"]
        print(f"Last received raw: {last_received_raw}")
        try:
            last_received_dt = datetime.strptime(
                last_received_raw, "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=ZoneInfo("America/Chicago"))
        except ValueError:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected datetime format in last_received: {last_received_raw}",
            )

        window_start = now - timedelta(minutes=1)
        window_end = now + timedelta(minutes=1)
        if not (window_start <= last_received_dt <= window_end):
            raise HTTPException(status_code=416, detail="No new email found")

        email_id = auth_email_list["items"][0]["emails"][0]["id"]
        email_str = await EmailService().get_email_content(email_id=email_id)
        auth_code = extract_html_auth_code(email_str)
        if auth_code:
            return TMOTP(detail=OTP(auth_code=auth_code))
        else:
            raise HTTPException(
                status_code=500,
                detail="Could not extract auth code from email content.",
            )


async def get_ticketmaster_sms_auth_code(recipient: str) -> TMOTP:
    """
    Fetches the most recent Ticketmaster SMS auth code content, and extracts the auth code.

    Args:
        recipient (str): The phone or email the sms auth code was sent to.

    Returns:
        TMOTP: The auth code details.
    """
    message = await get_ticketmaster_otp_message(recipient)
    if message:
        m = re.search(r"\b(\d{4,8})\b", message)
        if m:
            return TMOTP(detail=OTP(auth_code=m.group(1)))
        else:
            raise HTTPException(
                status_code=500, detail="Could not extract auth code from SMS content."
            )
    else:
        raise HTTPException(status_code=404, detail="No matching SMS found")
