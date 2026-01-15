from datetime import date, datetime
from decimal import Decimal
from math import atan2, cos, radians, sin, sqrt
import logging
import os
import time
from functools import wraps
from typing import Type, Tuple, Union
from uuid import UUID

import boto3
import httpx

logger = logging.getLogger(__name__)

sqs_client = boto3.client('sqs', os.getenv('AWS_REGION', 'us-east-1'))
queue_url = os.getenv('SQS_CSV_QUEUE_URL')

# SES client initialization
def get_ses_client():
    """Get AWS SES client using environment variables for credentials."""
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    
    if aws_access_key_id and aws_secret_access_key:
        return boto3.client(
            'ses',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    else:
        # Fallback to default credentials (IAM role, etc.)
        return boto3.client('ses', region_name=aws_region)


def retry_on_exception(
        exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]],
        max_attempts: int = 5,
        initial_wait: float = 1,
        backoff_factor: float = 2,
        should_retry_func: callable = None
):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            wait_time = initial_wait
            while attempt < max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts or (should_retry_func and not should_retry_func(e)):
                        raise
                    print(f"Attempt {attempt} failed. Retrying in {wait_time} seconds. Error: {str(e)}")
                    time.sleep(wait_time)
                    wait_time *= backoff_factor

        return wrapper

    return decorator


def haversine_distance(coord1, coord2):
    """Return distance in miles between two (lat, lon) coordinates."""
    R = 3958.8  # Earth radius in miles
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def nearby_states(state: str) -> list[str]:
    state_map = {
        "AL": ["TN", "GA", "FL", "MS"],
        "AK": [],
        "AZ": ["CA", "NV", "UT", "CO", "NM"],
        "AR": ["MO", "TN", "MS", "LA", "TX", "OK"],
        "CA": ["OR", "NV", "AZ"],
        "CO": ["WY", "NE", "KS", "OK", "NM", "UT"],
        "CT": ["NY", "RI", "MA"],
        "DE": ["PA", "MD", "NJ"],
        "FL": ["GA", "AL"],
        "GA": ["FL", "AL", "TN", "NC", "SC"],
        "HI": [],
        "ID": ["MT", "WY", "UT", "NV", "OR", "WA"],
        "IL": ["WI", "IA", "MO", "KY", "IN"],
        "IN": ["MI", "OH", "KY", "IL"],
        "IA": ["MN", "WI", "IL", "MO", "NE", "SD"],
        "KS": ["NE", "MO", "OK", "CO"],
        "KY": ["IL", "IN", "OH", "WV", "VA", "TN", "MO"],
        "LA": ["TX", "AR", "MS"],
        "ME": ["NH", "MA"],
        "MD": ["PA", "DE", "VA", "WV"],
        "MA": ["NY", "CT", "RI", "NH"],
        "MI": ["OH", "IN", "WI"],
        "MN": ["ND", "SD", "IA", "WI"],
        "MS": ["LA", "AR", "TN", "AL"],
        "MO": ["IA", "IL", "KY", "TN", "AR", "OK", "KS", "NE"],
        "MT": ["ND", "SD", "WY", "ID"],
        "NE": ["SD", "IA", "MO", "KS", "CO", "WY"],
        "NV": ["ID", "UT", "AZ", "CA", "OR"],
        "NH": ["VT", "MA", "ME"],
        "NJ": ["NY", "PA", "DE"],
        "NM": ["CO", "OK", "TX", "AZ", "UT"],
        "NY": ["PA", "NJ", "CT", "MA", "VT"],
        "NC": ["VA", "TN", "GA", "SC"],
        "ND": ["MT", "SD", "MN"],
        "OH": ["MI", "IN", "KY", "WV", "PA"],
        "OK": ["KS", "MO", "AR", "TX", "NM", "CO"],
        "OR": ["WA", "ID", "NV", "CA"],
        "PA": ["NY", "NJ", "DE", "MD", "WV", "OH"],
        "RI": ["CT", "MA"],
        "SC": ["NC", "GA"],
        "SD": ["ND", "MN", "IA", "NE", "WY", "MT"],
        "TN": ["KY", "VA", "NC", "GA", "AL", "MS", "AR", "MO"],
        "TX": ["NM", "OK", "AR", "LA"],
        "UT": ["ID", "WY", "CO", "AZ", "NV"],
        "VT": ["NY", "NH", "MA"],
        "VA": ["MD", "DC", "WV", "KY", "TN", "NC"],
        "WA": ["ID", "OR"],
        "WV": ["OH", "PA", "MD", "VA", "KY"],
        "WI": ["MI", "MN", "IA", "IL"],
        "WY": ["MT", "SD", "NE", "CO", "UT", "ID"],
        "DC": ["MD", "VA"],
    }
    return state_map.get(state, [])


def postgres_json_serializer(o):
    if isinstance(o, (datetime, date)):
        # Preserve timezone offset if present
        return o.isoformat()
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, Decimal):
        return float(o)  # or str(o) if you want exact decimal fidelity
    # Add more types as needed
    raise TypeError(f"Type not serializable: {type(o)}")


async def get_ip_info():
    url = "https://ipinfo.io/json"
    # Fail gracefully when network access is blocked (e.g. in local dev/CI).
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as exc:
        logger.warning("Unable to fetch IP info: %s", exc)
        return {"ip": "unknown"}
