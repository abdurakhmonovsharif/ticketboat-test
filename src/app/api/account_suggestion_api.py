##############################################################################
# File: account_suggestion_api.py                                            #
# Description: API endpoints for account suggestions and event details.      #
##############################################################################


import asyncio
import datetime
import json
import time
from collections import deque
from typing import Literal

import anyio
import os
import httpx
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Response,
    WebSocketDisconnect,
    status,
    WebSocket,
)

from app.auth.auth_system import get_current_user_with_roles
from app.db.account_suggestion_db import (
    TicketLimitResult,
    fetch_active_account_nicknames,
    fetch_suggestion_feedback,
    get_asset_account_suggestions,
    get_event_details,
    get_shadows_account_suggestions,
    get_unbought_ticket_limits,
    write_cooloff_record,
    write_suggestion_feedback,
)
from app.db.app_config_db import get_config_value
from app.db.ams_db import get_account_tags_for_account
from app.tasks.shadows_suggestions import refetch_shadows_suggestions_task
from app.utils import nearby_states, postgres_json_serializer

from pydantic import BaseModel

router = APIRouter(prefix="/suggestions")


class BulkAccountSuggestionEvent(BaseModel):
    eventId: str
    state: str
    latLng: tuple[float, float] | list[float] | None = None
    eventName: str | None = None


class BulkAccountSuggestionRequest(BaseModel):
    events: list[BulkAccountSuggestionEvent]
    tags: list[str]
    companies: list[str]
    tagLogicalOperator: Literal["AND", "OR"] = "AND"
    tagPresenceOperator: Literal["HAS", "DOES NOT HAVE"] = "HAS"


VIAGOGO_EVENT_SEARCH_URL = "https://api.viagogo.net/catalog/events/search"
VIAGOGO_MAX_CONCURRENT_REQUESTS = 3
VIAGOGO_MAX_REQUESTS_PER_WINDOW = 10
VIAGOGO_WINDOW_SECONDS = 30
VIAGOGO_CACHE_TTL_SECONDS = 15

_viagogo_concurrency = asyncio.Semaphore(VIAGOGO_MAX_CONCURRENT_REQUESTS)
_viagogo_rate_lock = asyncio.Lock()
_viagogo_request_timestamps: deque[float] = deque()
_viagogo_cache_lock = asyncio.Lock()
_viagogo_cache: dict[str, tuple[float, dict]] = {}
_viagogo_session_lock = asyncio.Lock()
_viagogo_session_tokens: dict[str, float] = {}


class _ViagogoRateLimiter:
    """Limits Viagogo traffic to avoid noisy bursts."""

    async def __aenter__(self):
        await _viagogo_concurrency.acquire()
        while True:
            async with _viagogo_rate_lock:
                now = time.monotonic()
                while (
                    _viagogo_request_timestamps
                    and (now - _viagogo_request_timestamps[0]) > VIAGOGO_WINDOW_SECONDS
                ):
                    _viagogo_request_timestamps.popleft()

                if len(_viagogo_request_timestamps) < VIAGOGO_MAX_REQUESTS_PER_WINDOW:
                    _viagogo_request_timestamps.append(now)
                    break

                sleep_for = VIAGOGO_WINDOW_SECONDS - (
                    now - _viagogo_request_timestamps[0]
                )

            await anyio.sleep(max(sleep_for, 0))

    async def __aexit__(self, exc_type, exc, tb):
        _viagogo_concurrency.release()


async def _register_viagogo_session(session_id: str) -> float:
    token = time.monotonic()
    async with _viagogo_session_lock:
        _viagogo_session_tokens[session_id] = token
    return token


async def _viagogo_session_is_current(
    session_id: str | None, token: float | None
) -> bool:
    if not session_id or token is None:
        return True
    async with _viagogo_session_lock:
        return _viagogo_session_tokens.get(session_id) == token


async def _clear_viagogo_session_token(session_id: str | None, token: float | None):
    if not session_id or token is None:
        return
    async with _viagogo_session_lock:
        if _viagogo_session_tokens.get(session_id) == token:
            _viagogo_session_tokens.pop(session_id, None)


@router.get(
    "/event/{event_id}",
    dependencies=[Depends(get_current_user_with_roles(["user"]))],
)
async def fetch_event_details(event_id: str):
    """
    Fetch details of a specific event by its ID.
    """
    try:
        return await get_event_details(event_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get(
    "/account-suggestions",
    dependencies=[Depends(get_current_user_with_roles(["user"]))],
)
async def fetch_account_suggestions(
    event_state: str = Query(default=None, description="State code of the event"),
    nearby_states: list[str] = Query(
        default=None, description="List of nearby state codes"
    ),
    company_id: str = Query(
        default=None, description="Company ID for filtering suggestions"
    ),
    event_id: str = Query(
        default=None, description="Event ID for filtering suggestions"
    ),
    pos: str | None = Query(
        default=None, description="Point of Sale (e.g., 'StubHub', 'SkyBox')"
    ),
    limit: int = Query(default=None, description="Limit the number of results"),
    lat_lng: str = Query(default=None, description="Lat and Long"),
):
    """
    Fetch account suggestions based on event state, nearby states, company ID, and event ID.
    """
    SHADOWS_COMPANY_IDS = {
        "4d187c5e-b74d-456a-a690-a68f3014c548",
        "2cb42500-171d-4e3f-8e9f-81921dc9e801",
    }

    ASSET_COMPANY_IDS = {
        "b1bb69ce-6c20-4ab0-a931-26c75a0417fd",
        "74aec335-0d4a-4aef-93b1-449c3d7e1d24",
    }

    try:
        is_shadows = company_id in SHADOWS_COMPANY_IDS and pos
        is_asset = company_id in ASSET_COMPANY_IDS

        if is_shadows:
            default_ticket_limit = int(
                await get_config_value("default_ticket_limit") or 4
            )

            # TODO: update ticket limit logic for show runs and multi-day event.
            # For now, we are using event_id as event_code to fetch ticket limit based on single event only.
            ticket_limit_response = await get_unbought_ticket_limits(
                [
                    {
                        "id": event_id,
                        "performer_id": "",
                        "venue_id": "",
                        "event_code": event_id,
                    }
                ]
            )

            if (
                ticket_limit_response
                and ticket_limit_response[0]["limit_value"] >= default_ticket_limit
            ):
                print("Using fetched ticket limit.")
                final_ticket_limit: TicketLimitResult = ticket_limit_response[0]
            else:
                print("Using default ticket limit.")
                final_ticket_limit: TicketLimitResult = {
                    "id": event_id,
                    "event_code": event_id,
                    "venue_code": None,
                    "performer_id": None,
                    "limit_type": "show",
                    "limit_value": default_ticket_limit,
                }

            return await get_shadows_account_suggestions(
                event_state=event_state,
                nearby_states=nearby_states,
                company_id=company_id,
                event_id=event_id,
                pos=pos,
                lat_lng=lat_lng,
                ticket_limit=final_ticket_limit,
            )

        if is_asset:
            # TODO: Implement asset specific ticket limit logic.
            return await get_asset_account_suggestions(
                event_state=event_state,
                nearby_states=nearby_states,
                company_id=company_id,
                event_id=event_id,
                pos=pos if pos else None,
                lat_lng=lat_lng,
            )
    except Exception as e:
        print(e)
        await _clear_viagogo_session_token(session_id, session_token)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post(
    "/account-suggestions/feedback",
    dependencies=[Depends(get_current_user_with_roles(["user"]))],
    status_code=status.HTTP_201_CREATED,
)
async def log_suggestion_feedback(
    attempt_data: dict, background_tasks: BackgroundTasks
):
    try:
        record = await write_suggestion_feedback(attempt_data)
        # certain error codes trigger a cool-off, and a background task to refetch suggestions
        if record["error_code"] in ["PAUSED", "U102", "U103", "U201"]:
            await write_cooloff_record(record)
            background_tasks.add_task(refetch_shadows_suggestions_task)
        return {"status": "success", "data": record}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.websocket(
    "/ws/account-suggestions/feedback",
)
async def websocket_suggestion_feedback(websocket: WebSocket):
    await websocket.accept()
    params = dict(websocket.query_params)

    try:
        timeframe_param = (
            params.get("timeframe")
            if params.get("timeframe") is not None
            else params.get("history")
        )
        history = (
            float(timeframe_param) if timeframe_param is not None else 24
        )  # default to last 24 hours
        # minimum lookback is 1 hour to limit payload size
        interval = float(params.get("interval", 3))  # default to every 3 seconds
        if interval < 3:
            interval = 3
        if history < 1:
            history = 1
    except ValueError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    watermark = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
        hours=history
    )
    watermark = watermark.replace(tzinfo=None)  # Remove timezone info for DB comparison

    try:
        initial_data = await fetch_suggestion_feedback(watermark)
        if initial_data:
            serialized = json.loads(
                json.dumps(initial_data, default=postgres_json_serializer)
            )
            await websocket.send_json(serialized)

            newest_ts = max(
                row["created_at"] for row in initial_data if row.get("created_at")
            )

            if newest_ts:
                watermark = newest_ts

        while True:
            await anyio.sleep(interval)
            new_data = await fetch_suggestion_feedback(watermark)
            if new_data:
                serialized = json.loads(
                    json.dumps(new_data, default=postgres_json_serializer)
                )
                await websocket.send_json(serialized)
                newest_ts = max(
                    row["created_at"] for row in new_data if row.get("created_at")
                )

                if newest_ts:
                    watermark = newest_ts
    except WebSocketDisconnect:
        return
    except Exception as e:
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason=str(e))
        return


@router.get(
    "/active-nicknames", dependencies=[Depends(get_current_user_with_roles(["user"]))]
)
async def get_active_account_nicknames(
    company_id: list[str] | None = Query(
        default=None, description="Filter by company IDs"
    )
):
    try:
        return await fetch_active_account_nicknames(company_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get(
    "/search-events",
    dependencies=[Depends(get_current_user_with_roles(["user"]))],
)
async def search_events(
    q: str = Query(default="", description="Search query string"),
    debounce_session_id: str | None = Query(
        default=None,
        description="Client-side debounce session identifier so only the latest response is returned.",
    ),
):
    print(f"Searching events with query: {q}")
    session_id = debounce_session_id.strip() if debounce_session_id else None
    session_token = await _register_viagogo_session(session_id) if session_id else None
    cache_key = q.strip().lower()
    now = time.monotonic()

    async with _viagogo_cache_lock:
        cached = _viagogo_cache.get(cache_key)
        if cached and (now - cached[0]) < VIAGOGO_CACHE_TTL_SECONDS:
            if not await _viagogo_session_is_current(session_id, session_token):
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            await _clear_viagogo_session_token(session_id, session_token)
            return cached[1]

    if not await _viagogo_session_is_current(session_id, session_token):
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    token = os.getenv("VIAGOGO_API_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="VIAGOGO_API_TOKEN not set")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            async with _ViagogoRateLimiter():
                res = await client.get(
                    VIAGOGO_EVENT_SEARCH_URL,
                    params={"q": q, "exclude_parking_passes": True},
                    headers={"Authorization": f"Bearer {token}"},
                )
                res.raise_for_status()
            payload = res.json()

        def _parse_start_ts(iso_str: str | None) -> datetime.datetime | None:
            if not iso_str:
                return None
            try:
                return datetime.datetime.fromisoformat(iso_str)
            except ValueError:
                return None

        items = payload.get("_embedded", {}).get("items", [])
        formatted: list[dict] = []
        for item in items:
            start_iso = item.get("start_date")
            parsed_ts = _parse_start_ts(start_iso)
            on_sale_iso = item.get("on_sale_date")

            venue_data = item.get("_embedded", {}).get("venue", {})
            venue_name = venue_data.get("name", "Unknown venue")
            venue_city = venue_data.get("city")
            venue_state = venue_data.get("state_province")
            venue_lat = venue_data.get("latitude")
            venue_lng = venue_data.get("longitude")
            postal_code = venue_data.get("postal_code")
            venue_country = (
                venue_data.get("_embedded", {}).get("country", {}).get("name")
            )
            venue_country_code = (
                venue_data.get("_embedded", {}).get("country", {}).get("code")
            )
            formatted.append(
                {
                    "id": item.get("id"),
                    "event_name": item.get("name", "Unnamed event"),
                    "venue": venue_name,
                    "city": venue_city,
                    "state": venue_state,
                    "postal_code": postal_code,
                    "country": venue_country,
                    "country_code": venue_country_code,
                    "latitude": venue_lat,
                    "longitude": venue_lng,
                    "start_timestamp": start_iso,
                    "on_sale_date": on_sale_iso,
                    "status": item.get("status"),
                    "min_ticket_price": item.get("min_ticket_price"),
                    "webpage": item.get("_links", {})
                    .get("event:webpage", {})
                    .get("href"),
                    "_sort_ts": parsed_ts,
                }
            )

        formatted.sort(key=lambda entry: (entry["_sort_ts"] is None, entry["_sort_ts"]))
        for entry in formatted:
            entry.pop("_sort_ts", None)

        response = {
            "query": q,
            "total_items": payload.get("total_items", 0),
            "items": formatted,
            "_links": payload.get("_links", {}),
        }
        async with _viagogo_cache_lock:
            _viagogo_cache[cache_key] = (time.monotonic(), response)

        if not await _viagogo_session_is_current(session_id, session_token):
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        await _clear_viagogo_session_token(session_id, session_token)
        return response
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post(
    "/bulk-account-suggestions",
    dependencies=[Depends(get_current_user_with_roles(["user"]))],
)
async def fetch_asset_account_suggestions(body: BulkAccountSuggestionRequest):
    """
    Fetch asset account suggestions for multiple event IDs.
    """
    print("Bulk fetching asset account suggestions.")
    SHADOWS_COMPANY_IDS = {
        "4d187c5e-b74d-456a-a690-a68f3014c548",
        "2cb42500-171d-4e3f-8e9f-81921dc9e801",
    }

    ASSET_COMPANY_IDS = {
        "b1bb69ce-6c20-4ab0-a931-26c75a0417fd",
        "74aec335-0d4a-4aef-93b1-449c3d7e1d24",
    }

    default_ticket_limit = None
    if any(company_id in SHADOWS_COMPANY_IDS for company_id in body.companies):
        default_ticket_limit = int(await get_config_value("default_ticket_limit") or 4)

    async def _fetch_suggestions(
        event_id: str,
        event_state: str,
        nearby: list[str],
        lat_lng: str | None,
        company_id: str,
    ) -> tuple[str, str, list[dict]]:

        if company_id in SHADOWS_COMPANY_IDS:
            effective_ticket_limit = (
                default_ticket_limit if default_ticket_limit is not None else 4
            )

            ticket_limit_response = await get_unbought_ticket_limits(
                [
                    {
                        "id": event_id,
                        "performer_id": "",
                        "venue_id": "",
                        "event_code": event_id,
                    }
                ]
            )

            if (
                ticket_limit_response
                and ticket_limit_response[0]["limit_value"] >= effective_ticket_limit
            ):
                final_ticket_limit: TicketLimitResult = ticket_limit_response[0]
            else:
                final_ticket_limit: TicketLimitResult = {
                    "id": event_id,
                    "event_code": event_id,
                    "venue_code": None,
                    "performer_id": None,
                    "limit_type": "show",
                    "limit_value": effective_ticket_limit,
                }

            return (
                event_id,
                company_id,
                await get_shadows_account_suggestions(
                    event_state=event_state,
                    nearby_states=nearby,
                    company_id=company_id,
                    event_id=event_id,
                    pos=None,
                    lat_lng=lat_lng,
                    ticket_limit=final_ticket_limit,
                ),
            )

        if company_id in ASSET_COMPANY_IDS:
            return (
                event_id,
                company_id,
                await get_asset_account_suggestions(
                    event_state=event_state,
                    nearby_states=nearby,
                    company_id=company_id,
                    event_id=event_id,
                    pos=None,
                    lat_lng=lat_lng,
                ),
            )

        return (event_id, company_id, [])

    tasks = []
    event_contexts = []
    event_records: dict[str, list[dict]] = {event.eventId: [] for event in body.events}
    for event in body.events:
        event_state = event.state
        event_contexts.append(
            (
                event.eventId,
                event_state,
                nearby_states(event_state) if event_state else [],
                f"{event.latLng[0]},{event.latLng[1]}" if event.latLng else None,
            )
        )

    for event_id, event_state, nearby, lat_lng in event_contexts:
        for company_id in body.companies:
            tasks.append(
                _fetch_suggestions(
                    event_id,
                    event_state,
                    nearby,
                    lat_lng,
                    company_id,
                )
            )

    results = await asyncio.gather(*tasks) if tasks else []
    for event_id, _, suggestions in results:
        event_records.setdefault(event_id, []).extend(suggestions or [])

    if not body.tags:
        return event_records

    tag_ids = list(dict.fromkeys(body.tags))
    all_account_ids = {
        account["id"]
        for suggestions in event_records.values()
        for account in (suggestions or [])
        if account.get("id")
    }

    tags_by_account = (
        await get_account_tags_for_account(
            [str(acc_id) for acc_id in list(all_account_ids)]
        )
        if all_account_ids
        else {}
    )

    logical_op = (body.tagLogicalOperator or "AND").upper()
    presence_op = (body.tagPresenceOperator or "HAS").upper()

    def _matches_tag_filter(acc_id: str | None) -> bool:
        if not acc_id:
            return False
        acc_key = str(acc_id)
        account_tags = {tag["id"] for tag in tags_by_account.get(acc_key, [])}

        def _tag_predicate(tag_id: str) -> bool:
            has_tag = tag_id in account_tags
            return has_tag if presence_op == "HAS" else not has_tag

        evaluations = [_tag_predicate(tag_id) for tag_id in tag_ids]
        if logical_op == "OR":
            return any(evaluations)
        return all(evaluations)

    return {
        event_id: [
            acct for acct in (suggestions or []) if _matches_tag_filter(acct.get("id"))
        ]
        for event_id, suggestions in event_records.items()
    }
