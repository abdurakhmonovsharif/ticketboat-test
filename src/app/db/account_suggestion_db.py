##############################################################################
# File: account_suggestion_db.py                                             #
# Description: Database interactions for account suggestions                 #
#              and event details.                                            #
##############################################################################


from datetime import datetime, timedelta, timezone
import os
import random
import json
from collections.abc import Iterable
import logging
from typing import Literal, TypedDict, cast
import anyio

from fastapi import HTTPException

from snowflake.connector import DictCursor
from app.database import (
    get_pg_buylist_database,
    get_pg_buylist_readonly_database,
    get_pg_readonly_database,
    get_snowflake_connection,
)
from app.db.app_config_db import get_config_value
from app.utils import haversine_distance, postgres_json_serializer, nearby_states


class TicketLimitData(TypedDict):
    id: str
    performer_id: str | None
    venue_id: str | None
    event_code: str | None


class TicketLimitResult(TypedDict):
    id: str
    event_code: str | None
    venue_code: str | None
    performer_id: str | None
    limit_type: Literal["show", "run"]
    limit_value: int


class UnsuggestedEvent(TypedDict):
    id: str
    event_code: str
    pos: str
    currency_code: str
    ticket_limit: TicketLimitResult | None


async def _run_snowflake_async(
    query,
    params=None,
    *,
    fetcher=lambda cur: cur.fetchall(),
    cursor_cls=DictCursor,
    poll_interval=0.5,
):
    conn = get_snowflake_connection()

    def _start():
        cur = conn.cursor(cursor_cls)
        cur.execute_async(query, params)
        return cur, cur.sfqid

    cur, query_id = await anyio.to_thread.run_sync(_start) # pyright: ignore[reportAttributeAccessIssue]

    try:
        while conn.is_still_running(
            await anyio.to_thread.run_sync(conn.get_query_status, query_id) # pyright: ignore[reportAttributeAccessIssue]
        ):
            await anyio.sleep(poll_interval)

        def _fetch():
            cur.get_results_from_sfqid(query_id)
            return fetcher(cur)

        return await anyio.to_thread.run_sync(_fetch) # pyright: ignore[reportAttributeAccessIssue]
    finally:
        cur.close()


async def get_event_details(event_id: str):
    query = """
        SELECT
            ee.source_name,
            ee.event_url,
            me.event_name,
            me.start_date,
            mv.venue_name,
            mv.city,
            mv.state,
            mv.latitude,
            mv.longitude
        FROM edm.matched_venue mv
        LEFT JOIN edm.matched_event me ON me.matched_venue_id = mv.matched_venue_id
        LEFT JOIN edm.exchange_event ee ON me.matched_event_id = ee.matched_event_id
        WHERE ee.source_key = %(event_id)s;
    """

    raw = await _run_snowflake_async(
        query=query, params={"event_id": event_id}, fetcher=lambda cur: cur.fetchone()
    )

    row = dict(raw)
    return {
        "id": event_id,
        "sourceName": row["SOURCE_NAME"],
        "eventUrl": row["EVENT_URL"],
        "eventName": row["EVENT_NAME"],
        "eventDate": row["START_DATE"],
        "venueName": row["VENUE_NAME"],
        "venueCity": row["CITY"],
        "venueState": row["STATE"],
        "lat": row["LATITUDE"],
        "lng": row["LONGITUDE"],
    }


async def get_asset_event_purchasers(event_id: str):
    try:
        query = """
            WITH target AS (
                SELECT
                    ee.matched_event_id
                FROM
                    edm.exchange_event ee
                WHERE
                    ee.source_key = %(event_id)s QUALIFY ROW_NUMBER() OVER (
                        ORDER BY
                            ee.start_date DESC NULLS LAST
                    ) = 1
            )
            SELECT
                DISTINCT v.email,
                ee.event_name,
                po.quantity
            FROM
                inventory_management.ticket tk
                JOIN inventory_management.ticket_ticketgroup ttg ON ttg.ticket_id = tk.ticket_id
                JOIN inventory_management.ticketgroup tg ON tg.ticketgroup_id = ttg.ticketgroup_id
                JOIN inventory_management.purchase_order po ON po.purchase_order_id = tk.purchase_order_id
                AND po.status <> 'REMOVED'
                LEFT JOIN inventory_management.vendor v ON v.vendor_id = po.vendor_id
                JOIN inventory_management.event e ON e.event_id = tg.primary_event_id
                JOIN edm.exchange_event ee ON ee.source_key = e.source_key
                JOIN target t ON t.matched_event_id = ee.matched_event_id
                JOIN inventory_management.source_type s ON s.source_type_id = tk.source_type_id
            WHERE
                tk.is_deleted = FALSE
                AND s.is_enabled = TRUE
                AND s.source_type_id = 110
                AND tg.status <> 'VOIDED'
            ORDER BY
                po.quantity DESC;
            """
        rows = await _run_snowflake_async(query, {"event_id": event_id})
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_asset_last_purchase_date(account_emails: list[str]):
    try:
        emails = sorted({e.strip().lower() for e in account_emails if e and e.strip()})
        query = """
            WITH emails AS (
                SELECT
                    LOWER(value::string) AS email
                FROM
                    TABLE(FLATTEN(INPUT => PARSE_JSON(%s)))
            )
            SELECT
                e.email,
                MAX(po.purchase_order_date) AS last_po_date
            FROM
                emails e
                JOIN inventory_management.vendor v ON LOWER(v.email) = e.email
                JOIN inventory_management.purchase_order po ON po.vendor_id = v.vendor_id
            WHERE
                po.status <> 'REMOVED'
            GROUP BY
                e.email
            ORDER BY
                last_po_date DESC;
            """
        rows = await _run_snowflake_async(query, (json.dumps(emails),))
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shadows_event_usage(event_id: str):
    try:
        query = """
            SELECT
                *
            FROM
                shadows_account_usage
            WHERE
                tm_event_code = :event_id
            ORDER BY
                last_used DESC;
        """
        pg_results = await get_pg_buylist_database().fetch_all(
            query, values={"event_id": event_id}
        )
        return pg_results
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shadows_last_used(account_nicknames: list[str]):
    try:
        query = """
            SELECT
                account_nickname,
                MAX(last_used) AS last_used
            FROM
                shadows_account_usage
            WHERE
                account_nickname = ANY(:nicknames)
            GROUP BY
                account_nickname;
            """
        pg_results = await get_pg_buylist_readonly_database().fetch_all(
            query, values={"nicknames": account_nicknames}
        )
        return [dict(row) for row in pg_results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_forward_events_counts(account_emails: list[str]):
    emails = sorted({e.strip().lower() for e in account_emails if e and e.strip()})
    query = """
        WITH emails AS (
            SELECT
                LOWER(value::string) AS email
            FROM
                TABLE(FLATTEN(INPUT => PARSE_JSON(%s)))
        )
        SELECT
            e_in.email,
            COUNT(DISTINCT ev.event_id) AS forward_events
        FROM
            emails e_in
            JOIN inventory_management.vendor v ON LOWER(v.email) = e_in.email
            JOIN inventory_management.purchase_order po ON po.vendor_id = v.vendor_id
            JOIN inventory_management.ticket tk ON tk.purchase_order_id = po.purchase_order_id
            JOIN inventory_management.ticket_ticketgroup ttg ON ttg.ticket_id = tk.ticket_id
            JOIN inventory_management.ticketgroup tg ON tg.ticketgroup_id = ttg.ticketgroup_id
            JOIN inventory_management.event ev ON ev.event_id = tg.primary_event_id
        WHERE
            ev.start_date > CURRENT_DATE
            AND po.status <> 'REMOVED'
            AND tk.is_deleted = FALSE
            AND tg.status <> 'VOIDED'
        GROUP BY
            e_in.email
        ORDER BY
            forward_events DESC;
        """
    rows = await _run_snowflake_async(query, (json.dumps(emails),))
    return [dict(row) for row in rows]


def _distance_from_latlong(lat_long: str | None, lat_lng: str | None) -> float | None:
    if lat_long and lat_lng:
        return haversine_distance(
            tuple(map(float, lat_long.split(","))),
            tuple(map(float, lat_lng.split(","))),
        )
    return None


def _proximity(state_abbreviation: str, event_state: str, nearby_states: Iterable[str]) -> int:
    if state_abbreviation == event_state:
        return 1
    if nearby_states and state_abbreviation in nearby_states:
        return 2
    return 3


def _format_accounts_for_suggestions(
    rows,
    *,
    purchasers: dict,
    purchaser_key: str,
    event_state: str,
    nearby_states: list[str],
    lat_lng: str | None,
    cooloff_accounts: set[str] | None = None,
):
    formatted_rows = []
    purchaser_full_names = []

    for row in rows:
        purchased = purchasers.get(row[purchaser_key], 0)
        if purchased > 0:
            purchaser_full_names.append(row["name"])

        formatted = {
            "id": row["id"],
            "status_code": row["status_code"],
            "nickname": row["nickname"],
            "name": row["name"] or "Unknown Name",
            "email": row["email"] or "Unknown Email",
            "location": {
                "city": row["metro_area_name"] or "Unknown City",
                "state": row["state_abbreviation"],
            },
            "lastPurchaseDate": None,
            "hasPurchasedEvent": purchased,
            "proximity": _proximity(row["state_abbreviation"], event_state, nearby_states),
            "distance": _distance_from_latlong(row["lat_long"], lat_lng),
        }

        if cooloff_accounts is not None:
            formatted["cooloff"] = row["nickname"] in cooloff_accounts

        formatted_rows.append(formatted)

    return formatted_rows, purchaser_full_names


async def _attach_usage_metadata(
    formatted_rows: list[dict],
    *,
    purchaser_full_names: list[str],
    company_group: str,
):
    just_emails = [
        row["email"] for row in formatted_rows if row["email"] and row["email"] != "Unknown Email"
    ]
    just_accts = [
        row["nickname"]
        for row in formatted_rows
        if row["nickname"] and row["nickname"] != "Unknown Nickname"
    ]

    if company_group == "shadows":
        last_used = await get_shadows_last_used(just_accts)
        last_u = {item["account_nickname"]: item["last_used"] for item in last_used}
    elif company_group == "asset":
        last_used = await get_asset_last_purchase_date(just_emails)
        email_to_date = {item["EMAIL"]: item["LAST_PO_DATE"] for item in last_used}
        last_u = {row["nickname"]: email_to_date.get(row["email"]) for row in formatted_rows}
    else:
        last_u = {}

    forward_events = await get_forward_events_counts(just_emails)
    f_events = {item["EMAIL"]: item["FORWARD_EVENTS"] for item in forward_events}

    for row in formatted_rows:
        row["lastPurchaseDate"] = last_u.get(row["nickname"])
        row["namePurchasedEvent"] = row["name"] in purchaser_full_names
        row["forwardEvents"] = f_events.get(row["email"], "Data Unavailable")


def _sort_and_filter_shadows(formatted_rows: list[dict], ticket_limit: TicketLimitResult | None = None):
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)

    formatted_rows.sort(
        key=lambda x: (
            1 if ticket_limit is not None and x["hasPurchasedEvent"] >= ticket_limit["limit_value"] else 0,
            x.get("cooloff", False),
            x["namePurchasedEvent"],
            x["hasPurchasedEvent"],
            not (x["lastPurchaseDate"] and x["lastPurchaseDate"] > seven_days_ago),
            x["distance"] if x["distance"] is not None else float("inf"),
            x["proximity"],
            x["forwardEvents"] if isinstance(x["forwardEvents"], int) else -1,
            (
                random.random()
                if x["lastPurchaseDate"] and x["lastPurchaseDate"] > seven_days_ago
                else 0
            ),
            x["lastPurchaseDate"] or datetime(1, 1, 1, 0, 0, tzinfo=timezone.utc),
            x["location"]["state"],
            x["location"]["city"],
        )
    )

    filtered_rows = [
        row
        for row in formatted_rows
        if row["lastPurchaseDate"]
        and row["nickname"] != "ZONE TEVO"
        and row.get("status_code") == "ACTIVE"
    ]

    for idx, row in enumerate(filtered_rows, start=1):
        row["rank"] = idx
        if ticket_limit is not None:
            row["ticketLimit"] = ticket_limit["limit_value"]

    return filtered_rows


def _sort_and_filter_asset(formatted_rows: list[dict], limit: int | None = None):
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)

    formatted_rows.sort(
        key=lambda x: (
            x["namePurchasedEvent"],
            x["hasPurchasedEvent"],
            not (x["lastPurchaseDate"] and x["lastPurchaseDate"] > seven_days_ago),
            x["distance"] if x["distance"] is not None else float("inf"),
            x["proximity"],
            x["forwardEvents"] if isinstance(x["forwardEvents"], int) else -1,
            (
                random.random()
                if x["lastPurchaseDate"] and x["lastPurchaseDate"] > seven_days_ago
                else 0
            ),
            x["lastPurchaseDate"] or datetime(1, 1, 1, 0, 0, tzinfo=timezone.utc),
            x["location"]["state"],
            x["location"]["city"],
        )
    )

    filtered_rows = [
        row
        for row in formatted_rows
        if row["status_code"] == "ACTIVE"
        and row["lastPurchaseDate"] and row["nickname"] != "ZONE TEVO"
    ]

    for idx, row in enumerate(filtered_rows, start=1):
        row["rank"] = idx

    if limit is not None and limit > 0:
        filtered_rows = [
            row
            for row in filtered_rows
            if not row["namePurchasedEvent"] and not row["hasPurchasedEvent"]
        ][:limit]

    return filtered_rows


async def get_ams_accounts(company_id: str, pos: str | None = None):
    try:
        sql = """
            SELECT
                  a.id
                , a.nickname
                , a.status_code
                , e.email_address AS email
                , p.full_name AS name
                , s.abbreviation AS state_abbreviation
                , ma.name AS metro_area_name
                , addr.lat_long AS lat_long
            FROM
                ams.ams_account a
            LEFT JOIN ams.ams_address addr ON
                a.ams_address_id = addr.id
            LEFT JOIN ams.state s ON
                addr.state_id = s.id
            LEFT JOIN ams.metro_area ma ON
                addr.metro_area_id = ma.id
            LEFT JOIN ams.ams_person p ON
                a.ams_person_id = p.id
            LEFT JOIN ams.ams_email e ON
                a.ams_email_id = e.id
            LEFT JOIN ams.account_point_of_sale_mapping aposm ON
                aposm.account_id = a.id
            LEFT JOIN ams.point_of_sale pos ON
                pos.id = aposm.point_of_sale_id
            WHERE
                a.company_id = :company_id
                AND (
                    :pos ::text IS NULL
                        OR pos.name = :pos ::text
                );
            """
        rows = await get_pg_readonly_database().fetch_all(
            sql,
            values={"company_id": company_id, "pos": pos},
        )
        return rows
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shadows_account_suggestions(
    event_state: str,
    nearby_states: list[str],
    company_id: str,
    event_id: str,
    pos: str | None = None,
    lat_lng: str | None = None,
    ticket_limit: TicketLimitResult | None = None,
):
    try:
        usage = await get_shadows_event_usage(event_id)
        purchasers = {item["account_nickname"]: item["ticket_count"] for item in usage}
        cooloff_accounts = set(await fetch_cooloff_accounts())
        rows = await get_ams_accounts(company_id, pos)

        formatted_rows, purchaser_full_names = _format_accounts_for_suggestions(
            rows,
            purchasers=purchasers,
            purchaser_key="nickname",
            event_state=event_state,
            nearby_states=nearby_states,
            lat_lng=lat_lng,
            cooloff_accounts=cooloff_accounts,
        )

        await _attach_usage_metadata(
            formatted_rows,
            purchaser_full_names=purchaser_full_names,
            company_group="shadows",
        )

        filtered_rows = _sort_and_filter_shadows(formatted_rows, ticket_limit=ticket_limit)

        return filtered_rows
    except Exception as e:
        print(e)
        raise


async def get_asset_account_suggestions(
    event_state: str,
    nearby_states: list[str],
    company_id: str,
    event_id: str,
    pos: str | None = None,
    lat_lng: str | None = None,
):
    try:
        purchasers_list = await get_asset_event_purchasers(event_id)
        purchasers = {
            item["EMAIL"]: item["QUANTITY"]
            for item in purchasers_list
            if item.get("EMAIL")
        }
        rows = await get_ams_accounts(company_id, pos)

        formatted_rows, purchaser_full_names = _format_accounts_for_suggestions(
            rows,
            purchasers=purchasers,
            purchaser_key="email",
            event_state=event_state,
            nearby_states=nearby_states,
            lat_lng=lat_lng,
        )

        await _attach_usage_metadata(
            formatted_rows,
            purchaser_full_names=purchaser_full_names,
            company_group="asset",
        )

        filtered_rows = _sort_and_filter_asset(formatted_rows)

        return filtered_rows
    except Exception as e:
        print(e)
        raise


async def check_for_shadows_suggestions(item_ids: list[str]) -> list[str]:
    if not item_ids:
        return []

    # Create placeholders for each ID
    placeholders = ", ".join(f":id{i}" for i in range(len(item_ids)))

    query = f"""
        WITH input_ids AS (
            SELECT unnest(ARRAY[{placeholders}]) AS id
        )
        SELECT i.id
        FROM input_ids i
        LEFT JOIN shadows_account_suggestion s ON s.id = i.id
        WHERE s.id IS NULL;
        """

    values = {f"id{i}": item_id for i, item_id in enumerate(item_ids)}
    db = get_pg_buylist_database()

    try:
        try:
            pg_results = await db.fetch_all(query, values=values)
        except AssertionError as e:
            if "Connection is already acquired" in str(e):
                logging.warning("Buylist DB connection was stuck; reconnecting and retrying")
                await db.disconnect()
                await db.connect()
                pg_results = await db.fetch_all(query, values=values)
            else:
                raise

        new_ids = [row["id"] for row in pg_results]
        return new_ids
    except anyio.get_cancelled_exc_class() as cancel_exc:
        logging.warning("check_for_shadows_suggestions cancelled; resetting buylist DB connection")
        if db.is_connected:
            await db.disconnect()
        await db.connect()
        raise cancel_exc
    except Exception as e:
        raise Exception(f"Error checking for shadows suggestions: {e}")


# TODO: HOTFIX for missing event codes, remove once MX event linking is fixed.
async def _insert_empty_suggestion(item_id: str):
    insert_query = """
        INSERT INTO shadows_account_suggestion (id, suggested_accounts, created_at, suggestions)
        VALUES (:id, ARRAY[]::text[], CURRENT_TIMESTAMP, '[]'::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET
            suggested_accounts = EXCLUDED.suggested_accounts,
            suggestions = EXCLUDED.suggestions,
            created_at = CURRENT_TIMESTAMP;
    """
    try:
        await get_pg_buylist_database().execute(
            insert_query,
            values={"id": item_id},
        )
    except Exception as e:
        print(f"Failed to insert empty suggestion for {item_id}: {e}")
        raise Exception(f"Error inserting empty suggestion for {item_id}: {e}")

# TODO: HOTFIX for missing event codes, remove once MX and INTL event codes are fixed.
def _is_invalid_event_code(event_code: str | None) -> bool:
    """Treat missing, blank, or '?'-containing event codes as invalid."""
    return (
        event_code is None
        or event_code.strip() == ""
        or "?" in event_code
    )


async def add_shadows_account_suggestions(suggestions: list[UnsuggestedEvent]):
    environment = os.getenv("ENVIRONMENT", "dev")

    if not suggestions:
        return

    sugg_to_add = []
    for suggestion in suggestions:
        if environment == "prod":
            if _is_invalid_event_code(suggestion["event_code"]):
                print(f"Skipping suggestion with missing event_code: {suggestion['id']}")
                await _insert_empty_suggestion(suggestion["id"])
                continue
            else:
                event_details = await get_event_details(suggestion["event_code"])
                if event_details:
                    print(f"Fetching account suggestions for event: {event_details['eventName']} ({suggestion['event_code']})")
                    nearby = nearby_states(event_details["venueState"])
                    company_id = (
                        "4d187c5e-b74d-456a-a690-a68f3014c548"  # Shadows company ID
                        if suggestion["currency_code"].upper() in ["USD", "CAD"]
                        else "2cb42500-171d-4e3f-8e9f-81921dc9e801"  # Shadows Intl company ID
                    )
                    suggest = await get_shadows_account_suggestions(
                        event_state=event_details["venueState"],
                        nearby_states=nearby,
                        company_id=company_id,
                        event_id=event_details["id"],
                        pos=suggestion.get("pos"),
                        lat_lng=(
                            f"{event_details['lat']},{event_details['lng']}"
                            if event_details["lat"] and event_details["lng"]
                            else None
                        ),
                        ticket_limit=suggestion["ticket_limit"],
                    )

                    now = datetime.now(timezone.utc)
                    seven_days_ago = now - timedelta(days=7)

                    current = [
                        row
                        for row in suggest
                        if row["lastPurchaseDate"]
                        and row["lastPurchaseDate"] > seven_days_ago
                    ]

                    sugg_to_add.append(
                        {
                            "id": suggestion["id"],
                            "suggested_accounts": [cs["nickname"] for cs in current][:10],
                            "suggestions": current,
                        }
                    )
        else:
            print(
                f"Skipping account suggestion fetch in non-prod environment: {environment}"
            )
            suggest = [{"id": suggestion["id"], "nickname": "DXTEST"}]

            sugg_to_add.append(
                {
                    "id": suggestion["id"],
                    "suggested_accounts": ["DXTEST"],
                    "suggestions": suggest,
                }
            )

    insert_query = """
        INSERT INTO shadows_account_suggestion (id, suggested_accounts, created_at, suggestions)
        VALUES (:id, :suggested_accounts, CURRENT_TIMESTAMP, CAST(:suggestions AS JSONB))
        ON CONFLICT (id) DO UPDATE
        SET
            suggested_accounts = EXCLUDED.suggested_accounts,
            suggestions = EXCLUDED.suggestions,
            created_at = CURRENT_TIMESTAMP;
    """
    try:
        async with get_pg_buylist_database().transaction():
            for sugg in sugg_to_add:
                sugg_json = json.dumps(sugg["suggestions"], default=postgres_json_serializer)
                await get_pg_buylist_database().execute(
                    insert_query,
                    values={
                        "id": str(sugg["id"]),
                        "suggested_accounts": sugg["suggested_accounts"],
                        "suggestions": sugg_json,
                    },
                )
    except Exception as e:
        print(f"Failed to add suggestions: {e}")
        raise Exception(f"Error adding account suggestions: {e}")


async def fetch_suggestions_by_ids(item_ids: list[str]) -> list[dict]:
    """
    Fetch stored account suggestions for the given buylist item IDs.
    Returns an empty list if none found or input empty.
    """
    if not item_ids:
        return []

    # Dynamically expand placeholders safely
    placeholders = ", ".join(f":id{i}" for i in range(len(item_ids)))
    query = f"""
        SELECT s.id, s.suggested_accounts, s.created_at, s.suggestions
        FROM shadows_account_suggestion s
        WHERE s.id IN ({placeholders});
    """

    values = {f"id{i}": item_id for i, item_id in enumerate(item_ids)}

    try:
        rows = await get_pg_buylist_database().fetch_all(query, values=values)
        items = []
        for row in rows:
            items.append(
                {
                    "id": row["id"],
                    "suggested_accounts": row["suggested_accounts"],
                    "created_at": row["created_at"],
                    "suggestions": json.loads(row["suggestions"]),
                }
            )
        return items
    except Exception as e:
        print(f"Failed to fetch suggestions by id: {e}")
        raise Exception(f"Error fetching suggestions by id: {e}")


async def get_unbought_buylist_items():
    try:
        sql = """
            SELECT
                id,
                account_id,
                event_code,
                currency_code,
                performer_id,
                venue_id
            FROM
                shadows_buylist
            WHERE
                buylist_order_status = 'Unbought'
                and created_at >= CURRENT_DATE
                and created_at < (CURRENT_DATE + interval '1 day')::timestamp
            ORDER BY
                created_at;
            """
        db = get_pg_buylist_readonly_database()
        try:
            pg_results = await db.fetch_all(query=sql)
        except AssertionError as e:
            # Work around occasional stuck connections by reconnecting the pool
            if "Connection is already acquired" in str(e):
                logging.warning("Buylist readonly DB connection was stuck; reconnecting and retrying")
                await db.disconnect()
                await db.connect()
                pg_results = await db.fetch_all(query=sql)
            else:
                raise

        return [dict(result) for result in pg_results]
    except anyio.get_cancelled_exc_class() as cancel_exc:
        logging.warning("get_unbought_buylist_items cancelled; resetting buylist readonly DB connection")
        db = get_pg_buylist_readonly_database()
        if db.is_connected:
            await db.disconnect()
        await db.connect()
        raise cancel_exc
    except Exception as e:
        raise Exception(
            f"An error occurred while getting unbought buylist items: {str(e)}"
        ) from e


async def get_unbought_ticket_limits(
    ticket_limit_data: list[TicketLimitData],
) -> list[TicketLimitResult]:
    """
    Gets the user-reported ticket limits for unbought buylist items.

    Currently only supports event-specific limits.

    TODO: Expand to support show runs and multi-day events. TBE-3842
    """
    try:
        event_code_to_ids: dict[str, list[str]] = {}

        for item in ticket_limit_data:
            event_code = item.get("event_code")

            if event_code:
                event_code_to_ids.setdefault(event_code, []).append(item["id"])
        
        sql = """
            SELECT
                  stl.event_code
                , stl.venue_code
                , stl.performer_id
                , stl.limit_type
                , stl.limit_value
            FROM
                shadows_ticket_limits stl
            WHERE
                stl.event_code = ANY(:event_codes ::text[]);
            """
        db = get_pg_buylist_readonly_database()
        event_codes = list(event_code_to_ids.keys())
        try:
            pg_results = await db.fetch_all(
                query=sql,
                values={"event_codes": event_codes},
            )
        except AssertionError as e:
            # Work around occasional stuck connections by reconnecting the pool
            if "Connection is already acquired" in str(e):
                logging.warning(
                    "Buylist readonly DB connection was stuck; reconnecting and retrying"
                )
                await db.disconnect()
                await db.connect()
                pg_results = await db.fetch_all(
                    query=sql,
                    values={"event_codes": event_codes},
                )
            else:
                raise
        results: list[TicketLimitResult] = []
        for result in pg_results:
            row = dict(result)
            event_code = row.get("event_code")

            matched_ids: list[str] = []
            if event_code and event_code in event_code_to_ids:
                matched_ids.extend(event_code_to_ids[event_code])

            if not matched_ids:
                continue

            for request_id in matched_ids:
                typed_row: TicketLimitResult = {
                    "id": request_id,
                    "event_code": cast(str | None, row.get("event_code")),
                    "venue_code": cast(str, row.get("venue_code")),
                    "performer_id": cast(str, row.get("performer_id")),
                    "limit_type": cast(str, row.get("limit_type")),
                    "limit_value": cast(int, row.get("limit_value")),
                }

                results.append(typed_row)
        return results

    except anyio.get_cancelled_exc_class() as cancel_exc:
        logging.warning(
            "get_unbought_ticket_limits cancelled; resetting buylist readonly DB connection"
        )
        db = get_pg_buylist_readonly_database()
        if db.is_connected:
            await db.disconnect()
        await db.connect()
        raise cancel_exc
    except Exception as e:
        raise Exception(
            f"An error occurred while getting unbought buylist items: {str(e)}"
        ) from e


async def write_suggestion_feedback(attempt_data: dict):
    try:
        sql = """
            INSERT INTO shadows_suggestion_feedback (data)
            VALUES (CAST(:data AS JSONB))
            RETURNING
                id,
                data ->> 'suggestion' AS account_nickname,
                data ->> 'error_code' AS error_code;
            """
        res = await get_pg_buylist_database().fetch_one(
            query=sql,
            values={"data": json.dumps(attempt_data, default=postgres_json_serializer)},
        )
        if res:
            return {
                "id": res["id"],
                "account_nickname": res["account_nickname"],
                "error_code": res["error_code"],
            }
        else:
            raise Exception("Failed to write suggestion feedback.")
    except Exception as e:
        raise Exception(f"An error occurred while writing feedback: {str(e)}") from e


async def write_cooloff_record(record: dict):
    cooloff = await get_config_value("shadows_account_base_cooloff_minutes")
    default_minutes = 60 * 24
    try:
        base_minutes = int(cooloff) if cooloff is not None else default_minutes
    except (ValueError, TypeError):
        logging.warning(
            "Invalid shadows_account_base_cooloff_minutes value '%s', using default %s",
            cooloff,
            default_minutes,
        )
        base_minutes = default_minutes

    values = {
        "account_nickname": record["account_nickname"],
        "reason": record["error_code"],
        "cooloff_minutes": base_minutes,
    }

    print(f"Writing cooloff record: {values}")

    try:
        sql = """
            INSERT INTO public.shadows_account_cooloff (
                account_nickname,
                reason,
                cooloff_count,
                created_at,
                expires_at
            )
            VALUES (
                :account_nickname,
                :reason,
                1,
                NOW(),
                NOW() + make_interval(mins => :cooloff_minutes) * 1
            )
            ON CONFLICT (account_nickname)
            DO UPDATE
            SET
                cooloff_count = public.shadows_account_cooloff.cooloff_count + 1,
                created_at    = NOW(),
                expires_at    = NOW()
                    + make_interval(mins => :cooloff_minutes)
                      * (public.shadows_account_cooloff.cooloff_count + 1),  -- offense N = base interval * N
                reason        = EXCLUDED.reason;
            """
        await get_pg_buylist_database().execute(
            query=sql,
            values=values,
        )
    except Exception as e:
        raise Exception(
            f"An error occurred while writing cooloff record: {str(e)}"
        ) from e


async def fetch_cooloff_accounts() -> list[str]:
    try:
        sql = """
            SELECT
                sac.account_nickname
            FROM
                shadows_account_cooloff sac
            WHERE
                sac.expires_at > CURRENT_TIMESTAMP;
            """
        rows = await get_pg_buylist_readonly_database().fetch_all(query=sql)
        accounts = [row["account_nickname"] for row in rows]
        return accounts
    except Exception as e:
        raise Exception(
            f"An error occurred while fetching cooloff accounts: {str(e)}"
        ) from e


async def get_accounts_to_refetch(cooloff_accounts: list[str]):
    query = """
        SELECT DISTINCT
              sas.id
            , sb.event_code
            , sb.currency_code
            , sb.account_id
            , sb.performer_id
            , sb.venue_id
        FROM shadows_account_suggestion sas
        JOIN shadows_buylist sb
            ON sb.id = sas.id
        CROSS JOIN LATERAL jsonb_array_elements(sas.suggestions)
            WITH ORDINALITY AS elem(account, ord)
        WHERE ord <= 30
          AND (elem.account ->> 'nickname') = ANY(:cooloff_accounts ::text[])
          AND sb.buylist_order_status = 'Unbought'
          AND (
                coalesce(sb.card, '') = ''
                AND  coalesce(sb.confirmation_number, '') = ''
              )
          AND sb.created_at >= (CURRENT_DATE - INTERVAL '1 day')
          AND sb.created_at <  (CURRENT_DATE + INTERVAL '1 day');
        """
    try:
        rows = await get_pg_buylist_database().fetch_all(
            query, values={"cooloff_accounts": cooloff_accounts}
        )
        return rows
    except Exception as e:
        logging.exception(f"Failed to fetch suggestions for refetch: {e}")
        raise


async def fetch_suggestion_feedback(timeframe: datetime):
    try:
        sql = """
            select *
            FROM shadows_suggestion_feedback
            WHERE (:timeframe ::timestamp IS NULL OR created_at > :timeframe ::timestamp)
            ORDER BY created_at DESC;
            """
        rows = await get_pg_buylist_database().fetch_all(
            query=sql, values={"timeframe": timeframe}
        )
        items = []
        for row in rows:
            items.append(
                {
                    "id": row["id"],
                    "data": json.loads(row["data"]),
                    "created_at": row["created_at"],
                }
            )
        return items
    except Exception as e:
        print(f"Failed to fetch suggestion feedback: {e}")
        raise Exception(f"An error occurred while fetching feedback: {str(e)}") from e


async def fetch_active_account_nicknames(company_id: list[str] | None) -> list[str]:
    try:
        sql = """
            SELECT nickname
            FROM ams.ams_account
            WHERE status_code = 'ACTIVE'
            AND (:company_id ::text[] IS NULL OR company_id = ANY(:company_id ::text[]));
            """
        rows = await get_pg_readonly_database().fetch_all(
            query=sql, values={"company_id": company_id}
        )
        nicknames = [row["nickname"] for row in rows if row["nickname"]]
        return nicknames
    except Exception as e:
        raise Exception(
            f"An error occurred while fetching active account nicknames: {str(e)}"
        ) from e
