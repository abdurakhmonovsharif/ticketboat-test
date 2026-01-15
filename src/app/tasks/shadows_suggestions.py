import logging
import anyio

from app.db.account_suggestion_db import (
    UnsuggestedEvent,
    add_shadows_account_suggestions,
    check_for_shadows_suggestions,
    fetch_cooloff_accounts,
    get_accounts_to_refetch,
    get_unbought_buylist_items,
    get_unbought_ticket_limits,
)
from app.db.app_config_db import get_config_value


async def shadows_suggestions_task(stop: anyio.Event, interval: float = 5.0):
    try:
        while not stop.is_set():
            unbought_today = await get_unbought_buylist_items()
            # TODO: Implmeent show run ticket limit logic TBE-3842
            unbought_ticket_limits = await get_unbought_ticket_limits(
                [
                    {
                        "id": e["id"],
                        "performer_id": e["performer_id"],
                        "venue_id": e["venue_id"],
                        "event_code": e["event_code"],
                    }
                    for e in unbought_today
                ]
            )
            default_ticket_limit = int(
                await get_config_value("default_ticket_limit") or 4
            )
            ticket_limit_by_id = {
                limit["id"]: limit for limit in unbought_ticket_limits
            }

            new = await check_for_shadows_suggestions([e["id"] for e in unbought_today])
            if new:
                logging.info(
                    f"Fetching shadows suggestions for {len(new)} new buylist items: {', '.join(new)}"
                )
                unsuggested: list[UnsuggestedEvent] = [
                    {
                        "id": e["id"],
                        "event_code": e["event_code"],
                        "pos": (
                            "StubHub"  # SHPOS
                            if e["account_id"] == "internationalshadows24@gmail.com"
                            else "SkyBox"  # SKYBOX
                        ),
                        "currency_code": e["currency_code"],
                        "ticket_limit": ticket_limit_by_id.get(
                            e["id"],
                            {
                                "id": e["id"],
                                "event_code": e["event_code"],
                                "venue_code": e["venue_id"],
                                "performer_id": e["performer_id"],
                                "limit_type": "show",
                                "limit_value": default_ticket_limit,
                            },
                        ),
                    }
                    for e in unbought_today
                    if e["id"] in new
                ]
                await add_shadows_account_suggestions(unsuggested)

            # Wait until either stop is set or interval passes
            with anyio.move_on_after(interval):
                await stop.wait()
    except anyio.get_cancelled_exc_class():
        raise


async def refetch_shadows_suggestions_task():
    cooloff_accounts = await fetch_cooloff_accounts()
    if not cooloff_accounts:
        logging.info("No cooloff accounts, skipping shadows suggestions refetch")
        return

    rows = await get_accounts_to_refetch(cooloff_accounts)

    unbought_ticket_limits = await get_unbought_ticket_limits(
        [
            {
                "id": e["id"],
                "performer_id": e["performer_id"],
                "venue_id": e["venue_id"],
                "event_code": e["event_code"],
            }
            for e in rows
        ]
    )
    default_ticket_limit = int(await get_config_value("default_ticket_limit") or 4)
    ticket_limit_by_id = {limit["id"]: limit for limit in unbought_ticket_limits}
    if not rows:
        logging.info("No shadows suggestions to refetch")
        return

    to_refresh: list[UnsuggestedEvent] = [
        {
            "id": row["id"],
            "event_code": row["event_code"],
            "pos": (
                "StubHub"
                if row["account_id"] == "internationalshadows24@gmail.com"
                else "SkyBox"
            ),
            "currency_code": row["currency_code"],
            "ticket_limit": ticket_limit_by_id.get(
                row["id"],
                {
                    "id": row["id"],
                    "event_code": row["event_code"],
                    "venue_code": row["venue_id"],
                    "performer_id": row["performer_id"],
                    "limit_type": "show",
                    "limit_value": default_ticket_limit,
                },
            ),
        }
        for row in rows
    ]

    logging.info(
        f"Refetching shadows suggestions for {len(to_refresh)} buylist items: {', '.join([r['id'] for r in to_refresh])}"
    )
    await add_shadows_account_suggestions(to_refresh)
