import json
import re
import traceback
from datetime import date, datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from uuid import uuid4

from fastapi import HTTPException

from app.database import get_pg_buylist_database, get_pg_readonly_database, get_pg_buylist_readonly_database
from app.model.buylist import (
    BuyListItemSerializer,
    UpdateBuyListItemRequest,
    SaveErrorReportRequest,
    UnclaimSalesResponse,
    SaleHistoryModel
)
from app.model.user import User

VALID_SORT_COLUMNS = [
    "id", "account_id", "exchange", "transaction_date", "event_name",
    "event_date", "section", "row", "quantity", "venue", "venue_city",
    "buylist_status", "link", "subs", "viagogo_order_status", "card",
    "amount", "confirmation_number", "buyer", "delivery_method",
    "discount", "was_offer_extended", "nih", "mismapped", "was_discount_code_used",
    "date_last_checked", "date_tickets_available", "bar_code", "notes",
    "buylist_order_status", "escalated_to", "listing_notes",
    "sales_source", "created_at", "nocharge_price",
    "purchase_confirmation_created_at", "order_claimed_created_at",
    "buyer_email", "currency_code"
]


def _build_optimized_search_filter(search_term: str) -> tuple[str, dict]:
    """Build optimized search filter that leverages GIN indexes."""
    if not search_term:
        return "1=1", {}

    cleaned_term = search_term.strip()
    params = {}

    if re.match(r'\d{1,4}-\d{3,8}/[A-Z0-9]{2,5}', cleaned_term.upper()):
        params["search_confirmation"] = cleaned_term.upper()
        return "sb.confirmation_number = :search_confirmation", params

    if (('.' in cleaned_term or ',' in cleaned_term) and
        cleaned_term.replace('.', '').replace(',', '').isdigit()):
        try:
            numeric_value = float(cleaned_term.replace(',', ''))
            params["search_amount"] = numeric_value
            return "sb.amount = :search_amount", params
        except ValueError:
            pass

    params["search_pattern"] = f"%{cleaned_term}%"
    params["search_term"] = cleaned_term

    search_conditions = []

    if cleaned_term.isdigit():
        search_conditions.extend([
            "sb.id = :search_term",
            "CAST(sb.id AS TEXT) ILIKE :search_pattern"
        ])
    else:
        search_conditions.append("CAST(sb.id AS TEXT) ILIKE :search_pattern")

    search_conditions.extend([
        "sb.exchange = :search_term",
        "sb.currency_code = :search_term"
    ])

    search_conditions.append("""
        (sb.event_name || ' ' ||
         COALESCE(sb.venue, '') || ' ' ||
         COALESCE(sb.exchange, '') || ' ' ||
         COALESCE(sb.venue_city, '') || ' ' ||
         COALESCE(sb.section, '') || ' ' ||
         COALESCE(sb."row", '') || ' ' ||
         COALESCE(sb.currency_code, '') || ' ' ||
         COALESCE(sb.sales_source, '') || ' ' ||
         COALESCE(sb.confirmation_number, '') || ' ' ||
         COALESCE(sb.card, '')) ILIKE :search_pattern
    """)

    return f"({' OR '.join(search_conditions)})", params

def validate_sort_params(sort_by: str, sort_order: str) -> None:
    """Validate sorting parameters."""
    if sort_by not in VALID_SORT_COLUMNS:
        raise HTTPException(status_code=400, detail="Invalid sort_by value.")
    if sort_order.lower() not in ["asc", "desc"]:
        raise HTTPException(status_code=400, detail="Invalid sort_order value.")


def build_common_filters(
        is_escalated: Optional[bool] = None,
        account_ids: Optional[List[str]] = None,
        exchange_marketplaces: Optional[List[str]] = None,
        event_name: Optional[str] = None,
        buylist_status: Optional[List[str]] = None,
        venue_city: Optional[str] = None,
        buyers: Optional[List[str]] = None,
        subs: Optional[bool] = None,
        nih: Optional[bool] = None,
        escalated: Optional[bool] = None,
        mismapped: Optional[bool] = None,
        event_start_date: Optional[date] = None,
        event_end_date: Optional[date] = None,
        transaction_start_date: Optional[date] = None,
        transaction_end_date: Optional[date] = None,
        search_term: Optional[str] = None,
        timezone: Optional[str] = 'America/Chicago',
        currency_code: Optional[List[str]] = None,
        is_hardstock: Optional[bool] = None
) -> tuple[list, dict]:
    """Build common query filters and parameters."""
    query_filters = []
    params = {}

    if is_escalated:
        query_filters.append("sb.escalated_to IN ('Tier 2', 'Tier 3', 'Tier 4')")
    if account_ids:
        account_placeholders = ", ".join([f":account_{i}" for i in range(len(account_ids))])
        query_filters.append(f"sb.account_id IN ({account_placeholders})")
        for i, account_id in enumerate(account_ids):
            params[f"account_{i}"] = account_id
    if exchange_marketplaces:
        exchange_placeholders = ", ".join([f":exchange_{i}" for i in range(len(exchange_marketplaces))])
        query_filters.append(f"sb.exchange IN ({exchange_placeholders})")
        for i, exchange in enumerate(exchange_marketplaces):
            params[f"exchange_{i}"] = exchange
    if event_name:
        query_filters.append("sb.event_name ILIKE :event_name")
        params["event_name"] = f"%{event_name}%"
    if buylist_status:
        status_placeholders = ", ".join([f":status_{i}" for i in range(len(buylist_status))])
        query_filters.append(f"sb.buylist_order_status IN ({status_placeholders})")
        for i, status in enumerate(buylist_status):
            params[f"status_{i}"] = status
    if venue_city:
        query_filters.append("sb.venue_city ILIKE :venue_city")
        params["venue_city"] = f"%{venue_city}%"
    if buyers:
        buyer_placeholders = ", ".join([f":buyer_{i}" for i in range(len(buyers))])
        query_filters.append(f"sb.buyer IN ({buyer_placeholders})")
        for i, buyer in enumerate(buyers):
            params[f"buyer_{i}"] = buyer
    if subs is not None:
        query_filters.append("sb.subs = :subs")
        params["subs"] = int(subs)
    if nih is not None:
        query_filters.append("sb.nih = :nih")
        params["nih"] = int(nih)
    if escalated:
        query_filters.append("sb.escalated_to IN ('Tier 2', 'Tier 3', 'Tier 4')")
    if mismapped is not None:
        query_filters.append("sb.mismapped = :mismapped")
        params["mismapped"] = int(mismapped)
    if transaction_start_date:
        query_filters.append(
            f"CASE WHEN vgs.created_at IS NOT NULL THEN "
            f"vgs.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}' "
            f"ELSE sb.transaction_date END >= "
            f"TO_TIMESTAMP(:transaction_start_date || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS')")
        params["transaction_start_date"] = transaction_start_date.strftime('%Y-%m-%d')

    if transaction_end_date:
        query_filters.append(
            f"CASE WHEN vgs.created_at IS NOT NULL THEN "
            f"vgs.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}' "
            f"ELSE sb.transaction_date END <= "
            f"TO_TIMESTAMP(:transaction_end_date || ' 23:59:59.999', 'YYYY-MM-DD HH24:MI:SS')")
        params["transaction_end_date"] = transaction_end_date.strftime('%Y-%m-%d')
    if event_start_date:
        query_filters.append("sb.event_date >= :event_start_date")
        params["event_start_date"] = event_start_date
    if event_end_date:
        query_filters.append("sb.event_date <= :event_end_date")
        params["event_end_date"] = event_end_date + timedelta(days=1)
    if search_term:
        search_filter, search_params = _build_optimized_search_filter(search_term)
        query_filters.append(search_filter)
        params.update(search_params)
    if currency_code:
        currency_placeholders = ", ".join([f":currency_{i}" for i in range(len(currency_code))])
        query_filters.append(f"sb.currency_code IN ({currency_placeholders})")
        for i, currency in enumerate(currency_code):
            params[f"currency_{i}"] = currency

    if is_hardstock is True:
        query_filters.append("sb.is_hardstock IS TRUE")

    return query_filters, params


async def get_buylist_count(query_filter_str: str, params: dict) -> int:
    """Get total count of buylist items matching filters."""
    count_query = f"""
        SELECT COUNT(*) FROM shadows_buylist sb LEFT JOIN viagogo_sales vgs ON sb.id = vgs.id WHERE {query_filter_str}
    """
    db = get_pg_buylist_readonly_database()
    async with db.transaction():
        await db.execute("SET LOCAL work_mem = '256MB'")
        total_count = await db.fetch_one(count_query, params)
    return total_count[0] if total_count else 0


async def get_buylist_items(
        page_size: int,
        page: int,
        sort_by: str,
        sort_order: str,
        timezone: Optional[str] = "America/Chicago",
        is_escalated: Optional[bool] = None,
        account_ids: Optional[List[str]] = None,
        exchange_marketplaces: Optional[List[str]] = None,
        event_name: Optional[str] = None,
        buylist_status: Optional[List[str]] = None,
        venue_city: Optional[str] = None,
        buyers: Optional[List[str]] = None,
        subs: Optional[bool] = None,
        nih: Optional[bool] = None,
        escalated: Optional[bool] = None,
        mismapped: Optional[bool] = None,
        event_start_date: Optional[date] = None,
        event_end_date: Optional[date] = None,
        transaction_start_date: Optional[date] = None,
        transaction_end_date: Optional[date] = None,
        search_term: Optional[str] = None,
        currency_code: Optional[List[str]] = None,
        is_hardstock: Optional[bool] = None
) -> Dict[str, Any]:
    """Get paginated buylist items with filters and sorting."""
    try:
        validate_sort_params(sort_by, sort_order)
        offset = (page - 1) * page_size

        query_filters, params = build_common_filters(
            is_escalated=is_escalated,
            account_ids=account_ids,
            exchange_marketplaces=exchange_marketplaces,
            event_name=event_name,
            buylist_status=buylist_status,
            venue_city=venue_city,
            buyers=buyers,
            subs=subs,
            nih=nih,
            escalated=escalated,
            mismapped=mismapped,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
            transaction_start_date=transaction_start_date,
            transaction_end_date=transaction_end_date,
            search_term=search_term,
            timezone=timezone,
            currency_code=currency_code,
            is_hardstock=is_hardstock
        )

        query_filter_str = " AND ".join(query_filters) if query_filters else "1=1"

        pg_query = f"""
            SELECT 
                sb.id,
                sb.event_state,
                sb.account_id,
                sb.exchange,
                sb.event_name,
                sb.event_date,
                sb.section,
                sb."row",
                sb.orig_section,
                sb.orig_row,
                sb.quantity,
                sb.venue,
                sb.venue_city,
                sb.buylist_status,
                sb.link,
                sb.subs,
                sb.viagogo_order_status,
                sb.card,
                sb.amount,
                sb.confirmation_number,
                sb.buyer,
                sb.delivery_method,
                sb.discount,
                sb.was_offer_extended,
                sb.nih,
                sb.mismapped,
                sb.was_discount_code_used,
                sb.date_last_checked,
                sb.date_tickets_available,
                sb.bar_code,
                sb.notes,
                sb.buylist_order_status,
                sb.escalated_to,
                sb.listing_notes,
                sb.sales_source,
                sb.created_at,
                sb.nocharge_price,
                sb.event_code,
                sb.performer_id,
                sb.venue_id,
                to_char(
                    (sb.purchase_confirmation_created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as purchase_confirmation_created_at,
                to_char(
                    CASE
                        WHEN vgs.created_at IS NOT NULL THEN
                            vgs.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'
                        ELSE
                            sb.transaction_date
                    END,
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) AS transaction_date,
                to_char(
                    (sb.order_claimed_created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as order_claimed_created_at,
                sb.buyer_email,
                sb.currency_code,
                to_char(
                    (sb.grabbed_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as grabbed_at,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = sb.event_code AND sb.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = sb.performer_id AND sb.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = sb.venue_id AND sb.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount,
                sb.is_hardstock,
                sb.primary_status
            FROM shadows_buylist sb
            LEFT JOIN viagogo_sales vgs ON sb.id = vgs.id
            LEFT JOIN potential_discounts_snapshot pds ON sb.event_code = pds.event_code
            WHERE {query_filter_str}
            ORDER BY sb.{sort_by} {sort_order.upper()} NULLS LAST, sb.created_at desc NULLS LAST, sb.order_claimed_created_at desc NULLS LAST, sb.id desc NULLS LAST
            LIMIT {page_size} OFFSET {offset}
        """

        print("Params being passed to the query:", params)
        
        db = get_pg_buylist_readonly_database()
        async with db.transaction():
            await db.execute("SET LOCAL work_mem = '256MB'")
            pg_results = await db.fetch_all(pg_query, params)

        # Get account nicknames and IDs for buyer emails
        buyer_emails = [r['buyer_email'] for r in pg_results if r['buyer_email']]
        account_map = {}
        if buyer_emails:
            # Create named parameters for each email
            email_params = {f'email_{i}': email for i, email in enumerate(buyer_emails)}
            placeholders = ','.join([f':{param_name}' for param_name in email_params.keys()])

            ams_query = f"""
                SELECT ae.email_address, aa.nickname, aa.id as account_id
                FROM ams.ams_email ae
                JOIN ams.ams_account aa ON aa.ams_email_id = ae.id
                WHERE ae.email_address IN ({placeholders})
            """
            ams_results = await get_pg_readonly_database().fetch_all(ams_query, email_params)
            account_map = {
                r['email_address']: {
                    'nickname': r['nickname'],
                    'account_id': r['account_id']
                }
                for r in ams_results
            }

        # Add account info to each item
        items = []
        for r in pg_results:
            item_dict = dict(r)
            account_info = account_map.get(r['buyer_email'], {})

            # Serialize without account info
            item = BuyListItemSerializer(**item_dict)

            # Convert back to dict and add account info
            item_with_account = item.model_dump()
            item_with_account['account'] = account_info.get('nickname')
            item_with_account['ams_account_id'] = account_info.get('account_id')

            items.append(item_with_account)

        total = await get_buylist_count(query_filter_str, params)
        return {"items": items, "total": total}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred while getting buy list: {str(e)}") from e


async def get_account_ids(
        is_escalated: Optional[bool] = None,
        exchange_marketplaces: Optional[List[str]] = None,
        event_name: Optional[str] = None,
        buylist_status: Optional[List[str]] = None,
        venue_city: Optional[str] = None,
        buyers: Optional[List[str]] = None,
        subs: Optional[bool] = None,
        nih: Optional[bool] = None,
        escalated: Optional[bool] = None,
        mismapped: Optional[bool] = None,
        search_term: Optional[str] = None,
        currency_code: Optional[List[str]] = None
) -> List[str]:
    """Get list of unique account IDs matching filters."""
    try:
        query_filters, params = build_common_filters(
            is_escalated=is_escalated,
            exchange_marketplaces=exchange_marketplaces,
            event_name=event_name,
            buylist_status=buylist_status,
            venue_city=venue_city,
            buyers=buyers,
            subs=subs,
            nih=nih,
            escalated=escalated,
            mismapped=mismapped,
            search_term=search_term,
            currency_code=currency_code
        )

        query_filter_str = " AND ".join(query_filters) if query_filters else "1=1"

        pg_query = f"""
            SELECT DISTINCT sb.account_id as account_id
            FROM shadows_buylist sb
            WHERE {query_filter_str}
        """
        pg_results = await get_pg_buylist_readonly_database().fetch_all(pg_query, params)
        return [dict(r)["account_id"] for r in pg_results]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting account ids") from e


async def get_buyers(
        is_escalated: Optional[bool] = None,
        account_ids: Optional[List[str]] = None,
        exchange_marketplaces: Optional[List[str]] = None,
        event_name: Optional[str] = None,
        buylist_status: Optional[List[str]] = None,
        venue_city: Optional[str] = None,
        subs: Optional[bool] = None,
        nih: Optional[bool] = None,
        escalated: Optional[bool] = None,
        mismapped: Optional[bool] = None,
        search_term: Optional[str] = None,
        currency_code: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """Get list of unique buyers matching filters."""
    try:
        query_filters, params = build_common_filters(
            is_escalated=is_escalated,
            account_ids=account_ids,
            exchange_marketplaces=exchange_marketplaces,
            event_name=event_name,
            buylist_status=buylist_status,
            venue_city=venue_city,
            subs=subs,
            nih=nih,
            escalated=escalated,
            mismapped=mismapped,
            search_term=search_term,
            currency_code=currency_code
        )

        query_filter_str = " AND ".join(query_filters) if query_filters else "1=1"

        pg_query = f"""
            SELECT DISTINCT sb.buyer as name
            FROM shadows_buylist sb
            WHERE {query_filter_str}
        """
        pg_results = await get_pg_buylist_readonly_database().fetch_all(pg_query, params)
        return [dict(r) for r in pg_results]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting buyers") from e


async def get_exchange_marketplaces(
        is_escalated: Optional[bool] = None,
        account_ids: Optional[List[str]] = None,
        exchange_marketplaces: Optional[List[str]] = None,
        event_name: Optional[str] = None,
        buyers: Optional[List[str]] = None,
        buylist_status: Optional[List[str]] = None,
        venue_city: Optional[str] = None,
        subs: Optional[bool] = None,
        nih: Optional[bool] = None,
        escalated: Optional[bool] = None,
        mismapped: Optional[bool] = None,
        search_term: Optional[str] = None,
        currency_code: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """Get list of unique exchange marketplaces matching filters."""
    try:
        query_filters, params = build_common_filters(
            is_escalated=is_escalated,
            account_ids=account_ids,
            exchange_marketplaces=exchange_marketplaces,
            event_name=event_name,
            buyers=buyers,
            buylist_status=buylist_status,
            venue_city=venue_city,
            subs=subs,
            nih=nih,
            escalated=escalated,
            mismapped=mismapped,
            search_term=search_term,
            currency_code=currency_code
        )

        query_filter_str = " AND ".join(query_filters) if query_filters else "1=1"

        pg_query = f"""
            SELECT DISTINCT sb.exchange as exchange
            FROM shadows_buylist sb
            WHERE {query_filter_str}
        """
        pg_results = await get_pg_buylist_readonly_database().fetch_all(pg_query, params)
        return [dict(r)["exchange"] for r in pg_results]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting buyers") from e


async def get_buylist_item(item_id: str, timezone: Optional[str] = "America/Chicago") -> BuyListItemSerializer:
    """Get single buylist item by ID."""
    try:
        params = {"id": item_id}
        pg_query = f"""
            SELECT 
                sb.id,
                sb.event_state,
                sb.account_id,
                sb.exchange,
                to_char(
                    (COALESCE(
                        vgs.created_at,
                        sb.transaction_date
                    ) AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as transaction_date,
                sb.event_name,
                sb.event_date,
                sb.section,
                sb."row",
                sb.quantity,
                sb.venue,
                sb.venue_city,
                sb.buylist_status,
                sb.link,
                sb.subs,
                sb.viagogo_order_status,
                sb.card,
                sb.amount,
                sb.confirmation_number,
                sb.buyer,
                sb.delivery_method,
                sb.discount,
                sb.was_offer_extended,
                sb.nih,
                sb.mismapped,
                sb.was_discount_code_used,
                sb.date_last_checked,
                sb.date_tickets_available,
                sb.bar_code,
                sb.notes,
                sb.buylist_order_status,
                sb.escalated_to,
                sb.listing_notes,
                sb.sales_source,
                sb.created_at,
                sb.nocharge_price,
                sb.event_code,
                sb.performer_id,
                sb.venue_id,
                to_char(
                    (sb.purchase_confirmation_created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as purchase_confirmation_created_at,
                to_char(
                    (sb.order_claimed_created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as order_claimed_created_at,
                sb.buyer_email,
                sb.currency_code,
                to_char(
                    (sb.grabbed_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as grabbed_at,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = sb.event_code AND sb.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = sb.performer_id AND sb.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = sb.venue_id AND sb.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount,
                pds.percent_discount,
                sb.is_hardstock,
                sb.primary_status
            FROM shadows_buylist sb
            LEFT JOIN viagogo_sales vgs ON sb.id = vgs.id 
            LEFT JOIN potential_discounts_snapshot pds ON sb.event_code = pds.event_code
            WHERE sb.id = :id
        """
        pg_result = await get_pg_buylist_readonly_database().fetch_one(pg_query, params)
        if not pg_result:
            raise HTTPException(status_code=404, detail="Item not found")
        return BuyListItemSerializer(**dict(pg_result))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting buy list item") from e


async def grab_items(item_ids: List[str], user: User) -> Dict[str, Any]:
    """Assign items to a user."""
    try:
        params = {
            "buyer": user.name,
            "buyer_email": user.email,
            "current_time": datetime.now(timezone.utc).replace(tzinfo=None)
        }

        item_placeholders = ", ".join([f":id_{i}" for i in range(len(item_ids))])
        for i, item_id in enumerate(item_ids):
            params[f"id_{i}"] = item_id

        query = f"""
            UPDATE shadows_buylist
            SET buyer = :buyer, buyer_email = :buyer_email, grabbed_at = :current_time
            WHERE id IN ({item_placeholders})
            RETURNING id
        """
        updated_items = await get_pg_buylist_database().fetch_all(query, params)
        updated_ids = [r["id"] for r in updated_items]
        not_found_ids = list(set(item_ids) - set(updated_ids))

        if not updated_ids:
            raise HTTPException(status_code=404, detail="No items found or update failed.")

        await log_buylist_action(
            operation="grab",
            user=user,
            module="buylist_grab",
            data={
                "grabbed_items": updated_ids,
                "not_found_items": not_found_ids
            }
        )

        return {
            "status": "success",
            "message": "Items successfully grabbed",
            "updated_items": updated_ids,
            "not_found_items": not_found_ids
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while grabbing items") from e


def is_valid_confirmation_number(confirmation_number: str) -> bool:
    """
    Validate if the input contains a valid confirmation number.
    Can extract confirmation numbers from text like "54-20279/VAN new : 54-20279/VAN"
    but rejects confirmation numbers in error contexts like "error: 31-45955/TOR"

    Expected formats:
    - XX-XXXXX/XXX (e.g., 27-56236/WES, 13-11297/CAR)
    - X-XXXXX/XXX (e.g., 8-46327/ARZ, 9-12673/CH6)
    - XXX-XXXXXX/XXX (flexible digit counts)
    - Pure numeric strings (e.g., 10232868, 2097607)

    Args:
        confirmation_number: The input string that may contain a confirmation number

    Returns:
        bool: True if a valid confirmation number is found in non-error context, False otherwise
    """
    if not confirmation_number or not isinstance(confirmation_number, str):
        return False

    cleaned_input = confirmation_number.strip().upper()

    # Check for error context keywords that should invalidate the confirmation
    error_keywords = [
        'ERROR', 'FAILED', 'FAILURE', 'INVALID', 'WRONG', 'INCORRECT',
        'PROBLEM', 'ISSUE', 'REJECTED', 'DECLINED', 'DENIED'
    ]

    # If the text contains error keywords, reject it
    for keyword in error_keywords:
        if keyword in cleaned_input:
            return False

    # Check for pure numeric confirmation numbers (4-12 digits)
    if cleaned_input.isdigit() and 4 <= len(cleaned_input) <= 12:
        return True

    # Pattern to find confirmation numbers anywhere in the text
    # Look for: 1-4 digits, hyphen, 3-8 digits, forward slash, 2-5 alphanumeric characters
    pattern = r'\d{1,4}-\d{3,8}/[A-Z0-9]{2,5}'

    # Find all potential confirmation numbers in the text
    matches = re.findall(pattern, cleaned_input)

    if not matches:
        return False

    # Validate at least one match meets our criteria
    for match in matches:
        parts = match.split('-')
        if len(parts) != 2:
            continue

        prefix_digits = parts[0]
        middle_and_suffix = parts[1].split('/')
        if len(middle_and_suffix) != 2:
            continue

        middle_digits = middle_and_suffix[0]
        suffix_chars = middle_and_suffix[1]

        # Check if this match meets our length constraints
        if (1 <= len(prefix_digits) <= 4 and
                3 <= len(middle_digits) <= 8 and
                2 <= len(suffix_chars) <= 5):
            return True

    return False


def add_purchase_confirmation_date(params: dict[str, Any]) -> None:
    if "confirmation_number" in params:
        params["purchase_confirmation_created_at"] = datetime.now(timezone.utc).replace(tzinfo=None)


async def update_item(item_id: str, update_data: UpdateBuyListItemRequest) -> Dict[str, Any]:
    """Update a single buylist item."""
    try:
        # First, get the current item to check its status
        current_item_query = """
            SELECT buylist_order_status, confirmation_number
            FROM shadows_buylist
            WHERE id = :id
        """
        current_item = await get_pg_buylist_readonly_database().fetch_one(current_item_query, {"id": item_id})

        if not current_item:
            raise HTTPException(status_code=404, detail="Item not found.")

        set_clauses = []
        params = update_data.model_dump(exclude_unset=True) | {"id": item_id}
        add_purchase_confirmation_date(params)

        # Check if confirmation_number is being updated and handle status change
        if "confirmation_number" in params:
            confirmation_number = params["confirmation_number"]
            current_status = current_item["buylist_order_status"]

            # Only update status if current status is "Unbought" and confirmation number is valid
            if (current_status == "Unbought" and
                    confirmation_number and
                    is_valid_confirmation_number(confirmation_number)):

                # Only set status to Pending if it's not already explicitly set in the update
                if "buylist_order_status" not in params:
                    params["buylist_order_status"] = "Pending"

        for key, value in params.items():
            set_clauses.append(f"{key} = :{key}")

        if not set_clauses:
            raise HTTPException(
                status_code=400,
                detail="At least one field must be provided to update."
            )

        set_clause = ", ".join(set_clauses)
        query = f"""
            UPDATE shadows_buylist
            SET {set_clause}
            WHERE id = :id
            RETURNING id
        """

        result = await get_pg_buylist_database().fetch_one(query, params)
        if result is None or result["id"] is None:
            raise HTTPException(status_code=404, detail="Item not found or update failed.")

        return {
            "status": "success",
            "message": "Information updated successfully",
            "item_id": result["id"]
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while updating") from e


async def update_items_status(item_ids: List[str], status: str) -> Dict[str, List[str]]:
    """Update buylist_order_status for multiple items atomically."""
    try:
        params = {"status": status}

        item_placeholders = ", ".join([f":id_{i}" for i in range(len(item_ids))])
        for i, item_id in enumerate(item_ids):
            params[f"id_{i}"] = item_id

            # Update query that returns only IDs where status actually changed
            update_query = f"""
                   UPDATE shadows_buylist
                   SET buylist_order_status = :status
                   WHERE id IN ({item_placeholders})
                   RETURNING id
               """

            # Execute update and get actually updated items
            updated_items = await get_pg_buylist_database().fetch_all(update_query, params)
            updated_ids = [item["id"] for item in updated_items]
            not_found_ids = list(set(item_ids) - set(updated_ids))

            return {
                "status": "success",
                "message": "Batch update completed",
                "updated_items": updated_ids,
                "not_found_items": not_found_ids
            }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail="An error occurred while updating items statuses"
        ) from e


async def log_buylist_action(
        operation: str,
        user: User,
        module: str,
        data: Dict[str, Any]
):
    """
    Logs buylist-specific actions into the tracker table.
    """
    log_entry = {
        "id": uuid4().hex,
        "operation": operation,
        "module": module,
        "user": user.name,
        "email": user.email,
        "data": json.dumps(data, default=str),
        "created": datetime.now(timezone.utc).replace(tzinfo=None)
    }

    query = """
            INSERT INTO shadows_user_tracker (id, operation, module, "user", email, data, created)
            VALUES (:id, :operation, :module, :user, :email, :data, :created)
        """
    await get_pg_buylist_database().execute(query, log_entry)


async def save_error_report(save_data: SaveErrorReportRequest, user_name: str) -> dict:
    """Save error report to buylist_error_report table."""
    try:
        # Prepare insert query
        insert_query = """
            INSERT INTO buylist_error_report (
                id, 
                buylist_id, 
                error, 
                added_by, 
                account_id
            ) VALUES (
                :id, 
                :buylist_id, 
                :error, 
                :added_by, 
                :account_id
            )
            RETURNING id, created_at
        """

        params = {
            "id": uuid4().hex,
            "buylist_id": save_data.buylist_id,
            "error": save_data.error,
            "added_by": user_name,
            "account_id": save_data.account_id
        }

        # Execute insert and get the created record
        result = await get_pg_buylist_database().fetch_one(insert_query, params)

        return {
            "status": "success",
            "message": "Error report saved successfully",
            "error_report_id": result["id"],
            "created_at": result["created_at"].isoformat()
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while saving error report: {str(e)}"
        ) from e


async def unclaim_sale(ids, user) -> UnclaimSalesResponse:
    try:
        db = get_pg_buylist_database()
        deleted_ids = []
        blocked_ids = []

        if ids:
            protected_statuses = ['Fulfilled', 'Invoiced']

            check_status_sql = """
                SELECT id, buylist_order_status
                FROM shadows_buylist
                WHERE id = :id
            """

            delete_sql = """
                DELETE FROM shadows_buylist
                WHERE id = :id AND buylist_order_status NOT IN ('Fulfilled', 'Invoiced')
                RETURNING id
            """

            async with db.transaction():
                for id in ids:
                    status_row = await db.fetch_one(check_status_sql, {"id": id})
                    if status_row:
                        if status_row["buylist_order_status"] in protected_statuses:
                            blocked_ids.append(id)
                        else:
                            delete_row = await db.fetch_one(delete_sql, {"id": id})
                            if delete_row:
                                deleted_ids.append(delete_row["id"])

            if deleted_ids:
                await log_buylist_action(
                    operation='unclaim',
                    user=user,
                    module='buylist_unclaim',
                    data={'unclaim_items': deleted_ids, 'blocked_items': blocked_ids}
                )

        return UnclaimSalesResponse(ids=deleted_ids, blocked_ids=blocked_ids)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def get_sale_history(id: str) -> List[SaleHistoryModel]:
    try:
        sql = """
            SELECT *
            FROM shadows_user_tracker
            WHERE (
                data::jsonb ? 'claim_items'
                AND data::jsonb->'claim_items' @> to_jsonb(ARRAY[:id]::text[])
            )
            OR (
                data::jsonb ? 'grabbed_items'
                AND data::jsonb->'grabbed_items' @> to_jsonb(ARRAY[:id]::text[])
            )
        """

        pg_results = await get_pg_buylist_readonly_database().fetch_all(query=sql, values={"id": id})
        items = [SaleHistoryModel(**dict(result)) for result in pg_results]
        return items

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
