import os
import traceback
from datetime import datetime, timezone
from typing import Optional, List

import pytz
from fastapi import APIRouter, Depends, Query, HTTPException
from starlette import status

from app.auth.auth_system import get_current_user_with_roles
from app.database import get_pg_buylist_database
from app.db.buylist_db import log_buylist_action
from app.model.sales import ClaimSalesRequest, UnclaimedSalesSerializer, ClaimSalesResponse
from app.model.user import User

POSTGRES_URL_BUYLIST = os.getenv("POSTGRES_URL_BUYLIST")
ALLOWED_STATUSES = ['Confirm Sales', 'Get Paid', 'Upload Transfer Receipts']

router = APIRouter(prefix="/unclaimed-sales")


@router.get("", dependencies=[Depends(get_current_user_with_roles(["user", "shadows"]))])
async def get_unclaimed_sales(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        page_size: Optional[int] = Query(
            default=None,
            description="Number of results per page"
        ),
        page: Optional[int] = Query(
            default=1,
            description="Page number to return",
            ge=1
        ),
        sort_by: Optional[str] = Query(
            default="created_at",
            description="Column to sort by",
        ),
        sort_order: Optional[str] = Query(
            default="asc",
            description="Sort order: 'asc' or 'desc'",
        ),
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        search_term: Optional[str] = Query(None),
        statuses: Optional[List[str]] = Query(None),
        # user: User = Depends(get_current_user_with_roles(["user", "shadows"])),
):
    try:
        offset = (page - 1) * page_size if page_size else 0
        limit_offset = "LIMIT :page_size OFFSET :offset" if page_size else ""
        query_filters = []
        params = {}

        valid_sort_columns = ["id", "account_id", "event_id", "status", "section",
                              "row", "listing_notes", "quantity", "amount", "event_name",
                              "start_date", "created_at", "currency_code", "delivery_method", "venue",
                              "city", "country", "topic", "sales_source", "exchange"]
        if sort_by not in valid_sort_columns:
            raise HTTPException(status_code=400, detail="Invalid sort_by value.")

        sort_order = sort_order.lower()
        if sort_order not in ["asc", "desc"]:
            raise HTTPException(status_code=400, detail="Invalid sort_order value.")

        vivid_filters = []
        viagogo_filters = []
        gotickets_filters = []
        seatgeek_filters = []
        stubhub_filters = []

        if account_ids:
            account_placeholders = ", ".join([f":account_{i}" for i in range(len(account_ids))])
            vivid_filters.append(f" vs.vivid_account_id IN ({account_placeholders})")
            viagogo_filters.append(f" vgs.viagogo_account_id IN ({account_placeholders})")
            gotickets_filters.append(f" gs.gotickets_account_id IN ({account_placeholders})")
            seatgeek_filters.append(f" ss.seatgeek_account_id IN ({account_placeholders})")
            stubhub_filters.append(f" shs.account_id IN ({account_placeholders})")
            for i, account_id in enumerate(account_ids):
                params[f"account_{i}"] = account_id

        if statuses:
            status_placeholders = ", ".join([f":status_{i}" for i in range(len(statuses))])
            if "Confirm Sales" in statuses:
                vivid_filters.append(f"1=1")
                stubhub_filters.append(f"1=1")
            else:
                vivid_filters.append(f"1=0")
                stubhub_filters.append(f"1=0")
            viagogo_filters.append(f" vgs.status IN ({status_placeholders})")
            gotickets_filters.append(f" gs.seller_status IN ({status_placeholders})")
            seatgeek_filters.append(f" ss.status IN ({status_placeholders})")
            for i, status in enumerate(statuses):
                params[f"status_{i}"] = status

        if exchange_marketplaces:
            pass
            # if exchange.lower() == 'ticketmaster':
            #     query_filters.append("source_table = 'vivid'")
            # elif exchange.lower() == 'viagogo':
            #     query_filters.append("source_table = 'viagogo'")
            # params["exchange"] = exchange
        if search_term:
            vivid_search = """
            ( vs.order_id::varchar ILIKE :search_term
            OR vs.vivid_account_id ILIKE :search_term
            OR vs.section ILIKE :search_term
            OR vs.row ILIKE :search_term
            OR vs.notes ILIKE :search_term
            OR vs.event ILIKE :search_term
            OR vs.venue ILIKE :search_term
            OR vs.status ILIKE :search_term
            OR vs.production_id::varchar ILIKE :search_term )
            """
            viagogo_search = """
            ( vgs.id ILIKE :search_term
            OR vgs.viagogo_account_id ILIKE :search_term
            OR vgs.section ILIKE :search_term
            OR vgs."row" ILIKE :search_term
            OR vgs.event_name ILIKE :search_term
            OR vgs.venue ILIKE :search_term
            OR vgs.status ILIKE :search_term
            OR vgs.viagogo_event_id::varchar ILIKE :search_term )
            """
            gotickets_search = """
            ( gs.id ILIKE :search_term
            OR gs.gotickets_account_id ILIKE :search_term
            OR gs.section ILIKE :search_term
            OR gs."row" ILIKE :search_term
            OR gs.notes ILIKE :search_term
            OR gs.event_name ILIKE :search_term
            OR gs.seller_status ILIKE :search_term
            OR gs.event_id::varchar ILIKE :search_term )
            """
            seatgeek_search = """
            ( ss.id ILIKE :search_term
            OR ss.seatgeek_account_id ILIKE :search_term
            OR ss.section ILIKE :search_term
            OR ss."row" ILIKE :search_term
            OR ss.event ILIKE :search_term
            OR ss.status ILIKE :search_term
            OR ss.event_id ILIKE :search_term )
            """
            stubhub_search = """
            ( shs.id::varchar ILIKE :search_term
            OR shs.account_id ILIKE :search_term
            OR shs.section ILIKE :search_term
            OR shs."row" ILIKE :search_term
            OR shs.listing_notes ILIKE :search_term
            OR shs.event_name ILIKE :search_term
            OR shs.external_id ILIKE :search_term )
            """
            vivid_filters.append(vivid_search)
            viagogo_filters.append(viagogo_search)
            gotickets_filters.append(gotickets_search)
            seatgeek_filters.append(seatgeek_search)
            stubhub_filters.append(stubhub_search)
            params["search_term"] = f"%{search_term}%"

        vivid_filter_str = " AND " + " AND ".join(vivid_filters) if vivid_filters else ""
        viagogo_filter_str = " AND " + " AND ".join(viagogo_filters) if viagogo_filters else ""
        gotickets_filters_str = " AND " + " AND ".join(gotickets_filters) if gotickets_filters else ""
        seatgeek_filters_str = " AND " + " AND ".join(seatgeek_filters) if seatgeek_filters else ""
        stubhub_filters_str = " AND " + " AND ".join(stubhub_filters) if stubhub_filters else ""
        main_filter_str = " WHERE " + " AND ".join(query_filters) if query_filters else ""

        allowed_statuses = ", ".join(f"'{status}'" for status in ALLOWED_STATUSES)

        pg_query = f"""
        WITH combined_sales AS (
            SELECT 
                vs.order_id::varchar as id,
                vs.vivid_account_id as account_id,
                vs.production_id as event_id,
                'Confirm Sales' as status,
                RIGHT(venue, 2) AS event_state,
                vs.section,
                vs.row,
                vs.orig_section,
                vs.orig_row,
                vs.notes as listing_notes,
                vs.quantity,
                vs.cost as amount,
                vs.nocharge_price,
                vs.event_url as link,
                vs.event as event_name,
                vs.event_code as event_code,
                vs.performer_id as performer_id,
                vs.venue_id as venue_id,
                to_char(vs.event_date, 'YYYY-MM-DD HH24:MI:SS') as start_date,
                to_char(
                    (vs.order_date AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as created_at,
                'USD' as currency_code,
                '' as delivery_method,
                TRIM(BOTH ' ' FROM SPLIT_PART(SPLIT_PART(vs.venue, ',', 1), '-', 1)) as venue,
                TRIM(BOTH ' ' FROM SPLIT_PART(SPLIT_PART(vs.venue, ',', 1), '-', 2)) as city,
                'US' as country,
                'Sales' as topic,
                'Vivid' as sales_source,
                'Ticketmaster' as exchange,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = vs.event_code AND vs.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = vs.performer_id AND vs.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = vs.venue_id AND vs.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount
            FROM vivid_sales vs
            LEFT JOIN potential_discounts_snapshot pds ON vs.event_code = pds.event_code
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = vs.order_id::varchar
            )
            AND vs.status != 'Complete'
            {vivid_filter_str}

            UNION ALL

            SELECT 
                vgs.id,
                vgs.viagogo_account_id as account_id,
                vgs.viagogo_event_id as event_id,
                vgs.status as status,
                vgs.state_province AS event_state,
                vgs.section,
                vgs."row",
                vgs.orig_section,
                vgs.orig_row,
                vgs.notes as listing_notes,
                vgs.number_of_tickets as quantity,
                vgs.amount,
                vgs.nocharge_price,
                vgs.event_url as link,
                vgs.event_name,
                vgs.event_code as event_code,
                vgs.performer_id as performer_id,
                vgs.venue_id as venue_id,
                to_char(vgs.start_date, 'YYYY-MM-DD HH24:MI:SS') as start_date,
                to_char(
                    (vgs.created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as created_at,
                COALESCE(vgs.currency_code, 'USD') as currency_code,
                COALESCE(vgs.delivery_name, '') as delivery_method,
                vgs.venue,
                vgs.city,
                vgs.country,
                'Sales' as topic,
                'Viagogo' as sales_source,
                CASE 
                    WHEN va.marketplaces->>0 = 'ticketmaster' THEN 'Ticketmaster'
                    WHEN va.marketplaces->>0 = 'tessitura' THEN 'Tessitura'
                    WHEN va.marketplaces->>0 = 'ticketmastermexico' THEN 'TicketmasterMexico'
                ELSE NULL     
            END AS exchange,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = vgs.event_code AND vgs.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = vgs.performer_id AND vgs.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = vgs.venue_id AND vgs.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds2.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount
            FROM viagogo_sales vgs
            JOIN viagogo_account va 
            ON va.viagogo_account_id = vgs.viagogo_account_id 
            AND vgs.viagogo_account_id LIKE '%gmail.com%'
            LEFT JOIN potential_discounts_snapshot pds2 ON vgs.event_code = pds2.event_code
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = vgs.id
            )
            AND vgs.status IN ({allowed_statuses}) AND vgs.created_at > '2024-09-09'
            {viagogo_filter_str}
            
            UNION ALL
            
            SELECT 
                gs.id,
                gs.gotickets_account_id as account_id,
                gs.event_id,
                gs.seller_status as status,
                gs.state AS event_state,
                gs.section,
                gs."row",
                gs.orig_section,
                gs.orig_row,
                gs.notes as listing_notes,
                gs.quantity,
                gs.total_payout as amount,
                gs.nocharge_price,
                gs.event_url as link,
                gs.event_name,
                gs.event_code as event_code,
                gs.performer_id as performer_id,
                gs.venue_id as venue_id,
                to_char(gs.event_date, 'YYYY-MM-DD HH24:MI:SS') as start_date,
                to_char(
                    (gs.create_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as created_at,
                CASE 
                    WHEN COALESCE(gs.country, 'US') = 'CA' THEN 'CAD'
                    ELSE 'USD'
                END as currency_code,
                COALESCE(gs.delivery_method, '') as delivery_method,
                gs.venue,
                gs.city,
                COALESCE(gs.country, 'US') as country,
                'Sales' as topic,
                'GoTickets' as sales_source,
                'Ticketmaster' as exchange,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = gs.event_code AND gs.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = gs.performer_id AND gs.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = gs.venue_id AND gs.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds3.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount
            FROM gotickets_sales gs
            LEFT JOIN potential_discounts_snapshot pds3 ON gs.event_code = pds3.event_code
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = gs.id
            )
            {gotickets_filters_str}
            
            UNION ALL
            
            SELECT 
                ss.id,
                ss.seatgeek_account_id as account_id,
                ss.event_id::int,
                ss.status,
                NULL AS event_state,
                ss.section,
                ss."row",
                ss.orig_section,
                ss.orig_row,
                ss.notes as listing_notes,
                ss.quantity,
                CEIL(ss.total * 100) / 100 as amount,
                CEIL(ss.nocharge_price * 100) / 100 as nocharge_price,
                ss.event_url as link,
                ss.event,
                ss.event_code as event_code,
                ss.performer_id as performer_id,
                ss.venue_id as venue_id,
                (ss.event_date || 'T' || ss.event_time) as start_date,
                to_char(
                    (ss.created AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as created_at,
                CASE 
                    WHEN COALESCE(ss.country, 'US') = 'CA' THEN 'CAD'
                    ELSE 'USD'
                END as currency_code,
                '' as delivery_method,
                ss.venue,
                '' as city,
                 COALESCE(ss.country, 'US') as country,
                'Sales' as topic,
                'SeatGeek' as sales_source,
                'Ticketmaster' as exchange,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = ss.event_code AND ss.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = ss.performer_id AND ss.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = ss.venue_id AND ss.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds4.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount
            FROM seatgeek_sales ss
            LEFT JOIN potential_discounts_snapshot pds4 ON ss.event_code = pds4.event_code
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = ss.id
            )
            {seatgeek_filters_str}
            
            UNION ALL
            
            SELECT 
                shs.id::varchar,
                shs.account_id as account_id,
                shs.inventory_id::int as event_id,
                'Confirm Sales' as status,
                NULL AS event_state,
                shs.section,
                shs."row",
                shs.orig_section,
                shs.orig_row,
                shs.listing_notes,
                shs.quantity,
                shs.total_net_proceeds as amount,
                shs.nocharge_price,
                shs.event_url as link,
                shs.event_name,
                shs.event_code as event_code,
                shs.performer_id as performer_id,
                shs.venue_id as venue_id,
                to_char(shs.event_date, 'YYYY-MM-DD HH24:MI:SS') as start_date,
                to_char(
                    (shs.sale_date AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'),
                    'YYYY-MM-DD HH24:MI:SS.MS'
                ) as created_at,
                COALESCE(shs.currency_code, 'USD') as currency_code,
                COALESCE(shs.stock_type, '') as delivery_method,
                shs.venue_name as venue,
                shs.city as city,
                COALESCE(shs.country, 'US') as country,
                'Sales' as topic,
                'Stubhub' as sales_source,
                'Ticketmaster' as exchange,
                (
                    EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.event_code = shs.event_code AND shs.event_code IS NOT NULL)
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.performer_id = shs.performer_id AND shs.performer_id IS NOT NULL) 
                    OR EXISTS(SELECT 1 FROM shadows_discount sd WHERE sd.venue_id = shs.venue_id AND shs.venue_id IS NOT NULL)
                ) as has_discounts,
                CASE WHEN pds5.event_code IS NOT NULL THEN true ELSE false END as has_potential_discount
            FROM stubhub_sales shs
            LEFT JOIN potential_discounts_snapshot pds5 ON shs.event_code = pds5.event_code
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = shs.id::varchar
            )
            {stubhub_filters_str}
        )
        SELECT * FROM combined_sales
        {main_filter_str}
        ORDER BY {sort_by} {sort_order.upper()}, created_at asc, id desc
        {limit_offset}
        """

        total_sales = await get_sales_count(
            vivid_filter_str,
            viagogo_filter_str,
            gotickets_filters_str,
            seatgeek_filters_str,
            stubhub_filters_str,
            main_filter_str,
            params
        )
        total_statuses = await get_available_statuses(account_ids, exchange_marketplaces, search_term)

        params.update({"page_size": page_size, "offset": offset}) if page_size else ""
        pg_results = await get_pg_buylist_database().fetch_all(pg_query, params)
        items = [UnclaimedSalesSerializer(**dict(r)) for r in pg_results]

        return {
            "items": items,
            "total": total_sales,
            "available_statuses": total_statuses
        }
    except Exception as e:
        traceback.print_exc()
        raise e


@router.get("/report")
async def get_unclaimed_sales_report(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        start_date: Optional[str] = Query(None),
        start_hr: Optional[int] = Query(1),
        end_date: Optional[str] = Query(None),
        end_hr: Optional[int] = Query(24),
        weekday: Optional[int] = Query(7),
        graph: Optional[str] = Query('each'),
        type: Optional[str] = Query('unclaimed'),
        track_interval: Optional[int] = Query(None)
):
    try:

        start_hr = f'0{start_hr - 1}' if start_hr <= 10 else f'{start_hr - 1}'
        end_hr = f'0{end_hr - 1}' if end_hr <= 10 else f'{end_hr - 1}'
        start_datetime = await _get_converted_time(f"{start_date} {start_hr}:00:00", timezone)
        end_datetime = await _get_converted_time(f"{end_date} {end_hr}:59:59", timezone)
        if graph == 'each':
            return await _get_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval, weekday,
                                                       type)
        else:
            return await _get_data_for_graph_avg_type(end_datetime, start_datetime, timezone, track_interval, weekday,
                                                      type)
    except Exception as e:
        traceback.print_exc()
        raise e


async def _get_data_for_graph_avg_type(end_datetime, start_datetime, timezone, track_interval, weekday, type):
    filter = f"and EXTRACT(DOW FROM (ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}')) = {weekday}"
    pg_query = f"""
                    SELECT
                        CASE
                        WHEN {weekday}=0 THEN 'Sunday'
                        WHEN {weekday}=1 THEN 'Monday'
                        WHEN {weekday}=2 THEN 'Tuesday'
                        WHEN {weekday}=3 THEN 'Wednesday'
                        WHEN {weekday}=4 THEN 'Thursday'
                        WHEN {weekday}=5 THEN 'Friday'
                        WHEN {weekday}=6 THEN 'Saturday'
                        WHEN {weekday}=7 THEN 'All Days'
                        end AS date,
                        TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'HH24:MI') AS time,
                        ROUND(AVG(coalesce({type}_number, 0))::numeric,2) as value,
                        ROUND(AVG(coalesce(total_cost_of_{type}, 0))::numeric,2) as sales_cost
                    FROM generate_series(
                        TIMESTAMP '{start_datetime}',
                        TIMESTAMP '{end_datetime}',
                        '{track_interval} minutes'::interval
                    ) AS ts
                    LEFT JOIN {type}_sales_report ON report_time = ts
                    WHERE 1=1
                    {filter if weekday < 7 else ""} 
                    group by date,time
                    ORDER BY date, time;
                """
    print(pg_query)
    pg_results = await get_pg_buylist_database().fetch_all(pg_query)
    return [dict(r) for r in pg_results]


async def _get_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval, weekday, type):
    filter = f"and EXTRACT(DOW FROM (ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}')) = {weekday}"
    pg_query = f"""
            SELECT
                TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'YYYY-MM-DD') AS date,
                TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'HH24:MI') AS time,
                coalesce({type}_number, 0) as value,
                coalesce(total_cost_of_{type}, 0) as sales_cost
            FROM generate_series(
                TIMESTAMP '{start_datetime}',
                TIMESTAMP '{end_datetime}',
                '{track_interval} minutes'::interval
            ) AS ts
            LEFT JOIN {type}_sales_report ON report_time = ts
            WHERE 1=1
            {filter if weekday < 7 else ""} 
            ORDER BY date, time;
        """
    pg_results = await get_pg_buylist_database().fetch_all(pg_query)
    return [dict(r) for r in pg_results]


async def _get_converted_time(dt_str, tmz):
    origin_tz = pytz.timezone(tmz)
    local_dt = origin_tz.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
    utc_dt = local_dt.astimezone(pytz.UTC)
    utc_str = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
    return utc_str


async def get_sales_count(
        vivid_filters_str: str,
        viagogo_filters_str: str,
        gotickets_filters_str: str,
        seatgeek_filters_str: str,
        stubhub_filters_str: str,
        main_filter_str: str,
        params: dict
):
    allowed_statuses = ", ".join(f"'{status}'" for status in ALLOWED_STATUSES)
    total_count_query = f"""
    WITH combined_sales AS (
        SELECT 'vivid' as source_table
        FROM vivid_sales vs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = vs.order_id::varchar
        )
        AND vs.status != 'Complete'
        {vivid_filters_str}

        UNION ALL

        SELECT 'viagogo' as source_table
        FROM viagogo_sales vgs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = vgs.id
        )
        AND vgs.status IN ({allowed_statuses}) AND vgs.created_at > '2024-09-09'
        {viagogo_filters_str}
        
        UNION ALL
        
        SELECT 
            'gotickets' as source_table
        FROM gotickets_sales gs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = gs.id
        )
        {gotickets_filters_str}
        
        UNION ALL
        
        SELECT 
            'seatgeek' as source_table
        FROM seatgeek_sales ss
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = ss.id
        )
        {seatgeek_filters_str}
        
        UNION ALL
        
        SELECT 
            'stubhub' as source_table
        FROM stubhub_sales shs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = shs.id::varchar
        )
        {stubhub_filters_str}
    )
    SELECT COUNT(*) 
    FROM combined_sales
    {main_filter_str}
    """

    total_count_result = await get_pg_buylist_database().fetch_one(total_count_query, params)
    return total_count_result[0] if total_count_result else 0


async def get_available_statuses(
        account_ids: Optional[List[str]] = Query(None),
        exchange_marketplaces: Optional[List[str]] = Query(None),
        search_term: Optional[str] = Query(None),
):
    vivid_filters = []
    viagogo_filters = []
    gotickets_filters = []
    seatgeek_filters = []
    stubhub_filters = []
    main_filters = []
    params = {}

    if account_ids:
        account_placeholders = ", ".join([f":account_{i}" for i in range(len(account_ids))])
        vivid_filters.append(f" vs.vivid_account_id IN ({account_placeholders})")
        viagogo_filters.append(f" vgs.viagogo_account_id IN ({account_placeholders})")
        gotickets_filters.append(f" gs.gotickets_account_id IN ({account_placeholders})")
        seatgeek_filters.append(f" ss.seatgeek_account_id IN ({account_placeholders})")
        stubhub_filters.append(f" shs.account_id IN ({account_placeholders})")
        for i, account_id in enumerate(account_ids):
            params[f"account_{i}"] = account_id

    if search_term:
        vivid_search = """
                           ( vs.order_id::varchar ILIKE :search_term
                           OR vs.vivid_account_id ILIKE :search_term
                           OR vs.section ILIKE :search_term
                           OR vs.row ILIKE :search_term
                           OR vs.notes ILIKE :search_term
                           OR vs.event ILIKE :search_term
                           OR vs.venue ILIKE :search_term
                           OR vs.status ILIKE :search_term
                           OR vs.production_id::varchar ILIKE :search_term )
                           """
        viagogo_search = """
                           ( vgs.id ILIKE :search_term
                           OR vgs.viagogo_account_id ILIKE :search_term
                           OR vgs.section ILIKE :search_term
                           OR vgs."row" ILIKE :search_term
                           OR vgs.event_name ILIKE :search_term
                           OR vgs.venue ILIKE :search_term
                           OR vgs.status ILIKE :search_term
                           OR vgs.viagogo_event_id::varchar ILIKE :search_term )
                           """
        gotickets_search = """
                   ( gs.id ILIKE :search_term
                   OR gs.gotickets_account_id ILIKE :search_term
                   OR gs.section ILIKE :search_term
                   OR gs."row" ILIKE :search_term
                   OR gs.notes ILIKE :search_term
                   OR gs.event_name ILIKE :search_term
                   OR gs.seller_status ILIKE :search_term
                   OR gs.event_id::varchar ILIKE :search_term )
                   """
        seatgeek_search = """
                   ( ss.id ILIKE :search_term
                   OR ss.seatgeek_account_id ILIKE :search_term
                   OR ss.section ILIKE :search_term
                   OR ss."row" ILIKE :search_term
                   OR ss.event ILIKE :search_term
                   OR ss.status ILIKE :search_term
                   OR ss.event_id ILIKE :search_term )
                   """
        stubhub_search = """
                   ( shs.id::varchar ILIKE :search_term
                   OR shs.account_id ILIKE :search_term
                   OR shs.section ILIKE :search_term
                   OR shs."row" ILIKE :search_term
                   OR shs.listing_notes ILIKE :search_term
                   OR shs.event_name ILIKE :search_term
                   OR shs.external_id ILIKE :search_term )
                   """
        vivid_filters.append(vivid_search)
        viagogo_filters.append(viagogo_search)
        gotickets_filters.append(gotickets_search)
        seatgeek_filters.append(seatgeek_search)
        stubhub_filters.append(stubhub_search)
        params["search_term"] = f"%{search_term}%"

    vivid_filter_str = " AND " + " AND ".join(vivid_filters) if vivid_filters else ""
    viagogo_filter_str = " AND " + " AND ".join(viagogo_filters) if viagogo_filters else ""
    gotickets_filters_str = " AND " + " AND ".join(gotickets_filters) if gotickets_filters else ""
    seatgeek_filters_str = " AND " + " AND ".join(seatgeek_filters) if seatgeek_filters else ""
    stubhub_filters_str = " AND " + " AND ".join(stubhub_filters) if stubhub_filters else ""
    main_filter_str = " WHERE " + " AND ".join(main_filters) if main_filters else ""
    statuses = ", ".join(f"'{status}'" for status in ALLOWED_STATUSES)

    pg_query = f"""
    WITH combined_accounts AS (
        SELECT 
            'Confirm Sales' as status
        FROM vivid_sales vs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = vs.order_id::varchar
        )
        AND vs.status != 'Complete'
        {vivid_filter_str}
        
        UNION
        
        SELECT 
            vgs.status as status
        FROM viagogo_sales vgs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = vgs.id
        )
        AND vgs.status IN ({statuses}) AND vgs.created_at > '2024-09-09'
        {viagogo_filter_str}
        
        UNION
        
        SELECT 
            gs.seller_status as status
        FROM gotickets_sales gs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = gs.id
        )
        {gotickets_filters_str}
        
        UNION
        
        SELECT 
            ss.status as status
        FROM seatgeek_sales ss
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = ss.id
        )
        {seatgeek_filters_str}
        
        UNION
        
        SELECT 
            'Confirm Sales' as status
        FROM stubhub_sales shs
        WHERE NOT EXISTS (
            SELECT 1 FROM shadows_buylist sb WHERE sb.id = shs.id::varchar
        )
        {stubhub_filters_str}
    )
    SELECT DISTINCT status
    FROM combined_accounts
    {main_filter_str}
    """
    pg_results = await get_pg_buylist_database().fetch_all(pg_query, params)
    return [dict(r)["status"] for r in pg_results]


@router.get("/available-account-ids")
async def get_account_ids(
        exchange: Optional[str] = Query(None),
        search_term: Optional[str] = Query(None),
        statuses: Optional[List[str]] = Query(None),
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    try:
        vivid_filters = []
        viagogo_filters = []
        gotickets_filters = []
        seatgeek_filters = []
        stubhub_filters = []
        params = {}

        if statuses:
            status_placeholders = ", ".join([f":status_{i}" for i in range(len(statuses))])
            if "Confirm Sales" in statuses:
                vivid_filters.append(f"1=1")
                stubhub_filters.append(f"1=1")
            else:
                vivid_filters.append(f"1=0")
                stubhub_filters.append(f"1=0")
            viagogo_filters.append(f" vgs.status IN ({status_placeholders})")
            gotickets_filters.append(f" gs.seller_status IN ({status_placeholders})")
            seatgeek_filters.append(f" ss.status IN ({status_placeholders})")
            for i, status in enumerate(statuses):
                params[f"status_{i}"] = status

        if search_term:
            vivid_search = """
            ( vs.order_id::varchar ILIKE :search_term
                OR vs.vivid_account_id ILIKE :search_term
                OR vs.section ILIKE :search_term
                OR vs.row ILIKE :search_term
                OR vs.notes ILIKE :search_term
                OR vs.event ILIKE :search_term
                OR vs.venue ILIKE :search_term
                OR vs.status ILIKE :search_term
                OR vs.production_id::varchar ILIKE :search_term )
            """
            viagogo_search = """
            ( vgs.id ILIKE :search_term
                OR vgs.viagogo_account_id ILIKE :search_term
                OR vgs.section ILIKE :search_term
                OR vgs."row" ILIKE :search_term
                OR vgs.event_name ILIKE :search_term
                OR vgs.venue ILIKE :search_term
                OR vgs.status ILIKE :search_term
                OR vgs.viagogo_event_id::varchar ILIKE :search_term )
            """
            gotickets_search = """
                ( gs.id ILIKE :search_term
                OR gs.gotickets_account_id ILIKE :search_term
                OR gs.section ILIKE :search_term
                OR gs."row" ILIKE :search_term
                OR gs.notes ILIKE :search_term
                OR gs.event_name ILIKE :search_term
                OR gs.seller_status ILIKE :search_term
                OR gs.event_id::varchar ILIKE :search_term )
                """
            seatgeek_search = """
                ( ss.id ILIKE :search_term
                OR ss.seatgeek_account_id ILIKE :search_term
                OR ss.section ILIKE :search_term
                OR ss."row" ILIKE :search_term
                OR ss.event ILIKE :search_term
                OR ss.status ILIKE :search_term
                OR ss.event_id ILIKE :search_term )
                """
            stubhub_search = """
                ( shs.id::varchar ILIKE :search_term
                OR shs.account_id ILIKE :search_term
                OR shs.section ILIKE :search_term
                OR shs."row" ILIKE :search_term
                OR shs.listing_notes ILIKE :search_term
                OR shs.event_name ILIKE :search_term
                OR shs.external_id ILIKE :search_term )
                """
            vivid_filters.append(vivid_search)
            viagogo_filters.append(viagogo_search)
            gotickets_filters.append(gotickets_search)
            seatgeek_filters.append(seatgeek_search)
            stubhub_filters.append(stubhub_search)
            params["search_term"] = f"%{search_term}%"

        vivid_filter_str = " AND " + " AND ".join(vivid_filters) if vivid_filters else ""
        viagogo_filter_str = " AND " + " AND ".join(viagogo_filters) if viagogo_filters else ""
        gotickets_filters_str = " AND " + " AND ".join(gotickets_filters) if gotickets_filters else ""
        seatgeek_filters_str = " AND " + " AND ".join(seatgeek_filters) if seatgeek_filters else ""
        stubhub_filters_str = " AND " + " AND ".join(stubhub_filters) if stubhub_filters else ""
        statuses = ", ".join(f"'{status}'" for status in ALLOWED_STATUSES)

        pg_query = f"""
        WITH combined_accounts AS (
            SELECT
                vivid_account_id as account_id
            FROM vivid_sales vs
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = vs.order_id::varchar
            )
            AND vs.status != 'Complete'
            {vivid_filter_str}
            
            UNION
            
            SELECT 
                viagogo_account_id as account_id
            FROM viagogo_sales vgs
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = vgs.id
            )
            AND vgs.status IN ({statuses}) AND vgs.created_at > '2024-09-09'
            {viagogo_filter_str}
            
            UNION 
            
            SELECT 
                gs.gotickets_account_id as account_id
            FROM gotickets_sales gs
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = gs.id
            )
            {gotickets_filters_str}
            
            UNION 
            
            SELECT 
                ss.seatgeek_account_id as account_id
            FROM seatgeek_sales ss
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = ss.id
            )
            {seatgeek_filters_str}
            
            UNION 
            
            SELECT 
                shs.account_id as account_id
            FROM stubhub_sales shs
            WHERE NOT EXISTS (
                SELECT 1 FROM shadows_buylist sb WHERE sb.id = shs.id::varchar
            )
            {stubhub_filters_str}
        )
        SELECT DISTINCT account_id
        FROM combined_accounts
        """

        pg_results = await get_pg_buylist_database().fetch_all(pg_query)
        return [dict(r)["account_id"] for r in pg_results]
    except Exception as e:
        traceback.print_exc()
        raise e


@router.post("/claim", response_model=ClaimSalesResponse, status_code=status.HTTP_200_OK)
async def claim_sales(
        requests: List[ClaimSalesRequest],
        user: User = Depends(get_current_user_with_roles(["user", "shadows"]))
):
    try:
        db = get_pg_buylist_database()
        current_time = datetime.now(timezone.utc).replace(tzinfo=None)

        buylist_data_list = []
        failed_sales = {}

        for request in requests:
            try:
                buylist_data = {
                    "id": request.id,
                    "event_state": request.event_state,
                    "account_id": request.account_id,
                    "exchange": request.exchange,
                    "transaction_date": datetime.fromisoformat(request.created_at).astimezone(timezone.utc).replace(
                        tzinfo=None),
                    "event_name": request.event_name,
                    "event_date": datetime.fromisoformat(request.start_date).replace(tzinfo=None),
                    "section": request.section,
                    "row": request.row,
                    "quantity": request.quantity,
                    "venue": request.venue,
                    "venue_city": request.city,
                    "buylist_status": '',
                    "link": request.link,
                    "subs": 0,
                    "viagogo_order_status": 'Confirm Sales',
                    "buylist_order_status": 'Unbought',
                    "card": '',
                    "nocharge_price": request.nocharge_price,
                    "amount": request.amount,
                    "confirmation_number": '',
                    "buyer": user.name,
                    "delivery_method": '',
                    "discount": '',
                    "was_offer_extended": 0,
                    "nih": 0,
                    "was_discount_code_used": 0,
                    "date_last_checked": None,
                    "date_tickets_available": None,
                    "bar_code": '',
                    "notes": '',
                    "listing_notes": request.listing_notes,
                    "sales_source": request.sales_source,
                    "created_at": current_time,
                    "order_claimed_created_at": current_time,
                    "buyer_email": user.email,
                    "currency_code": request.currency_code,
                    "orig_section": request.orig_section,
                    "orig_row": request.orig_row,
                    "event_code": request.event_code,
                    "performer_id": request.performer_id,
                    "venue_id": request.venue_id,
                    "primary_status": request.primary_status
                }
                buylist_data_list.append(buylist_data)
            except Exception as e:
                failed_sales[request.id] = str(e)

        inserted_ids = []
        if buylist_data_list:
            insert_query = """
            INSERT INTO shadows_buylist (
                id, event_state, account_id, exchange, transaction_date, event_name, event_date, section, row, quantity,
                venue, venue_city, buylist_status, link, subs, viagogo_order_status, buylist_order_status,
                card, nocharge_price, amount, confirmation_number, buyer, delivery_method, discount,
                was_offer_extended, nih, was_discount_code_used, date_last_checked, date_tickets_available,
                bar_code, notes, listing_notes, sales_source, created_at, order_claimed_created_at, buyer_email,
                currency_code, orig_section, orig_row, event_code, performer_id, venue_id, primary_status
            )
            VALUES (
                :id, :event_state, :account_id, :exchange, :transaction_date, :event_name, :event_date, :section, :row, :quantity,
                :venue, :venue_city, :buylist_status, :link, :subs, :viagogo_order_status, :buylist_order_status,
                :card, :nocharge_price, :amount, :confirmation_number, :buyer, :delivery_method, :discount,
                :was_offer_extended, :nih, :was_discount_code_used, :date_last_checked, :date_tickets_available,
                :bar_code, :notes, :listing_notes, :sales_source, :created_at, :order_claimed_created_at, :buyer_email, 
                :currency_code, :orig_section, :orig_row, :event_code, :performer_id, :venue_id, :primary_status
            )
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """

            # Execute individual inserts within a transaction
            async with db.transaction():
                result = []
                for data in buylist_data_list:
                    row = await db.fetch_one(insert_query, data)
                    if row:
                        result.append(row)
                inserted_ids = [r['id'] for r in result]

            # Insert user tracker
            if inserted_ids:
                await log_buylist_action(
                    operation="claim",
                    user=user,
                    module="buylist_claim",
                    data={
                        "claim_items": inserted_ids
                    }
                )
        all_requested_ids = {req.id for req in requests}
        already_claimed_ids = all_requested_ids - set(inserted_ids) - set(failed_sales.keys())

        return ClaimSalesResponse(
            claimed_sales=inserted_ids,
            already_claimed_sales=list(already_claimed_ids),
            failed_sales=list(failed_sales.keys())
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {str(e)}"
        )
