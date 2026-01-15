import json
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional, List, Dict, Any
from fastapi import HTTPException

import snowflake

from app.database import get_snowflake_connection, get_pg_database
from app.model.a_to_z_report import (
    ReviewStatusItem,
    ReviewStatusInput,
    ReviewStatusRequest,
    CustomViewPayload,
    CustomViewResponse,
    DeleteCustomViewPayload
)

def parse_datetime(dt_str: str) -> datetime:
    """
    Parse a date/time string in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format.
    """
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid date/time format: {dt_str}")


async def get_a_to_z_report_overview(
    search_term: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_to_sellout: Optional[int] = None,
    sellout_confidence_min: Optional[float] = None,
    sellout_confidence_max: Optional[float] = None,
    weekend_only: Optional[bool] = None,
    weekdays_only: Optional[bool] = None,
    days_to_show_min: Optional[int] = None,
    days_to_show_max: Optional[int] = None,
    projected_margin_min: Optional[float] = None,
    projected_margin_max: Optional[float] = None,
    velocity_min: Optional[float] = None,
    velocity_max: Optional[float] = None,
    tickets_available_primary_min: Optional[int] = None,
    tickets_available_primary_max: Optional[int] = None,
    tickets_available_secondary_min: Optional[int] = None,
    tickets_available_secondary_max: Optional[int] = None,
    get_in_primary_tickets_min: Optional[int] = None,
    get_in_primary_tickets_max: Optional[int] = None,
    get_in_primary_min: Optional[float] = None,
    get_in_primary_max: Optional[float] = None,
    get_in_secondary_min: Optional[float] = None,
    get_in_secondary_max: Optional[float] = None,
    percent_inventory_currently_available_min: Optional[float] = None,
    percent_inventory_currently_available_max: Optional[float] = None,
    seat_geek_velocity_min: Optional[float] = None,
    seat_geek_velocity_max: Optional[float] = None,
    stubhub_velocity_min: Optional[float] = None,
    stubhub_velocity_max: Optional[float] = None,
    sort_by: Optional[str] = "start_date",
    sort_order: Optional[str] = "desc",
):
    try:
        values = {}
        conditions = []
        # Date filtering
        if start_date and end_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', cp.start_date) >= %(start_date)s")
            conditions.append("DATE_TRUNC('DAY', cp.start_date) <= %(end_date)s")
            values["start_date"] = start_date
            values["end_date"] = end_date

        # Search filtering
        if search_term:
            conditions.append("(cp.event_name ILIKE %(search_term)s)")
            values["search_term"] = f"%{search_term}%"

        # Sellout filtering
        if days_to_sellout is not None:
            conditions.append("cp.days_to_sellout_date <= %(days_to_sellout)s")
            values["days_to_sellout"] = days_to_sellout

        # Sellout confidence filtering
        if sellout_confidence_min is not None:
            conditions.append("cp.sellout_confidence >= %(sellout_confidence_min)s")
            values["sellout_confidence_min"] = sellout_confidence_min

        if sellout_confidence_max is not None:
            conditions.append("cp.sellout_confidence <= %(sellout_confidence_max)s")
            values["sellout_confidence_max"] = sellout_confidence_max

        # day type filtering
        if weekend_only and not weekdays_only:
            conditions.append("cp.day_type = 'Weekend'")
        elif weekdays_only and not weekend_only:
            conditions.append("cp.day_type = 'Weekday'")

        # days to show filtering
        if days_to_show_min is not None:
            conditions.append("cp.days_to_show >= %(days_to_show_min)s")
            values["days_to_show_min"] = days_to_show_min

        if days_to_show_max is not None:
            conditions.append("cp.days_to_show <= %(days_to_show_max)s")
            values["days_to_show_max"] = days_to_show_max

        # projected margin filtering
        if projected_margin_min is not None:
            conditions.append("cp.margin >= %(projected_margin_min)s")
            values["projected_margin_min"] = projected_margin_min

        if projected_margin_max is not None:
            conditions.append("cp.margin <= %(projected_margin_max)s")
            values["projected_margin_max"] = projected_margin_max

        # velocity filtering
        if velocity_min is not None:
            conditions.append("cp.velocityprimary >= %(velocity_min)s")
            values["velocity_min"] = velocity_min

        if velocity_max is not None:
            conditions.append("cp.velocityprimary <= %(velocity_max)s")
            values["velocity_max"] = velocity_max

        # tickets available primary filtering
        if tickets_available_primary_min is not None:
            conditions.append("cp.ticketsavailableprimary >= %(tickets_available_primary_min)s")
            values["tickets_available_primary_min"] = tickets_available_primary_min

        if tickets_available_primary_max is not None:
            conditions.append("cp.ticketsavailableprimary <= %(tickets_available_primary_max)s")
            values["tickets_available_primary_max"] = tickets_available_primary_max
        
        # tickets available secondary filtering
        if tickets_available_secondary_min is not None:
            conditions.append("cp.ticketsavailablesecondary >= %(tickets_available_secondary_min)s")
            values["tickets_available_secondary_min"] = tickets_available_secondary_min

        if tickets_available_secondary_max is not None:
            conditions.append("cp.ticketsavailablesecondary <= %(tickets_available_secondary_max)s")
            values["tickets_available_secondary_max"] = tickets_available_secondary_max
        
        # get in primary tickets filtering
        if get_in_primary_tickets_min is not None:
            conditions.append("cp.getin_primary_tickets >= %(get_in_primary_tickets_min)s")
            values["get_in_primary_tickets_min"] = get_in_primary_tickets_min

        if get_in_primary_tickets_max is not None:
            conditions.append("cp.getin_primary_tickets <= %(get_in_primary_tickets_max)s")
            values["get_in_primary_tickets_max"] = get_in_primary_tickets_max
        
        # get in primary filtering
        if get_in_primary_min is not None:
            conditions.append("cp.getin_primary >= %(get_in_primary_min)s")
            values["get_in_primary_min"] = get_in_primary_min

        if get_in_primary_max is not None:
            conditions.append("cp.getin_primary <= %(get_in_primary_max)s")
            values["get_in_primary_max"] = get_in_primary_max

        # get in secondary filtering
        if get_in_secondary_min is not None:
            conditions.append("cp.getin_secondary >= %(get_in_secondary_min)s")
            values["get_in_secondary_min"] = get_in_secondary_min

        if get_in_secondary_max is not None:
            conditions.append("cp.getin_secondary <= %(get_in_secondary_max)s")
            values["get_in_secondary_max"] = get_in_secondary_max

        # percent inventory currently available filtering
        if percent_inventory_currently_available_min is not None:
            conditions.append(
                "cp.percentage_inventory_currently_available_primary >= %(percent_inventory_currently_available_min)s"
            )
            values["percent_inventory_currently_available_min"] = percent_inventory_currently_available_min

        if percent_inventory_currently_available_max is not None:
            conditions.append(
                "cp.percentage_inventory_currently_available_primary <= %(percent_inventory_currently_available_max)s"
            )
            values["percent_inventory_currently_available_max"] = percent_inventory_currently_available_max

        # seat geek velocity filtering
        if seat_geek_velocity_min is not None:
            conditions.append("cp.seatgeek_velocity >= %(seat_geek_velocity_min)s")
            values["seat_geek_velocity_min"] = seat_geek_velocity_min

        if seat_geek_velocity_max is not None:
            conditions.append("cp.seatgeek_velocity <= %(seat_geek_velocity_max)s")
            values["seat_geek_velocity_max"] = seat_geek_velocity_max
        
        # stubhub velocity filtering
        if stubhub_velocity_min is not None:
            conditions.append("cp.stubhub_velocity >= %(stubhub_velocity_min)s")
            values["stubhub_velocity_min"] = stubhub_velocity_min

        if stubhub_velocity_max is not None:
            conditions.append("cp.stubhub_velocity <= %(stubhub_velocity_max)s")
            values["stubhub_velocity_max"] = stubhub_velocity_max

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        base_sql = f"""
            SELECT 
            cp.TM_EVENT_CODE,
            cp.PRIMARY_EXCHANGE_EVENT_ID,
            cp.SECONDARY_EXCHANGE_EVENT_ID,
            cp.EVENT_NAME,
            cp.START_DATE,
            cp.VENUE,
            cp.TICKETSAVAILABLEPRIMARY,
            cp.TICKETSAVAILABLESECONDARY,
            cp.GETIN_PRIMARY_TICKETS,
            cp.GETIN_PRIMARY,
            cp.GETIN_SECONDARY,
            cp.MARGIN,
            cp.PREDICTED_SELL_OUT_DATE,
            cp.SELLOUT_CONFIDENCE,
            cp.VELOCITYPRIMARY,
            cp.SEATGEEK_VELOCITY,
            cp.STUBHUB_VELOCITY,
            cp.PERCENTAGE_INVENTORY_CURRENTLY_AVAILABLE_PRIMARY,
            cp.DAYS_TO_SELLOUT_DATE,
            cp.DAYS_TO_SHOW,
            cp.DAY_TYPE
            FROM PUBLIC.AtoZ_Events cp
            {where_clause}
        """
        valid_sort_fields = {
            "event_name": "event_name",
            "start_date": "start_date",
            "getin_primary": "getin_primary",
            "getin_secondary": "getin_secondary",
            "margin": "margin",
            "getin_primary_tickets": "getin_primary_tickets",
            "ticketsavailableprimary": "ticketsavailableprimary",
            "predicted_sell_out_date": "predicted_sell_out_date",
            "sellout_confidence": "sellout_confidence",
            "velocityprimary": "velocityprimary",
            "percentage_inventory_currently_available_primary": "percentage_inventory_currently_available_primary",
            "ticketsavailablesecondary": "ticketsavailablesecondary",
            "seatgeek_velocity": "seatgeek_velocity",
            "stubhub_velocity": "stubhub_velocity",
            "venue": "venue",
        }
        sort_by = sort_by.lower()
        if sort_by not in valid_sort_fields:
            sort_by = "start_date"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "asc"
        order_by_clause = f"ORDER BY {sort_by} {sort_order}"

        count_query = f"""
            WITH base_cte AS ({base_sql})
            SELECT COUNT(*) AS total
            FROM base_cte
        """
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_query, values)
            total = cur.fetchone()["TOTAL"]
            # Get paginated data
            if page_size is not None and page is not None:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                    LIMIT %(page_size)s OFFSET %(offset)s
                """
                values["page_size"] = page_size
                values["offset"] = (page - 1) * page_size
            else:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                """
            cur.execute(data_query, values)

            results = cur.fetchall()
            return {
                "items": [dict(r) for r in results],
                "total": total,  # Total number of records
            }
    except Exception as e:
        print(f"Error in get_a_to_z_report_overview: {str(e)}")
        return {"error": str(e)}


async def get_primary_event_stats(event_code: str):
    """Get primary event stats for a specific event code."""
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            query = """
                SELECT 
                    GETIN_PRICE,
                    AVAILABLE_SEATS,
                    COLLECTION_SESSION_TS
                FROM PUBLIC.ATOZ_LISTINGS_PRIMARY
                WHERE TM_EVENT_CODE = %(event_code)s
                ORDER BY COLLECTION_SESSION_TS ASC
            """
            cur.execute(query, {"event_code": event_code})
            results = cur.fetchall()
            return {
                "items": [dict(r) for r in results],
                "total": len(results),  # Total number of records
            }

    except Exception as e:
        print(f"Error in get_primary_event_stats: {str(e)}")
        raise e


async def get_secondary_event_stats(event_code: str):
    """Get primary event stats for a specific event code."""
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            query = """
                SELECT 
                    GETIN_SECONDARY as GETIN_PRICE,
                    AVAILABLE_SEATS_SECONDARY as AVAILABLE_SEATS,
                    SALES_SECONDARY,
                    COLLECTION_DATE as COLLECTION_SESSION_TS
                FROM PUBLIC.ATOZ_LISTINGS_SECONDARY
                WHERE TM_EVENT_CODE = %(event_code)s
                ORDER BY COLLECTION_DATE ASC
            """
            cur.execute(query, {"event_code": event_code})
            results = cur.fetchall()
            return {
                "items": [dict(r) for r in results],
                "total": len(results),  # Total number of records
            }

    except Exception as e:
        print(f"Error in get_secondary_event_stats: {str(e)}")
        raise e

async def get_review_status(
    event_codes: List[str]
) -> List[ReviewStatusItem]:
    query = """
        SELECT event_code, review_status, created_at, updated_at
        FROM atoz_review_status
        WHERE event_code = ANY(:event_codes)
    """
    values = {"event_codes": event_codes}
    rows = await get_pg_database().fetch_all(query=query, values=values)
    return [ReviewStatusItem(**row) for row in rows]

async def post_review_status(payload: ReviewStatusInput) -> dict:
    if payload.review_status is None:
        query = """
            UPDATE atoz_review_status
            SET review_status = '',
                reviewed_by = :reviewed_by,
                updated_at = NOW()
            WHERE event_code = :event_code
        """
    else:
        query = """
            INSERT INTO atoz_review_status (event_code, review_status, reviewed_by, created_at, updated_at)
            VALUES (:event_code, :review_status, :reviewed_by, NOW(), NOW())
            ON CONFLICT (event_code)
            DO UPDATE SET
                review_status = EXCLUDED.review_status,
                reviewed_by = EXCLUDED.reviewed_by,
                updated_at = NOW()
        """

    await get_pg_database().execute(query=query, values=payload.model_dump())
    return {"message": "Review status updated"}


async def get_event_codes_review_status(
    review_status: List[str],
    page_size: int,
    page: int
) -> Dict[str, Any]:
    try:
        offset = (page - 1) * page_size

        if not review_status:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size
            }

        placeholders = ", ".join([f":status_{i}" for i in range(len(review_status))])

        data_query = f"""
            SELECT event_code, review_status, reviewed_by, created_at, updated_at
            FROM atoz_review_status
            WHERE review_status IN ({placeholders})
            ORDER BY event_code
            LIMIT {page_size}
            OFFSET {offset}
        """

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM atoz_review_status
            WHERE review_status IN ({placeholders})
        """

        params = {f"status_{i}": status for i, status in enumerate(review_status)}

        db = get_pg_database()

        rows = await db.fetch_all(query=data_query, values=params)
        total_result = await db.fetch_one(query=count_query, values=params)

        total_count = total_result["total"] if total_result else 0

        return {
            "items": rows,
            "total": total_count,
            "page": page,
            "page_size": page_size
        }
    except Exception as e:
        print(f"Error in get_event_codes_review_status: {str(e)}")
        return {"error": str(e)}

async def api_get_events_with_review_status(payload: ReviewStatusRequest) -> Dict[str, Any]:
    try:
        if not payload.items:
            return {"items": []}

        placeholders = ','.join(['%s'] * len(payload.items))

        query = f"""
            SELECT 
                cp.TM_EVENT_CODE,
                cp.PRIMARY_EXCHANGE_EVENT_ID,
                cp.SECONDARY_EXCHANGE_EVENT_ID,
                cp.EVENT_NAME,
                cp.START_DATE,
                cp.VENUE,
                cp.TICKETSAVAILABLEPRIMARY,
                cp.TICKETSAVAILABLESECONDARY,
                cp.GETIN_PRIMARY_TICKETS,
                cp.GETIN_PRIMARY,
                cp.GETIN_SECONDARY,
                cp.MARGIN,
                cp.PREDICTED_SELL_OUT_DATE,
                cp.SELLOUT_CONFIDENCE,
                cp.VELOCITYPRIMARY,
                cp.SEATGEEK_VELOCITY,
                cp.STUBHUB_VELOCITY,
                cp.PERCENTAGE_INVENTORY_CURRENTLY_AVAILABLE_PRIMARY,
                cp.DAYS_TO_SELLOUT_DATE,
                cp.DAYS_TO_SHOW,
                cp.DAY_TYPE
            FROM PUBLIC.AtoZ_Events cp
            WHERE cp.TM_EVENT_CODE IN ({placeholders})
        """

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, payload.items)
            rows = cur.fetchall()

            return {
                "items": [dict(r) for r in rows]
            }
    except Exception as e:
        print(f"Error in api_get_events_with_review_status: {str(e)}")
        return {"error": str(e)}
    
async def get_section_mapping(
    search_term: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tm_event_code: Optional[str] = None,
    td_event_id: Optional[str] = None,
    tm_section: Optional[str] = None,
    td_section: Optional[str] = None,
    tm_quantity_min: Optional[int] = None,
    tm_quantity_max: Optional[int] = None,
    td_quantity_min: Optional[int] = None,
    td_quantity_max: Optional[int] = None,
    tm_section_capacity_min: Optional[int] = None,
    tm_section_capacity_max: Optional[int] = None,
    td_section_capacity_min: Optional[int] = None,
    td_section_capacity_max: Optional[int] = None,
    tm_percent_remaining_section_min: Optional[float] = None,
    tm_percent_remaining_section_max: Optional[float] = None,
    td_percent_remaining_section_min: Optional[float] = None,
    td_percent_remaining_section_max: Optional[float] = None,
    tm_total_quantity_min: Optional[int] = None,
    tm_total_quantity_max: Optional[int] = None,
    tm_total_capacity_min: Optional[int] = None,
    tm_total_capacity_max: Optional[int] = None,
    tm_total_percent_remaining_min: Optional[float] = None,
    tm_total_percent_remaining_max: Optional[float] = None,
    tm_section_getin_min: Optional[float] = None,
    tm_section_getin_max: Optional[float] = None,
    td_section_getin_min: Optional[float] = None,
    td_section_getin_max: Optional[float] = None,
    tm_section_has_resale: Optional[str] = None,
    predicted_section_sellout_start_date: Optional[str] = None,
    predicted_section_sellout_end_date: Optional[str] = None,
    days_to_sellout_min: Optional[int] = None,
    days_to_sellout_max: Optional[int] = None,
    section_sellout_confidence_min: Optional[float] = None,
    section_sellout_confidence_max: Optional[float] = None,
    section_velocity_min: Optional[float] = None,
    section_velocity_max: Optional[float] = None,
    source_name: Optional[str] = None,
    sort_by: Optional[str] = "start_date",
    sort_order: Optional[str] = "desc",
    review_event_codes: Optional[List[str]] = None
):
    try:
        values = {}
        conditions = []
        
        # Base date filtering
        if start_date and end_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', sm.start_date) >= %(start_date)s")
            conditions.append("DATE_TRUNC('DAY', sm.start_date) <= %(end_date)s")
            values["start_date"] = start_date
            values["end_date"] = end_date

        # Search filtering
        if search_term:
            conditions.append("(sm.event_name ILIKE %(search_term)s OR sm.venue ILIKE %(search_term)s)")
            values["search_term"] = f"%{search_term}%"

        # Event code filtering
        if tm_event_code:
            conditions.append("sm.tm_event_code = %(tm_event_code)s")
            values["tm_event_code"] = tm_event_code
            
        if td_event_id:
            conditions.append("sm.td_event_id = %(td_event_id)s")
            values["td_event_id"] = td_event_id

        # Section filtering
        if tm_section:
            conditions.append("sm.tm_section = %(tm_section)s")
            values["tm_section"] = tm_section
            
        if td_section:
            conditions.append("sm.td_section = %(td_section)s")
            values["td_section"] = td_section

        # Quantity filtering
        if tm_quantity_min is not None:
            conditions.append("sm.tm_quantity >= %(tm_quantity_min)s")
            values["tm_quantity_min"] = tm_quantity_min
            
        if tm_quantity_max is not None:
            conditions.append("sm.tm_quantity <= %(tm_quantity_max)s")
            values["tm_quantity_max"] = tm_quantity_max
            
        if td_quantity_min is not None:
            conditions.append("sm.td_quantity >= %(td_quantity_min)s")
            values["td_quantity_min"] = td_quantity_min
            
        if td_quantity_max is not None:
            conditions.append("sm.td_quantity <= %(td_quantity_max)s")
            values["td_quantity_max"] = td_quantity_max

        # Capacity filtering
        if tm_section_capacity_min is not None:
            conditions.append("sm.tm_capacity >= %(tm_section_capacity_min)s")
            values["tm_section_capacity_min"] = tm_section_capacity_min
            
        if tm_section_capacity_max is not None:
            conditions.append("sm.tm_capacity <= %(tm_section_capacity_max)s")
            values["tm_section_capacity_max"] = tm_section_capacity_max
            
        if td_section_capacity_min is not None:
            conditions.append("sm.td_capacity >= %(td_section_capacity_min)s")
            values["td_section_capacity_min"] = td_section_capacity_min
            
        if td_section_capacity_max is not None:
            conditions.append("sm.td_capacity <= %(td_section_capacity_max)s")
            values["td_section_capacity_max"] = td_section_capacity_max

        # Percent remaining filtering
        if tm_percent_remaining_section_min is not None:
            conditions.append("sm.tm_percent_remaining_section >= %(tm_percent_remaining_section_min)s")
            values["tm_percent_remaining_section_min"] = tm_percent_remaining_section_min
            
        if tm_percent_remaining_section_max is not None:
            conditions.append("sm.tm_percent_remaining_section <= %(tm_percent_remaining_section_max)s")
            values["tm_percent_remaining_section_max"] = tm_percent_remaining_section_max
            
        if td_percent_remaining_section_min is not None:
            conditions.append("sm.td_percent_remaining_section >= %(td_percent_remaining_section_min)s")
            values["td_percent_remaining_section_min"] = td_percent_remaining_section_min
            
        if td_percent_remaining_section_max is not None:
            conditions.append("sm.td_percent_remaining_section <= %(td_percent_remaining_section_max)s")
            values["td_percent_remaining_section_max"] = td_percent_remaining_section_max
        
        if tm_total_quantity_min is not None:
            conditions.append("sm.tm_total_quantity >= %(tm_total_quantity_min)s")
            values["tm_total_quantity_min"] = tm_total_quantity_min
            
        if tm_total_quantity_max is not None:
            conditions.append("sm.tm_total_quantity <= %(tm_total_quantity_max)s")
            values["tm_total_quantity_max"] = tm_total_quantity_max
        
        # Capacity filtering
        if tm_total_capacity_min is not None:
            conditions.append("sm.tm_total_capacity >= %(tm_total_capacity_min)s")
            values["tm_total_capacity_min"] = tm_total_capacity_min
            
        if tm_total_capacity_max is not None:
            conditions.append("sm.tm_total_capacity <= %(tm_total_capacity_max)s")
            values["tm_total_capacity_max"] = tm_total_capacity_max
        
        # Percent remaining filtering
        if tm_total_percent_remaining_min is not None:
            conditions.append("sm.tm_total_percent_remaining >= %(tm_total_percent_remaining_min)s")
            values["tm_total_percent_remaining_min"] = tm_total_percent_remaining_min
            
        if tm_total_percent_remaining_max is not None:
            conditions.append("sm.tm_total_percent_remaining <= %(tm_total_percent_remaining_max)s")
            values["tm_total_percent_remaining_max"] = tm_total_percent_remaining_max
            
        # Section getin filtering
        if tm_section_getin_min is not None:
            conditions.append("sm.tm_min_price_this_section >= %(tm_section_getin_min)s")
            values["tm_section_getin_min"] = tm_section_getin_min
            
        if tm_section_getin_max is not None:
            conditions.append("sm.tm_min_price_this_section <= %(tm_section_getin_max)s")
            values["tm_section_getin_max"] = tm_section_getin_max
        
        if td_section_getin_min is not None:
            conditions.append("sm.td_min_price_this_section >= %(td_section_getin_min)s")
            values["td_section_getin_min"] = td_section_getin_min
            
        if td_section_getin_max is not None:
            conditions.append("sm.td_min_price_this_section <= %(td_section_getin_max)s")
            values["td_section_getin_max"] = tm_section_getin_max
        
        if tm_section_has_resale is not None:
            conditions.append("sm.tm_section_has_resale = %(tm_section_has_resale)s")
            values["tm_section_has_resale"] = tm_section_has_resale
        
        # Predicted sellout filtering
        if predicted_section_sellout_start_date and predicted_section_sellout_end_date:
            predicted_section_sellout_start_date = datetime.strptime(predicted_section_sellout_start_date, "%Y-%m-%d").date()
            predicted_section_sellout_end_date = datetime.strptime(predicted_section_sellout_end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', sm.predicted_section_sell_out_date) >= %(predicted_start)s")
            conditions.append("DATE_TRUNC('DAY', sm.predicted_section_sell_out_date) <= %(predicted_end)s")
            values["predicted_start"] = predicted_section_sellout_start_date
            values["predicted_end"] = predicted_section_sellout_end_date
        
        if days_to_sellout_min is not None:
            conditions.append("sm.days_to_sellout_date >= %(days_to_sellout_min)s")
            values["days_to_sellout_min"] = days_to_sellout_min
            
        if days_to_sellout_max is not None:
            conditions.append("sm.days_to_sellout_date <= %(days_to_sellout_max)s")
            values["days_to_sellout_max"] = days_to_sellout_max
            
        # Confidence filtering
        if section_sellout_confidence_min is not None:
            conditions.append("sm.confidence >= %(confidence_min)s")
            values["confidence_min"] = section_sellout_confidence_min
            
        if section_sellout_confidence_max is not None:
            conditions.append("sm.confidence <= %(confidence_max)s")
            values["confidence_max"] = section_sellout_confidence_max

        # Velocity filtering
        if section_velocity_min is not None:
            conditions.append("sm.predicted_section_velocity >= %(velocity_min)s")
            values["velocity_min"] = section_velocity_min
            
        if section_velocity_max is not None:
            conditions.append("sm.predicted_section_velocity <= %(velocity_max)s")
            values["velocity_max"] = section_velocity_max

        if source_name:
            conditions.append("sm.source = %(source_name)s")
            values["source_name"] = source_name

        if review_event_codes is not None and len(review_event_codes) > 0:
            event_code_keys = [f"event_code_{i}" for i in range(len(review_event_codes))]
            placeholders = ', '.join([f"%({key})s" for key in event_code_keys])
            conditions.append(f"sm.tm_event_code IN ({placeholders})")

            for key, val in zip(event_code_keys, review_event_codes):
                values[key] = val

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        base_sql = f"""
            SELECT 
                tm_event_code,
                td_event_id,
                event_name,
                start_date,
                venue,
                tm_section,
                tm_quantity,
                tm_capacity,
                tm_percent_remaining_section,
                tm_total_quantity,
                tm_total_capacity,
                tm_total_percent_remaining,
                tm_min_price_this_section,
                tm_section_has_resale,
                td_section,
                td_quantity,
                td_capacity,
                td_percent_remaining_section,
                td_min_price_this_section,
                predicted_section_sell_out_date,
                confidence,
                predicted_section_velocity,
                days_to_sellout_date,
                source
            FROM TICKETBOAT_JZAFAR.PUBLIC.ATOZ_SECTION_MAPPING sm
            {where_clause}
        """
        valid_sort_fields = {
            "start_date": "start_date",
            "tm_event_code":"tm_event_code",
            "td_event_id":"td_event_id",
            "tm_section": "tm_section",
            "td_section": "td_section",
            "tm_quantity": "tm_quantity",
            "td_quantity": "td_quantity",
            "tm_capacity": "tm_capacity",
            "td_capacity": "td_capacity",
            "tm_total_quantity": "tm_total_quantity",
            "tm_total_capacity": "tm_total_capacity",
            "predicted_section_sell_out_date": "predicted_section_sell_out_date",
            "confidence": "confidence",
            "predicted_section_velocity": "predicted_section_velocity",
            "days_to_sellout_date":"days_to_sellout_date",
            "tm_percent_remaining_section": "tm_percent_remaining_section",
            "td_percent_remaining_section": "td_percent_remaining_section",
            "tm_total_percent_remaining": "tm_total_percent_remaining",
            "tm_min_price_this_section": "tm_min_price_this_section",
            "td_min_price_this_section": "td_min_price_this_section",
            "tm_section_has_resale":"tm_section_has_resale",
            "venue": "venue",
            "event_name": "event_name",
            "source" : "source"
        }
        sort_by = sort_by.lower()
        if sort_by not in valid_sort_fields:
            sort_by = "start_date"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "asc"
        order_by_clause = f"ORDER BY {sort_by} {sort_order}"

        count_query = f"""
            WITH base_cte AS ({base_sql})
            SELECT COUNT(*) AS total
            FROM base_cte
        """
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_query, values)
            total = cur.fetchone()["TOTAL"]
            # Get paginated data
            if page_size is not None and page is not None:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                    LIMIT %(page_size)s OFFSET %(offset)s
                """
                values["page_size"] = page_size
                values["offset"] = (page - 1) * page_size
            else:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                """
            cur.execute(data_query, values)

            results = cur.fetchall()
            return {
                "items": [dict(r) for r in results],
                "total": total,  # Total number of records
                "page": page,
                "page_size": page_size
            }
    except Exception as e:
        print(f"Error in get_section_mapping: {str(e)}")
        return {"error": str(e)}

async def insert_custom_view(payload: CustomViewPayload) -> CustomViewResponse:
    try:
        query = """
            INSERT INTO atoz_custom_views (
                username,
                view_name,
                filters,
                days_range
            )
            VALUES (:username, :view_name, :filters, :days_range)
            RETURNING id, username, view_name, filters, days_range, created_at
        """

        values = {
            "username": payload.username,
            "view_name": payload.view_name,
            "filters": json.dumps(payload.filters),
            "days_range": payload.days_range
        }

        row = await get_pg_database().fetch_one(query=query, values=values)
        if row is None:
            raise HTTPException(status_code=500, detail="Insert failed: no row returned.")

        return CustomViewResponse(
            id=str(row["id"]),
            username=row["username"],
            view_name=row["view_name"],
            filters=json.loads(row["filters"]) if isinstance(row["filters"], str) else row["filters"],
            days_range=row["days_range"],
            created_at=row["created_at"]
        )
    except Exception as e:
        print(f"Error in create custom view: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while inserting custom view: {str(e)}")

async def update_custom_view(view_id: str, payload: CustomViewPayload) -> CustomViewResponse:
    try:
        # First check if the view exists and get current data
        check_query = """
            SELECT username FROM atoz_custom_views
            WHERE id = :view_id
        """
        
        current_view = await get_pg_database().fetch_one(query=check_query, values={"view_id": view_id})
        if current_view is None:
            raise HTTPException(status_code=404, detail="Custom view not found.")
        
        # Check if the current user is the owner of the view
        if current_view["username"] != payload.username:
            raise HTTPException(status_code=403, detail="You can only update views that you created.")

        query = """
            UPDATE atoz_custom_views
            SET username = :username,
                view_name = :view_name,
                filters = :filters,
                days_range = :days_range
            WHERE id = :view_id
            RETURNING id, username, view_name, filters, days_range, created_at
        """

        values = {
            "view_id": view_id,
            "username": payload.username,
            "view_name": payload.view_name,
            "filters": json.dumps(payload.filters),
            "days_range": payload.days_range
        }

        row = await get_pg_database().fetch_one(query=query, values=values)
        if row is None:
            raise HTTPException(status_code=404, detail="Custom view not found.")

        return CustomViewResponse(
            id=str(row["id"]),
            username=row["username"],
            view_name=row["view_name"],
            filters=json.loads(row["filters"]) if isinstance(row["filters"], str) else row["filters"],
            days_range=row["days_range"],
            created_at=row["created_at"]
        )
    except Exception as e:
        print(f"Error in update custom view: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while updating custom view: {str(e)}")

async def get_all_custom_views() -> List[CustomViewResponse]:
    try:
        query = """
            SELECT id, username, view_name, filters, days_range, created_at
            FROM atoz_custom_views
            ORDER BY created_at DESC
        """
        rows = await get_pg_database().fetch_all(query)
        return [
            CustomViewResponse(
                id=str(row["id"]),
                username=row["username"],
                view_name=row["view_name"],
                filters=json.loads(row["filters"]) if isinstance(row["filters"], str) else row["filters"],
                days_range=row["days_range"],
                created_at=row["created_at"]
            )
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while retrieving custom views: {e}")

async def delete_custom_view(payload: DeleteCustomViewPayload) -> Dict[str, str]:
    try:
        query = """
            DELETE FROM atoz_custom_views
            WHERE id = :id
        """
        values = {
            "id": str(payload.id)
        }

        result = await get_pg_database().execute(query=query, values=values)

        if result == 0:
            raise HTTPException(status_code=404, detail="Custom view not found or not authorized to delete.")

        return {"message": "Custom view deleted successfully."}

    except Exception as e:
        print(f"Error deleting custom view: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while deleting the custom view.")


async def get_price_break(
    search_term: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    event_code: Optional[str] = None,
    event_name: Optional[str] = None,
    section: Optional[str] = None,
    offer_code: Optional[str] = None,
    venue: Optional[str] = None,
    city: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    td_price_bracket: Optional[str] = None,
    tm_quantity_min: Optional[int] = None,
    tm_quantity_max: Optional[int] = None,
    td_quantity_min: Optional[int] = None,
    td_quantity_max: Optional[int] = None,
    total_price_min: Optional[float] = None,
    total_price_max: Optional[float] = None,
    offer_predicted_sellout_start_date: Optional[str] = None,
    offer_predicted_sellout_end_date: Optional[str] = None,
    offer_sellout_confidence_min: Optional[float] = None,
    offer_sellout_confidence_max: Optional[float] = None,
    predicted_velocity_min: Optional[float] = None,
    predicted_velocity_max: Optional[float] = None,
    days_to_sellout_min: Optional[int] = None,
    days_to_sellout_max: Optional[int] = None,
    percent_tickets_remaining_min: Optional[float] = None,
    percent_tickets_remaining_max: Optional[float] = None,
    sort_by: Optional[str] = "start_date",
    sort_order: Optional[str] = "desc",
    review_event_codes: Optional[List[str]] = None
):
    try:
        values = {}
        conditions = []
        

        # Search filtering
        if search_term:
            conditions.append("(sm.event_name ILIKE %(search_term)s OR sm.venue ILIKE %(search_term)s)")
            values["search_term"] = f"%{search_term}%"

        # Event code filtering
        if event_code:
            conditions.append("sm.event_code = %(event_code)s")
            values["event_code"] = event_code
            
        if event_name:
            conditions.append("sm.event_name = %(event_name)s")
            values["event_name"] = event_name

        if section:
            conditions.append("sm.section = %(section)s")
            values["section"] = section

        if venue:
            conditions.append("sm.venue = %(venue)s")
            values["venue"] = venue
            
        if city:
            conditions.append("sm.city = %(city)s")
            values["city"] = city
        
        if start_date and end_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', sm.start_date) >= %(start_date)s")
            conditions.append("DATE_TRUNC('DAY', sm.start_date) <= %(end_date)s")
            values["start_date"] = start_date
            values["end_date"] = end_date
        
        if offer_code:
            conditions.append("sm.offer_code = %(offer_code)s")
            values["offer_code"] = offer_code
            
        if td_price_bracket:
            conditions.append("sm.td_price_bracket = %(td_price_bracket)s")
            values["td_price_bracket"] = td_price_bracket

        # Quantity filtering
        if tm_quantity_min is not None:
            conditions.append("sm.tm_quantity >= %(tm_quantity_min)s")
            values["tm_quantity_min"] = tm_quantity_min
            
        if tm_quantity_max is not None:
            conditions.append("sm.tm_quantity <= %(tm_quantity_max)s")
            values["tm_quantity_max"] = tm_quantity_max
            
        if td_quantity_min is not None:
            conditions.append("sm.td_quantity >= %(td_quantity_min)s")
            values["td_quantity_min"] = td_quantity_min
            
        if td_quantity_max is not None:
            conditions.append("sm.td_quantity <= %(td_quantity_max)s")
            values["td_quantity_max"] = td_quantity_max

        # Total price filtering
        if total_price_min is not None:
            conditions.append("sm.total_price >= %(total_price_min)s")
            values["total_price_min"] = total_price_min
            
        if total_price_max is not None:
            conditions.append("sm.total_price <= %(total_price_max)s")
            values["total_price_max"] = total_price_max
        
        # Predicted sellout filtering
        if offer_predicted_sellout_start_date and offer_predicted_sellout_end_date:
            offer_predicted_sellout_start_date = datetime.strptime(offer_predicted_sellout_start_date, "%Y-%m-%d").date()
            offer_predicted_sellout_end_date = datetime.strptime(offer_predicted_sellout_end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', sm.offer_predicted_sellout_date) >= %(predicted_start)s")
            conditions.append("DATE_TRUNC('DAY', sm.offer_predicted_sellout_date) <= %(predicted_end)s")
            values["predicted_start"] = offer_predicted_sellout_start_date
            values["predicted_end"] = offer_predicted_sellout_end_date
            
        # Confidence filtering
        if offer_sellout_confidence_min is not None:
            conditions.append("sm.offer_sellout_confidence >= %(offer_sellout_confidence_min)s")
            values["offer_sellout_confidence_min"] = offer_sellout_confidence_min
            
        if offer_sellout_confidence_max is not None:
            conditions.append("sm.offer_sellout_confidence <= %(offer_sellout_confidence_max)s")
            values["offer_sellout_confidence_max"] = offer_sellout_confidence_max

        if predicted_velocity_min is not None:
            conditions.append("sm.predicted_velocity >= %(predicted_velocity_min)s")
            values["predicted_velocity_min"] = predicted_velocity_min
            
        if predicted_velocity_max is not None:
            conditions.append("sm.predicted_velocity <= %(predicted_velocity_max)s")
            values["predicted_velocity_max"] = predicted_velocity_max

        if days_to_sellout_min is not None:
            conditions.append("sm.days_to_sellout_date >= %(days_to_sellout_min)s")
            values["days_to_sellout_min"] = days_to_sellout_min
            
        if days_to_sellout_max is not None:
            conditions.append("sm.days_to_sellout_date <= %(days_to_sellout_max)s")
            values["days_to_sellout_max"] = days_to_sellout_max

        if percent_tickets_remaining_min is not None:
            conditions.append("sm.percent_tickets_remaining >= %(percent_tickets_remaining_min)s")
            values["percent_tickets_remaining_min"] = percent_tickets_remaining_min
            
        if percent_tickets_remaining_max is not None:
            conditions.append("sm.percent_tickets_remaining <= %(percent_tickets_remaining_max)s")
            values["percent_tickets_remaining_max"] = percent_tickets_remaining_max
            
        if review_event_codes is not None and len(review_event_codes) > 0:
            event_code_keys = [f"event_code_{i}" for i in range(len(review_event_codes))]
            placeholders = ', '.join([f"%({key})s" for key in event_code_keys])
            conditions.append(f"sm.tm_event_code IN ({placeholders})")

            for key, val in zip(event_code_keys, review_event_codes):
                values[key] = val

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        base_sql = f"""
            SELECT 
                EVENT_CODE,
                EVENT_NAME,
                SECTION,
                OFFER_CODE,
                VENUE,
                CITY,
                START_DATE,
                TOTAL_PRICE,
                TM_QUANTITY,
                TD_PRICE_BRACKET,
                TD_QUANTITY,
                OFFER_SELLOUT_CONFIDENCE,
                OFFER_PREDICTED_SELLOUT_DATE,
                PREDICTED_VELOCITY,
                DAYS_TO_SELLOUT_DATE,
                PERCENT_TICKETS_REMAINING
            FROM TICKETBOAT_STAGING.PUBLIC.ATOZ_PRICE_BREAK_DT sm
            {where_clause}
        """
        valid_sort_fields = {
            "event_code":"event_code",
            "event_name":"event_name",
            "section":"section",
            "offer_code": "offer_code",
            "venue": "venue",
            "city": "city",
            "start_date": "start_date",
            "total_price": "total_price",
            "tm_quantity": "tm_quantity",
            "td_quantity": "td_quantity",
            "td_price_bracket": "td_price_bracket",
            "offer_predicted_sellout_date": "offer_predicted_sellout_date",
            "offer_sellout_confidence": "offer_sellout_confidence",
            "predicted_velocity": "predicted_velocity",
            "days_to_sellout_date":"days_to_sellout_date",
            "percent_tickets_remaining": "percent_tickets_remaining"
        }
        sort_by = sort_by.lower()
        if sort_by not in valid_sort_fields:
            sort_by = "start_date"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "asc"
        order_by_clause = f"ORDER BY {sort_by} {sort_order}"

        count_query = f"""
            WITH base_cte AS ({base_sql})
            SELECT COUNT(*) AS total
            FROM base_cte
        """
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_query, values)
            total = cur.fetchone()["TOTAL"]
            # Get paginated data
            if page_size is not None and page is not None:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                    LIMIT %(page_size)s OFFSET %(offset)s
                """
                values["page_size"] = page_size
                values["offset"] = (page - 1) * page_size
            else:
                data_query = f"""
                    {base_sql}
                    {order_by_clause}
                """
            cur.execute(data_query, values)

            results = cur.fetchall()
            return {
                "items": [dict(r) for r in results],
                "total": total,  # Total number of records
                "page": page,
                "page_size": page_size
            }
    except Exception as e:
        print(f"Error in get_price_break: {str(e)}")
        return {"error": str(e)}
        
        
async def post_pricebreak_review_status(payload: dict) -> dict:

    create_table_query = """
        CREATE TABLE IF NOT EXISTS atoz_pricebreak_review_status (
            event_code TEXT PRIMARY KEY,
            review_status TEXT,
            reviewed_by TEXT,
            section TEXT,
            pricebreak TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """
    await get_pg_database().execute(query=create_table_query)


    if payload.get("review_status") is None:
        query = """
            UPDATE atoz_pricebreak_review_status
            SET review_status = '',
                reviewed_by = :reviewed_by,
                section = :section,
                pricebreak = :pricebreak,
                updated_at = NOW()
            WHERE event_code = :event_code
        """
    else:
        query = """
            INSERT INTO atoz_pricebreak_review_status (
                event_code, review_status, reviewed_by, section, pricebreak, created_at, updated_at
            )
            VALUES (
                :event_code, :review_status, :reviewed_by, :section, :pricebreak, NOW(), NOW()
            )
            ON CONFLICT (event_code)
            DO UPDATE SET
                review_status = EXCLUDED.review_status,
                reviewed_by = EXCLUDED.reviewed_by,
                section = EXCLUDED.section,
                pricebreak = EXCLUDED.pricebreak,
                updated_at = NOW()
        """

    await get_pg_database().execute(query=query, values=payload)
    return {"message": "Pricebreak review status table ensured and data updated"}
