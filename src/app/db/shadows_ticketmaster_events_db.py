import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_ticketmaster import (
    ShadowsTicketmasterEvents,
    ShadowsTicketmasterSeating,
    ShadowsTicketmasterSearchQuery
)


async def get_items(page: int, page_size: int) -> List[ShadowsTicketmasterEvents]:
    try:
        offset = (page - 1) * page_size
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select 
                    t2.*,
                    case when tpo.price_override is null then 0 else tpo.price_override end price_override,
                    tpo.created_at as tpo_created_at
                from ticketmaster2 t2 
                left join ticketmaster_pricing_override tpo
                on tpo.id = t2.id
                where t2.status = 'onsale' 
                and t2.is_cancelled = 0
                and t2.is_sold_out = 0
                and t2.start_date > current_timestamp
                limit %(page_size)s offset %(offset)s
            """
            cur.execute(sql, {"page_size": page_size, "offset": offset})
            results = cur.fetchall()
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                results_list.append(ShadowsTicketmasterEvents(**normalized_data))
        return results_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def create_sql_query(data: ShadowsTicketmasterSearchQuery) -> str:
    conditions = []

    for key, value in data.model_dump(exclude_none=True).items():
        if value:
            if key == "start_date":
                conditions.append(f"CAST(start_date AS TEXT) LIKE '%{value}%'")
            else:
                conditions.append(f"t2.{key} ILIKE '%{value}%'")

    where_clause = " AND ".join(conditions)
    
    sql_query = """
        select
            t2.*,
            case when tpo.price_override is null then 0 else tpo.price_override end price_override,
            tpo.created_at as tpo_created_at
        from ticketmaster2 t2 
        left join ticketmaster_pricing_override tpo
        on tpo.id = t2.id
    """
    if where_clause:
        sql_query += f""" WHERE {where_clause} and t2.status = 'onsale' 
                and t2.is_cancelled = 0
                and t2.is_sold_out = 0
                and t2.start_date > current_timestamp"""

    return sql_query

async def search(payload: ShadowsTicketmasterSearchQuery) -> List[ShadowsTicketmasterEvents]:
    sql = create_sql_query(payload)
    try:
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql)
            results = cur.fetchall()
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                results_list.append(ShadowsTicketmasterEvents(**normalized_data))
        return results_list
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_details(event_code: str) -> List[ShadowsTicketmasterSeating]:
    try:
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select 
                    id,
                    event_code,
                    event_ticketnumber,
                    offer_code,
                    section,
                    "row",
                    seats,
                    seat_from,
                    seat_to,
                    quantity,
                    updated_at
                from ticketmaster_seating2_current
                where event_code = %(event_code)s
            """
            cur.execute(sql, {"event_code": event_code})
            results = cur.fetchall()
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                results_list.append(ShadowsTicketmasterSeating(**normalized_data))
        return results_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
