from typing import List, Dict, Optional, Tuple

import snowflake.connector

from app.database import get_snowflake_connection, get_pg_buylist_database


def _build_time_filters(start_time: Optional[str], end_time: Optional[str]) -> Tuple[str, list]:
    filters = []
    params: list = []
    if start_time:
        filters.append("start_time >= TO_TIMESTAMP(%s)")
        params.append(start_time)
    if end_time:
        filters.append("start_time <= TO_TIMESTAMP(%s)")
        params.append(end_time)
    return (" AND ".join(filters), params)


def fetch_query_history(
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        user_name: Optional[str] = None,
        limit: int = 1000,
) -> List[Dict]:
    base_query = (
        """
        SELECT 
            query_id,
            user_name,
            role_name,
            warehouse_name,
            database_name,
            schema_name,
            query_text,
            start_time,
            end_time,
            execution_status,
            bytes_scanned,
            rows_produced,
            total_elapsed_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE 1=1
        """
    )

    clauses: list[str] = []
    params: list = []

    time_clause, time_params = _build_time_filters(start_time, end_time)
    if time_clause:
        clauses.append(time_clause)
        params.extend(time_params)

    if user_name:
        clauses.append("LOWER(user_name) = LOWER(%s)")
        params.append(user_name)

    where_sql = (" AND " + " AND ".join(clauses)) if clauses else ""
    order_limit_sql = " ORDER BY start_time DESC LIMIT %s"
    params.append(limit)

    sql = base_query + where_sql + order_limit_sql

    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_login_history(
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        user_name: Optional[str] = None,
        limit: int = 1000,
) -> List[Dict]:
    base_query = (
        """
        SELECT 
            event_id,
            event_timestamp,
            user_name,
            directive,
            client_ip,
            reported_client_type,
            is_success AS success,
            error_code,
            error_message
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE 1=1
        """
    )

    clauses: list[str] = []
    params: list = []

    # LOGIN_HISTORY uses event_timestamp for time filtering
    if start_time:
        clauses.append("event_timestamp >= TO_TIMESTAMP(%s)")
        params.append(start_time)
    if end_time:
        clauses.append("event_timestamp <= TO_TIMESTAMP(%s)")
        params.append(end_time)
    if user_name:
        clauses.append("LOWER(user_name) = LOWER(%s)")
        params.append(user_name)

    where_sql = (" AND " + " AND ".join(clauses)) if clauses else ""
    order_limit_sql = " ORDER BY event_timestamp DESC LIMIT %s"
    params.append(limit)

    sql = base_query + where_sql + order_limit_sql

    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_logs(
        log_type: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        user_name: Optional[str] = None,
        limit: int = 1000,
) -> List[Dict]:
    if log_type.lower() == "query":
        return fetch_query_history(start_time, end_time, user_name, limit)
    if log_type.lower() == "login":
        return fetch_login_history(start_time, end_time, user_name, limit)
    raise ValueError("Unsupported log_type. Use 'query' or 'login'.")


async def fetch_viagogo_logs_by_sale_id(sale_id: str, limit: int = 100) -> List[Dict]:
    count_query = f"""
            select external_listing_id from viagogo_sales where id='{sale_id}' limit 1;
        """
    sale_data = await get_pg_buylist_database().fetch_one(count_query)
    external_id = None
    if sale_data:
        external_id = sale_data[0]
    else:
        return []
    sql = (
        f"""
        select vc.listing_id as listing_id
              ,vc.external_id as external_id
              ,vc.event_id as event_id
              ,vc.orig_event_name as event_name
              ,vc.start_date as start_date
              ,vc.venue as venue
              ,vc.section as section
              ,vc."row" as "ROW"
              ,vc.orig_event_code as orig_event_code
              ,vc.price as price
              ,'{sale_id}' as order_id
        from viagogo_change_history vc
        where vc.db_created_at >= current_date-30
        and (vc.external_id='{external_id}' or vc.orig_external_id='{external_id}')
        order by vc.db_created_at desc
        limit %s
        """
    )
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


async def fetch_gotickets_logs_by_sale_id(sale_id: str, limit: int = 100) -> List[Dict]:
    count_query = f"""
            select external_ticket_id from gotickets_sales  where id='{sale_id}' limit 1;
        """
    sale_data = await get_pg_buylist_database().fetch_one(count_query)
    external_id = None
    if sale_data:
        external_id = sale_data[0]
    else:
        return []
    sql = (
        f"""
            select vc.LISTING_ID as listing_id
                  ,vc.external_id as external_id
                  ,vc.event_id as event_id
                  ,vc.event_name as event_name
                  ,vc.start_date as start_date
                  ,vc.venue as venue
                  ,vc.section as section
                  ,vc."row" as "ROW"
                  ,vc.orig_event_code as orig_event_code
                  ,vc.price as price
                  ,'{sale_id}' as order_id
            from gotickets_change_history vc
            where vc.created_at >= current_date-30
            and (vc.external_id='{external_id}' or vc.orig_external_id='{external_id}')
            order by vc.created_at desc
            limit %s;
        """
    )
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


async def fetch_seatgeek_logs_by_sale_id(sale_id: str, limit: int = 100) -> List[Dict]:
    count_query = f"""
            select listing_id from seatgeek_sales where id='{sale_id}' limit 1;
        """
    sale_data = await get_pg_buylist_database().fetch_one(count_query)
    external_id = None
    if sale_data:
        external_id = sale_data[0]
    else:
        return []
    sql = (
        f"""
            select vc.LISTING_ID as listing_id
                  ,vc.external_id as external_id
                  ,vc.event_id as event_id
                  ,vc.event_name as event_name
                  ,vc.EVENT_START_DATE as start_date
                  ,vc.venue_name as venue
                  ,vc.section as section
                  ,vc."row" as "ROW"
                  ,vc.orig_event_code as orig_event_code
                  ,vc.cost as price
                  ,'{sale_id}' as order_id
            from seatgeek_change_history vc
            where vc.created_at >= current_date-30
            and seller_listing_id='{external_id}'
            order by vc.created_at desc
            limit %s;
        """
    )
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


async def fetch_market_logs_by_sale_id(market: str, sale_id: str, limit: int = 100) -> List[Dict]:
    market_key = market.lower()
    if market_key == "viagogo":
        return await fetch_viagogo_logs_by_sale_id(sale_id, limit)
    elif market_key == "vivid":
        raise ValueError("Unsupported market. Use one of: viagogo, seatgeek, gotickets.")
    elif market_key == "seatgeek":
        return await fetch_seatgeek_logs_by_sale_id(sale_id, limit)
    elif market_key == "gotickets":
        return await fetch_gotickets_logs_by_sale_id(sale_id, limit)
    raise ValueError("Unsupported market. Use one of: viagogo, seatgeek, gotickets.")
