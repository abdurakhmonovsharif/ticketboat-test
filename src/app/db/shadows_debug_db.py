import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection

async def get_viagogo_change_history_stats() -> List[Dict[str, Any]]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select date_trunc('hour', db_created_at) hr
                      ,count(distinct viagogo_account_id) num_accounts
                      ,count(case when status = 'create_success' then 1 end)-count(distinct case when status = 'delete_success' then 1 end) net_create_success
                      ,count(case when status = 'create_success' then 1 end) create_success
                      ,count(case when status = 'create_failure' then 1 end) create_failure
                      ,count(case when status = 'update_success' then 1 end) update_success
                      ,count(case when status = 'update_failure' then 1 end) update_failure
                      ,count(case when status = 'delete_success' then 1 end) delete_success
                      ,count(case when status = 'delete_failure' then 1 end) delete_failure
                      ,min(error_message) min_error_message
                      ,max(error_message) max_error_message
                from viagogo_change_history
                where db_created_at >= current_date-1
                  and source = 'ticketmaster'
                group by 1
                order by 1 asc
            """
            cur.execute(sql)
            results = cur.fetchall()
            # Convert to list of dictionaries with lowercase keys
            return [
                {
                    'hr': row['HR'],
                    'num_accounts': row['NUM_ACCOUNTS'],
                    'net_create_success': row['NET_CREATE_SUCCESS'],
                    'create_success': row['CREATE_SUCCESS'],
                    'create_failure': row['CREATE_FAILURE'],
                    'update_success': row['UPDATE_SUCCESS'],
                    'update_failure': row['UPDATE_FAILURE'],
                    'delete_success': row['DELETE_SUCCESS'],
                    'delete_failure': row['DELETE_FAILURE'],
                    'min_error_message': row['MIN_ERROR_MESSAGE'],
                    'max_error_message': row['MAX_ERROR_MESSAGE']
                }
                for row in results
            ]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_viagogo_sale_by_order_id(order_id: str) -> Dict[str, Any]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select created_at
                      ,event_name viagogo_event_name
                      ,start_date viagogo_start_date
                      ,venue viagogo_venue
                      ,external_listing_id
                      ,number_of_tickets
                      ,section
                      ,"ROW"
                      ,viagogo_event_id
                from viagogo_sales
                where id = %s
            """
            cur.execute(sql, (order_id,))
            result = cur.fetchone()
            if not result:
                return {}
            return {
                'created_at': result['CREATED_AT'],
                'viagogo_event_name': result['VIAGOGO_EVENT_NAME'],
                'viagogo_start_date': result['VIAGOGO_START_DATE'],
                'viagogo_venue': result['VIAGOGO_VENUE'],
                'external_listing_id': result['EXTERNAL_LISTING_ID'],
                'number_of_tickets': result['NUMBER_OF_TICKETS'],
                'section': result['SECTION'],
                'row': result['ROW'],
                'viagogo_event_id': result['VIAGOGO_EVENT_ID']
            }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_viagogo_sales_by_location_id(location_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all viagogo sales for a given location_id (external_listing_id starts with location_id)
    """
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select created_at
                      ,event_name viagogo_event_name
                      ,start_date viagogo_start_date
                      ,venue viagogo_venue
                      ,external_listing_id
                      ,number_of_tickets
                      ,section
                      ,"ROW"
                      ,viagogo_event_id
                from viagogo_sales
                where external_listing_id like %s
            """
            like_pattern = f"{location_id}%"
            cur.execute(sql, (like_pattern,))
            results = cur.fetchall()
            return [
                {
                    'created_at': row['CREATED_AT'],
                    'viagogo_event_name': row['VIAGOGO_EVENT_NAME'],
                    'viagogo_start_date': row['VIAGOGO_START_DATE'],
                    'viagogo_venue': row['VIAGOGO_VENUE'],
                    'external_listing_id': row['EXTERNAL_LISTING_ID'],
                    'number_of_tickets': row['NUMBER_OF_TICKETS'],
                    'section': row['SECTION'],
                    'row': row['ROW'],
                    'viagogo_event_id': row['VIAGOGO_EVENT_ID']
                }
                for row in results
            ]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_viagogo_change_history(location_id: str, anchor_utc_timestamp: str, before_hours: int = 6, after_hours: int = 3) -> list[dict]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = f"""
                select h.db_created_at
                      ,h.status
                      ,h.reason_for_delete
                      ,h.orig_number_of_tickets
                      ,h.external_id
                      ,h.orig_event_code
                      ,h.price
                      ,h.orig_seating_section
                      ,h.orig_seating_row
                      ,r.seating_segment
                from viagogo_change_history h
                    left join ticketmaster_all_rows_mv r
                        on r.location_id = split_part(external_id, ';', 1)
                where external_id like %s
                  and db_created_at >= %s::timestamp - interval '{before_hours} hour'
                  and db_created_at <  %s::timestamp + interval '{after_hours} hour'
                order by db_created_at
            """
            like_pattern = f"{location_id}%"
            cur.execute(sql, (like_pattern, anchor_utc_timestamp, anchor_utc_timestamp))
            results = cur.fetchall()
            return [
                {
                    'db_created_at': row['DB_CREATED_AT'],
                    'status': row['STATUS'],
                    'reason_for_delete': row['REASON_FOR_DELETE'],
                    'orig_number_of_tickets': row['ORIG_NUMBER_OF_TICKETS'],
                    'external_id': row['EXTERNAL_ID'],
                    'orig_event_code': row['ORIG_EVENT_CODE'],
                    'price': row['PRICE'],
                    'orig_seating_section': row['ORIG_SEATING_SECTION'],
                    'orig_seating_row': row['ORIG_SEATING_ROW'],
                    'seating_segment': row['SEATING_SEGMENT']
                }
                for row in results
            ]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_ticketmaster_seating_history(event_code: str, section: str, row: str, anchor_utc_timestamp: str, before_hours: int = 6, after_hours: int = 3) -> list[dict]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            if row == "GA":
                row_condition = '(("row" = %s) OR ("row" = \'\') OR ("row" IS NULL))'
            else:
                row_condition = '"row" = %s'
            sql = f"""
                select   updated_at
                        ,event_code
                        ,event_ticketnumber
                        ,offer_code
                        ,section
                        ,"row" as "ROW"
                        ,segment
                        ,sum(quantity) as quantity
                from ticketmaster_seating2_history
                where event_code = %s
                    and section = %s
                    and {row_condition}
                    and updated_at >= %s::timestamp - interval '{before_hours} hour'
                    and updated_at <  %s::timestamp + interval '{after_hours} hour'
                group by 1, 2, 3, 4, 5, 6, 7
                order by 1
            """
            cur.execute(sql, (event_code, section, row, anchor_utc_timestamp, anchor_utc_timestamp))
            results = cur.fetchall()
            return [
                {
                    'updated_at': row['UPDATED_AT'],
                    'event_code': row['EVENT_CODE'],
                    'event_ticketnumber': row['EVENT_TICKETNUMBER'],
                    'offer_code': row['OFFER_CODE'],
                    'section': row['SECTION'],
                    'row': row['ROW'],
                    'segment': row['SEGMENT'],
                    'quantity': row['QUANTITY']
                }
                for row in results
            ]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e)) 