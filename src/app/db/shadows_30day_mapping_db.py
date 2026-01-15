import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_viagogo_event_mapping import ShadowsViagogoUnmappedEventsModel, ShadowsViagogoMappedEventsModel


async def get_thirty_day_mapped_events(start_date: str, end_date: str, page: int, page_size: int) -> Dict[str, Any]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            offset = (page - 1) * page_size
            if start_date and end_date:
                cur.execute("""
                    select
                        COUNT(*) OVER() AS total_count,
                        'Ticketmaster' as primary,
                        t.*
                    from ticketmaster_viagogo_event_mapping_v as t
                    where datetime_added BETWEEN %(start_date)s AND %(end_date)s
                        limit %(page_size)s offset %(offset)s
                """, {"start_date": start_date, "end_date": end_date, "page_size": page_size, "offset": offset})
                results = cur.fetchall()
                items = []
                for result in results:
                    normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                    items.append(ShadowsViagogoMappedEventsModel(**normalized_data))
                total_count = results[len(results) - 1]['TOTAL_COUNT'] # type: ignore
                return {"items": items, "count": total_count}
            else:
                cur.execute("""
                    select
                        COUNT(*) OVER() AS total_count,
                        'Ticketmaster' as primary,
                        t.*
                    from ticketmaster_viagogo_event_mapping_v as t
                    where datetime_added <= CURRENT_TIMESTAMP()
                    and datetime_added > DATEADD('DAY', -30, CURRENT_TIMESTAMP())
                        limit %(page_size)s offset %(offset)s
                """, {"page_size": page_size, "offset": offset})
                results = cur.fetchall()
                items = []
                for result in results:
                    normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                    items.append(ShadowsViagogoMappedEventsModel(**normalized_data))
                total_count = results[len(results) - 1]['TOTAL_COUNT'] # type: ignore
                return {"items": items, "count": total_count}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_thirty_day_unmapped_events(start_date: str, end_date: str, page: int, page_size: int) -> Dict[str, Any]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            offset = (page - 1) * page_size
            if start_date and end_date:
                cur.execute("""
                    select
                        COUNT(*) OVER() AS total_count,
                        'Ticketmaster' as primary,
                        t.*
                    from ticketmaster_unmapped_events_v t
                    where datetime_added BETWEEN %(start_date)s AND %(end_date)s
                        limit %(page_size)s offset %(offset)s
                """, {"start_date": start_date, "end_date": end_date, "page_size": page_size, "offset": offset})
                results = cur.fetchall()
                items = []
                if results:
                    for result in results:
                        normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                        items.append(ShadowsViagogoUnmappedEventsModel(**normalized_data))
                    total_count = results[len(results) - 1]['TOTAL_COUNT'] # type: ignore
                    return {"items": items, "count": total_count}
                else:
                    return {"items": items, "count": 0}
            else:
                cur.execute("""
                    select
                        COUNT(*) OVER() AS total_count,
                        'Ticketmaster' as primary,
                        t.*
                    from ticketmaster_unmapped_events_v t
                    where datetime_added > DATEADD('DAY', -30, CURRENT_TIMESTAMP())
                        limit %(page_size)s offset %(offset)s
                """, {"page_size": page_size, "offset": offset})
                results = cur.fetchall()
                items = []
                if results:
                    for result in results:
                        normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                        items.append(ShadowsViagogoUnmappedEventsModel(**normalized_data))
                    total_count = results[len(results) - 1]['TOTAL_COUNT'] # type: ignore
                    return {"items": items, "count": total_count}
                else:
                    return {"items": items, "count": 0}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
