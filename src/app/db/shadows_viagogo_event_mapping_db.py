import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_viagogo_event_mapping import *
from app.model.shadows_user_tracker import *
from app.db.shadows_user_tracker import create_user_tracker_entry
from app.model.user import User
from datetime import datetime


async def get_viagogo_unmapped_events(page: int, page_size: int) -> List[ShadowsViagogoUnmappedEventsModel]:
    try:
        offset = (page - 1) * page_size
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                select 
                    event_name,
                    start_date,
                    datetime_added,
                    datetime_updated,
                    venue,
                    event_code,
                    url,
                    available_seats,
                    case 
                        when ignore is null then 'False'
                        else 'True'
                    end as ignore,
                    'Ticketmaster' as primary
                from ticketmaster_unmapped_events_v
                        limit %(page_size)s offset %(offset)s
            """, {"page_size": page_size, "offset": offset})
            print(cur.query)
            results = cur.fetchall()
            print(results)
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsViagogoUnmappedEventsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
def create_mapped_sql_query(payload: ShadowsViagogoSearchMappedEventModel) -> str:
    print(payload.model_dump())
    query = "select 'Ticketmaster' as primary, * from ticketmaster_viagogo_event_mapping_v "

    if payload.event_name is not None:
        query += f" where viagogo_event_id is not null and event_name ilike '%{payload.event_name}%'"
    if payload.ticketmaster_event_code is not None:
        query += f" where viagogo_event_id is not null and ticketmaster_event_code = '{payload.ticketmaster_event_code}'"

    return query

async def get_viagogo_mapped_events(payload: ShadowsViagogoSearchMappedEventModel) -> List[ShadowsViagogoMappedEventsModel]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = create_mapped_sql_query(payload)
            print(sql)
            cur.execute(sql)
            results = cur.fetchall()
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsViagogoMappedEventsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def update_viagogo_mapping(payload: ShadowsUpdateEventModel, user: User):
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        try:
            cur.execute("""
                update ticketmaster_viagogo_event_mapping
                    set 
                        viagogo_event_id = %(viagogo_event_id)s,
                        datetime_updated = %(datetime_updated)s
                where ticketmaster_event_code = %(ticketmaster_event_code)s
            """, payload.model_dump())
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(e))
        else:
            data = payload.model_dump_json()
            user_tracker = ShadowsUserTrackerModel(**{
                "operation": "update",
                "module": "viagogo_event_mapping_update_event_id",
                "user": user.name,
                "data": data
            })
            await create_user_tracker_entry(user_tracker)
            return payload.model_dump()

async def update_ignore_mapping(payload: ShadowsUpdateIgnoreModel, user: User):
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        try:
            if payload.ignore == 'True':
                cur.execute("""
                    update ticketmaster_viagogo_event_mapping
                        set 
                            ignore = %(ignore)s,
                            datetime_updated = %(datetime_updated)s
                    where ticketmaster_event_code = %(ticketmaster_event_code)s
                """, payload.model_dump())
            else:
                cur.execute("""
                    update ticketmaster_viagogo_event_mapping
                        set 
                            ignore = NULL,
                            datetime_updated = %(datetime_updated)s
                    where ticketmaster_event_code = %(ticketmaster_event_code)s
                """, payload.model_dump())
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(e))
        else:
            data = payload.model_dump_json()
            user_tracker = ShadowsUserTrackerModel(**{
                "operation": "update",
                "module": "viagogo_event_mapping_update_ignore",
                "user": user.name,
                "data": data
            })
            await create_user_tracker_entry(user_tracker)
            return payload.model_dump()

async def remove_viagogo_mapping_event(payload: ShadowsRemoveEventModel, user: User):
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        try:
            cur.execute("""
                update ticketmaster_viagogo_event_mapping
                    set 
                        viagogo_event_id = NULL,
                        datetime_updated = %(datetime_updated)s
                where ticketmaster_event_code = %(ticketmaster_event_code)s
            """, payload.model_dump())
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=str(e))
        else:
            data = payload.model_dump_json()
            user_tracker = ShadowsUserTrackerModel(**{
                "operation": "update",
                "module": "viagogo_event_mapping_remove_event_id",
                "user": user.name,
                "data": data
            })
            await create_user_tracker_entry(user_tracker)
            return payload.model_dump()
