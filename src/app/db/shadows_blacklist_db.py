import traceback
import snowflake.connector
import uuid
import os
import boto3
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from app.database import get_pg_realtime_catalog_database, get_snowflake_connection
from app.model.shadows_blacklist import ShadowsBlacklistModel, ShadowsDeleteBlacklistModel, ShadowsBlacklistSQSMessage
from app.model.user import User
from app.cache import invalidate_shadows_cache

VIAGOGO_DELETE_SQS_QUEUE = os.getenv("VIAGOGO_DELETE_SQS_QUEUE")

async def get_blacklist_items(
    page_size: int,
    page: int,
    search: Optional[str] = None
) -> Dict[str, Any]:
    try:
        offset = (page - 1) * page_size
        base_query = """
            SELECT 
                id,
                event_code,
                event_name,
                start_date,
                notes,
                url,
                section,
                expiration_date,
                added_by,
                market,
                created_at AT TIME ZONE 'UTC' AT TIME ZONE 'CST' AS created_at
            FROM shadows_blacklist
        """

        where_clause = ""
        values: Dict[str, Any] = {
            "limit": page_size,
            "offset": offset
        }

        if search:
            where_clause = """
                WHERE 
                    event_code ILIKE :search 
                    OR event_name ILIKE :search 
                    OR url ILIKE :search
            """
            values["search"] = f"%{search}%"

        pg_query = f"""
            {base_query}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit
            OFFSET :offset
        """

        count_query = f"""
            SELECT COUNT(*) AS total
            FROM shadows_blacklist
        """

        db = get_pg_realtime_catalog_database()

        pg_results = await db.fetch_all(pg_query, values=values)
        total_result = await db.fetch_one(count_query)

        items = [ShadowsBlacklistModel(**dict(result)) for result in pg_results]
        return {"items": items, "total": total_result.total}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting blacklist items") from e

async def get_event_ids(event_code: str, market: str) -> Dict[str, Any]:
    try:
        markets = market.split(',')
        result_dict = {}
        
        for market_name in markets:
            query = """
                select 
                    MATCHED_EVENT_ID,
                    MARKET
                FROM UNIVERSAL_TICKETMASTER_EVENT_MAPPING 
                WHERE event_code = %(event_code)s
                AND MARKET = %(market)s
                LIMIT 1
            """
            params = {
                "event_code": event_code,
                "market": market_name
            }
            
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(query, params)
                result = cur.fetchone()
                
                if result:
                    result_dict[f"{market_name}_event_id"] = result.get("MATCHED_EVENT_ID")
            
        return result_dict
            
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting event IDs") from e


async def get_snowflake_ticketmaster_data(
    id: Optional[Any], 
    event_code: Optional[Any], 
    market: Any, 
    user: User
) -> ShadowsBlacklistModel:
    """Get TM Event data from Snowflake"""
    try:
        market_str = ",".join(m.lower() for m in market)
        query = None
        query_params = {}

        event_ids = await get_event_ids(event_code, market_str)

        if id:
            query = "select * FROM ticketmaster2 WHERE id = %(id)s"
            query_params = {"id": id}
        elif event_code:
            query = "select * FROM ticketmaster2 WHERE event_code = %(event_code)s"
            query_params = {"event_code": event_code}
        else:
            raise HTTPException(status_code=400, detail="Must provide either 'id' or 'event_code'")

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, query_params)
            result = cur.fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="No items found")   

            data = {
                "id": result.get("ID"), # type: ignore
                "event_code": result.get("EVENT_CODE"), # type: ignore
                "event_name": result.get("EVENT_NAME"), # type: ignore
                "start_date": result.get("START_DATE"), # type: ignore
                "notes": "blacklist_tm_id" if id else "blacklist_event_code",
                "url": result.get("URL"), # type: ignore
                "section": None,
                "expiration_date": result.get("START_DATE"), # type: ignore
                "added_by": user.name,
                "market": market_str
            } | event_ids

            return ShadowsBlacklistModel(**data)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def get_snowflake_all_rows_data(
    blacklist_type: str,
    section: str,
    external_id: str, 
    market: List[str], 
    user: User, 
    viagogo_event_id: Optional[str] = None, 
    vivid_event_id: Optional[str] = None,
    seatgeek_event_id: Optional[str] = None,
    gotickets_event_id: Optional[str] = None
) -> ShadowsBlacklistModel:
    try:
        market_str = ",".join(m.lower() for m in market)
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(
                """
                select
                    event_id as id,
                    event_name,
                    start_date,
                    event_code,
                    url,
                    seating_section,
                    viagogo_event_id
                FROM ticketmaster_all_rows_mv
                WHERE location_id = split_part(%(external_id)s, ';', 1)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
                """,
                {"external_id": external_id}
            )
            result = cur.fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="No items found")

            viagogo_id = result.get("VIAGOGO_EVENT_ID") or viagogo_event_id # type: ignore
            vivid_id = result.get("VIVID_EVENT_ID") or vivid_event_id # type: ignore
            seatgeek_id = result.get("SEATGEEK_EVENT_ID") or seatgeek_event_id # type: ignore
            gotickets_id = result.get("GOTICKETS_EVENT_ID") or gotickets_event_id # type: ignore

            data = {
                "id": result.get("ID"),  # type: ignore
                "event_code": result.get("EVENT_CODE"), # type: ignore  
                "event_name": result.get("EVENT_NAME"),  # type: ignore
                "start_date": result.get("START_DATE"),  # type: ignore
                "url": result.get("URL"),  # type: ignore
                "expiration_date": result.get("START_DATE"),  # type: ignore
                "added_by": user.name,
                "market": market_str,
                "viagogo_event_id": viagogo_id,
                "vivid_event_id": vivid_id,
                "seatgeek_event_id": seatgeek_id,
                "gotickets_event_id": gotickets_id
            }

            if blacklist_type == 'listing_section':
                data["notes"] = "blacklist_section_code"
                data["section"] = section # type: ignore
            else:
                data["notes"] = "blacklist_listing_id"
                data["section"] = None

            return ShadowsBlacklistModel(**data)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def create_blacklist(data: ShadowsBlacklistModel) -> Dict[str, Any]:
    sql = """
        insert into shadows_blacklist (id, event_code, event_name, start_date, notes, url, section, expiration_date, added_by, market)
        values (:id, :event_code, :event_name, :start_date, :notes, :url, :section, :expiration_date, :added_by, :market)
    """
    await get_pg_realtime_catalog_database().execute(query=sql, values={
        "id": data.id,
        "event_code": data.event_code,
        "event_name": data.event_name,
        "start_date": data.start_date,
        "notes": data.notes,
        "url": data.url,
        "section": data.section,
        "expiration_date": data.expiration_date,
        "added_by": data.added_by,
        "market": data.market
    })
    return {"message": "Blacklist entry created successfully", "data": data.model_dump()}

async def send_to_snowflake(data: ShadowsBlacklistModel):
    sql = """
        insert into ticketmaster_blacklist(id, event_name, start_date, created, notes, url, section, expiration_date, added_by, market)
        values(%(id)s, %(event_name)s, %(start_date)s, current_timestamp, %(notes)s, %(url)s, %(section)s, %(expiration_date)s, %(added_by)s, %(market)s)
    """
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(sql, {
                "id": data.id,
                "event_name": data.event_name,
                "start_date": data.start_date,
                "notes": data.notes,
                "url": data.url,
                "section": data.section,
                "expiration_date": data.expiration_date,
                "added_by": data.added_by,
                "market": data.market
            })
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        invalidate_shadows_cache(f"blacklist_{data.event_code}")
    
async def delete_blacklist_snowflake(data: ShadowsDeleteBlacklistModel):
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            if data.section is not None:
                sql = """
                    delete from ticketmaster_blacklist where id = %(id)s and section = %(section)s
                """
                cur.execute(sql, {"id": data.id, "section": data.section})
            else:
                sql = """
                    delete from ticketmaster_blacklist where id = %(id)s
                """
                cur.execute(sql, {"id": data.id})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def delete_blacklist(data: ShadowsDeleteBlacklistModel):
    if data.section is not None:
        sql = """
            delete from shadows_blacklist 
            where id = :id and event_code = :event_code and notes = :notes and section = :section
            """
        result = await get_pg_realtime_catalog_database().execute(query=sql, values=data.model_dump())
        if result == 0:
            raise ValueError("No Blacklist Entry Found")
        
        return {"message": "Blacklist entry deleted successfully", "data": data.model_dump()}
    else:
        sql = """
            delete from shadows_blacklist 
            where id = :id and event_code = :event_code and notes = :notes and section is null
            """
        result = await get_pg_realtime_catalog_database().execute(query=sql, values={
            "id": data.id,
            "event_code": data.event_code,
            "notes": data.notes
        })
        if result == 0:
            raise ValueError("No Blacklist Entry Found")
    
        return {"message": "Blacklist entry deleted successfully", "data": data.model_dump()}

async def create_blacklist_log(payload: Dict[str, Any], operation: str, user: User) -> None:
    data = payload.get("data")
    sql = """
            insert into shadows_blacklist_change_log (id, operation, event_code, event_name, start_date, section, url, added_by, market)
            values (:id, :operation, :event_code, :event_name, :start_date, :section, :url, :added_by, :market)
            """
    await get_pg_realtime_catalog_database().execute(query=sql, values={
        "id": uuid.uuid4().hex,
        "operation": operation,
        "event_code": data.get("event_code", None), # type: ignore
        "event_name": data.get("event_name", None), # type: ignore
        "start_date": data.get("start_date", None), # type: ignore
        "section": data.get("section", None), # type: ignore
        "url": data.get("url", None), # type: ignore
        "added_by": user.name,
        "market": data.get("market", None) # type: ignore
    })

def format_date_str(datetime):
    return datetime.strftime("%Y-%m-%d %H:%M:%S")

def format_date_epoch(datetime):
    return int(datetime.timestamp() * 1_000_000)

def format_sqs_message(item) -> ShadowsBlacklistSQSMessage:
    market = item.get("market", "")
    market_list = market.split(',') if market else []

    event_ids = {}
    if item.get("viagogo_event_id"):
        event_ids["viagogo_event_id"] = item["viagogo_event_id"]
    if item.get("vivid_event_id"):
        event_ids["vivid_event_id"] = item["vivid_event_id"]
    if item.get("gotickets_event_id"):
        event_ids["gotickets_event_id"] = item["gotickets_event_id"]
    if item.get("seatgeek_event_id"):
        event_ids["seatgeek_event_id"] = item["seatgeek_event_id"]

    data = {
        "id": f"ticketmaster_event#{item.get('event_code')}",
        "sub_id": "blacklisted",
        "seating_section": item.get("section"),
        "event_blacklisted_at": format_date_epoch(item.get("start_date")),
        "event_blacklisted_at_str": format_date_str(item.get("start_date")),
        "event_blacklisted_reason": item.get("notes"),
        "event_blacklisted_expires_at": format_date_epoch(item.get("start_date")),
        "event_blacklisted_expires_at_str": format_date_str(item.get("start_date")),
        "market": market_list,
        **event_ids
    }

    return ShadowsBlacklistSQSMessage(**data)

async def send_data_to_sqs(message: ShadowsBlacklistSQSMessage):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    try:
        # Send the message to SQS
        response = sqs_client.send_message(
            QueueUrl=VIAGOGO_DELETE_SQS_QUEUE,
            MessageBody=message.model_dump_json()
        )
        return {
            "status": "success",
            "message_id": response["MessageId"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
