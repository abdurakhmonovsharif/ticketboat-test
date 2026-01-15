import traceback
import snowflake.connector
import uuid
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from app.database import get_pg_realtime_catalog_database, get_snowflake_connection
from app.model.shadows_blacklist_wildcard import (
    ShadowsWildcardBlacklist,
    ShadowsWildcardBlacklistChangeLog
)
from app.model.user import User


async def get_items() -> Dict[str, Any]:
    sql = """
        select
            id,
            event_name_like,
            reason,
            added_by,
            created_at AT TIME ZONE 'UTC' AT TIME ZONE 'CST' AS created_at,
            market,
            city,
            similarity,
            field
        from shadows_blacklist_wildcard
    """
    try:
        results = await get_pg_realtime_catalog_database().fetch_all(sql)
        items = [ShadowsWildcardBlacklist(**dict(result)) for result in results]
        return {"items": items, "total": len(items)}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting blacklist items") from e

async def get_item(id: str) -> ShadowsWildcardBlacklist:
    sql = """
        select * from shadows_blacklist_wildcard where id = :id
    """
    try:
        result = await get_pg_realtime_catalog_database().fetch_one(sql, {"id": id})
        return ShadowsWildcardBlacklist(**dict(result)) # type: ignore
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while getting blacklist item") from e

async def update_item(event_name_like: str, reason: str, id: str, user: User, similarity: str, field: str, market: Any, city: Optional[str]) -> Dict[str, Any]:
    sql = """
        update shadows_blacklist_wildcard set event_name_like = :event_name_like, reason = :reason, similarity = :similarity, field = :field, market = :market, city = :city where id = :id
        returning *
    """
    params = {
        "event_name_like": event_name_like,
        "reason": reason,
        "id": id,
        "similarity": similarity,
        "field": field,
        "market": market,
        "city": city
    }
    try:
        result = await get_pg_realtime_catalog_database().fetch_one(sql, params)
        if not result:
            raise HTTPException(status_code=404, detail="No row found to update")

        params["added_by"] = user.name # type: ignore
        change_log_data = sanitize_change_log_data('update', params)
        await update_item_snowflake(event_name_like, id, similarity, field, market)
        await create_wildcard_blacklist_log(change_log_data)
        return {
            "status": "success",
            "message": "Information updated successfully",
            "item": ShadowsWildcardBlacklist(**dict(result))
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while updating blacklist item") from e
    
async def update_item_snowflake(event_name_like: str, id: str, similarity: str, field: str, market: Any):
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                update wildcard_blacklist set event_name_like = %(event_name_like)s, similarity = %(similarity)s, field = %(field)s, market = %(market)s where id = %(id)s
            """
            cur.execute(sql, {"id": id, "event_name_like": event_name_like, "similarity": similarity, "field": field, "market": market})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def create_item(event_name_like: str, reason: str, market: Any, user: User, similarity: str, field: str, city: Optional[str]) -> Dict[str, Any]:
    market_str = ",".join(m.lower() for m in market)
    sql = """
        insert into shadows_blacklist_wildcard (event_name_like, reason, added_by, market, similarity, field, city)
        values (:event_name_like, :reason, :added_by, :market, :similarity, :field, :city)
        returning *
    """
    data = {
        "event_name_like": event_name_like,
        "reason": reason,
        "added_by": user.name,
        "market": market_str,
        "similarity": similarity,
        "field": field,
        "city": city
    }
    try:
        result = await get_pg_realtime_catalog_database().fetch_one(sql, data)
        if not result:
            raise HTTPException(status_code=500, detail="Unable to insert data to db")

        change_log_data = sanitize_change_log_data('create', data)
        model_result = ShadowsWildcardBlacklist(**dict(result))
        await create_item_snowflake(model_result)
        await create_wildcard_blacklist_log(change_log_data)
        return {
            "message": "Wildcard blacklist entry created",
            "data": model_result
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while inserting data") from e
    
async def create_item_snowflake(data: ShadowsWildcardBlacklist):
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                insert into wildcard_blacklist(id, event_name_like, created, reason, market, similarity, field)
                values(%(id)s, %(event_name_like)s, current_timestamp, %(reason)s, %(market)s, %(similarity)s, %(field)s)
            """
            cur.execute(sql, {
                "id": str(data.id),
                "event_name_like": data.event_name_like,
                "reason": data.reason,
                "market": data.market,
                "similarity": data.similarity,
                "field": data.field
            })
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def delete_item(id: str, event_name_like: str, user: User) -> Dict[str, Any]:
    sql = """
        delete from shadows_blacklist_wildcard where id = :id
    """
    try:
        await get_pg_realtime_catalog_database().fetch_one(sql, {"id": id})
        change_log_data = sanitize_change_log_data('delete', {
            "event_name_like": event_name_like,
            "reason": "delete",
            "added_by": user.name
        })
        await delete_item_snowflake(id)
        await create_wildcard_blacklist_log(change_log_data)
        return {
            "message": "Wildcard blacklist entry deleted",
            "item_id": id
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while removing data") from e

async def delete_item_snowflake(id: str) -> None:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                delete from wildcard_blacklist where id = %(id)s
            """
            cur.execute(sql, {"id": id})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def create_wildcard_blacklist_log(payload: ShadowsWildcardBlacklistChangeLog) -> None:
    sql = """
        insert into shadows_blacklist_wildcard_change_log (operation, event_name_like, reason, added_by, market)
        values (:operation, :event_name_like, :reason, :added_by, :market)
        returning id
    """
    try:
        result = await get_pg_realtime_catalog_database().fetch_one(sql, payload.model_dump())
        if result is None or result["id"] is None:
            raise HTTPException(status_code=500, detail="Item not found or update failed.")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="An error occurred while inserting data") from e

def sanitize_change_log_data(operation: str, payload: dict) -> ShadowsWildcardBlacklistChangeLog:
    data = {
        "operation": operation,
        "event_name_like": payload.get("event_name_like"),
        "reason": payload.get("reason"),
        "added_by": payload.get("added_by"),
        "market": payload.get("market")
    }
    return ShadowsWildcardBlacklistChangeLog(**data)
