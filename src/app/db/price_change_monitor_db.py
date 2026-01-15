from fastapi import HTTPException
from typing import List, Optional
import json

from app.database import get_pg_database
from app.model.price_change_monitor import PriceChangeMonitorCreate, PriceChangeMonitorUpdate
from app.model.user import User


async def create_monitor(data: PriceChangeMonitorCreate, user: User):
    """Create a new price change monitor"""
    insert_query = """
    INSERT INTO price_change_monitor (
        event_id, event_name, increase_threshold, decrease_threshold,
        increase_monitored, decrease_monitored, email_recipients, created_by
    )
    VALUES (
        :event_id, :event_name, :increase_threshold, :decrease_threshold,
        :increase_monitored, :decrease_monitored, :email_recipients, :created_by
    )
    RETURNING id, event_id, event_name, increase_threshold, decrease_threshold,
              increase_monitored, decrease_monitored, email_recipients, status,
              created_at, updated_at, created_by
    """
    
    values = {
        "event_id": data.event_id,
        "event_name": data.event_name,
        "increase_threshold": data.increase_threshold,
        "decrease_threshold": data.decrease_threshold,
        "increase_monitored": data.increase_monitored,
        "decrease_monitored": data.decrease_monitored,
        "email_recipients": json.dumps(data.email_recipients),
        "created_by": user.email
    }
    
    try:
        result = await get_pg_database().fetch_one(insert_query, values)
        if result:
            monitor_dict = dict(result)
            # Parse email_recipients from JSONB to list
            if monitor_dict.get('email_recipients'):
                if isinstance(monitor_dict['email_recipients'], str):
                    monitor_dict['email_recipients'] = json.loads(monitor_dict['email_recipients'])
            else:
                monitor_dict['email_recipients'] = []
            return monitor_dict
        return None
    except Exception as e:
        print(f"Failed to create price change monitor: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def get_all_monitors(user: User):
    """Get all price change monitors"""
    query = """
    SELECT id, event_id, event_name, increase_threshold, decrease_threshold,
           increase_monitored, decrease_monitored, email_recipients, status,
           created_at, updated_at, created_by
    FROM price_change_monitor
    WHERE status != 'DELETED'
    ORDER BY created_at DESC
    """
    
    try:
        rows = await get_pg_database().fetch_all(query)
        if rows:
            monitors = []
            for row in rows:
                monitor_dict = dict(row)
                # Parse email_recipients from JSONB to list
                if monitor_dict.get('email_recipients'):
                    if isinstance(monitor_dict['email_recipients'], str):
                        monitor_dict['email_recipients'] = json.loads(monitor_dict['email_recipients'])
                else:
                    monitor_dict['email_recipients'] = []
                monitors.append(monitor_dict)
            return monitors
        return []
    except Exception as e:
        print(f"Failed to fetch price change monitors: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def get_monitor_by_id(monitor_id: int, user: User):
    """Get a specific price change monitor by ID"""
    query = """
    SELECT id, event_id, event_name, increase_threshold, decrease_threshold,
           increase_monitored, decrease_monitored, email_recipients, status,
           created_at, updated_at, created_by
    FROM price_change_monitor
    WHERE id = :monitor_id AND status != 'DELETED'
    """
    
    try:
        result = await get_pg_database().fetch_one(query, {"monitor_id": monitor_id})
        if result:
            monitor_dict = dict(result)
            # Parse email_recipients from JSONB to list
            if monitor_dict.get('email_recipients'):
                if isinstance(monitor_dict['email_recipients'], str):
                    monitor_dict['email_recipients'] = json.loads(monitor_dict['email_recipients'])
            else:
                monitor_dict['email_recipients'] = []
            return monitor_dict
        return None
    except Exception as e:
        print(f"Failed to fetch price change monitor: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def update_monitor(monitor_id: int, data: PriceChangeMonitorUpdate, user: User):
    """Update an existing price change monitor"""
    # Build dynamic update query based on provided fields
    update_fields = []
    values = {"monitor_id": monitor_id}
    
    if data.event_name is not None:
        update_fields.append("event_name = :event_name")
        values["event_name"] = data.event_name
    
    if data.increase_threshold is not None:
        update_fields.append("increase_threshold = :increase_threshold")
        values["increase_threshold"] = data.increase_threshold
    
    if data.decrease_threshold is not None:
        update_fields.append("decrease_threshold = :decrease_threshold")
        values["decrease_threshold"] = data.decrease_threshold
    
    if data.increase_monitored is not None:
        update_fields.append("increase_monitored = :increase_monitored")
        values["increase_monitored"] = data.increase_monitored
    
    if data.decrease_monitored is not None:
        update_fields.append("decrease_monitored = :decrease_monitored")
        values["decrease_monitored"] = data.decrease_monitored
    
    if data.email_recipients is not None:
        update_fields.append("email_recipients = :email_recipients")
        values["email_recipients"] = json.dumps(data.email_recipients)
    
    if data.status is not None:
        update_fields.append("status = :status")
        values["status"] = data.status
    
    if not update_fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    # Always update updated_at
    update_fields.append("updated_at = NOW()")
    
    update_query = f"""
    UPDATE price_change_monitor
    SET {', '.join(update_fields)}
    WHERE id = :monitor_id AND status != 'DELETED'
    RETURNING id, event_id, event_name, increase_threshold, decrease_threshold,
              increase_monitored, decrease_monitored, email_recipients, status,
              created_at, updated_at, created_by
    """
    
    try:
        result = await get_pg_database().fetch_one(update_query, values)
        if result:
            monitor_dict = dict(result)
            # Parse email_recipients from JSONB to list
            if monitor_dict.get('email_recipients'):
                if isinstance(monitor_dict['email_recipients'], str):
                    monitor_dict['email_recipients'] = json.loads(monitor_dict['email_recipients'])
            else:
                monitor_dict['email_recipients'] = []
            return monitor_dict
        raise HTTPException(status_code=404, detail="Monitor not found")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Failed to update price change monitor: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def delete_monitor(monitor_id: int, user: User):
    """Soft delete a price change monitor by setting status to DELETED"""
    delete_query = """
    UPDATE price_change_monitor
    SET status = 'DELETED', updated_at = NOW()
    WHERE id = :monitor_id AND status != 'DELETED'
    RETURNING id
    """
    
    try:
        result = await get_pg_database().fetch_one(delete_query, {"monitor_id": monitor_id})
        if result:
            return {"message": "Monitor deleted successfully", "id": result["id"]}
        raise HTTPException(status_code=404, detail="Monitor not found")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Failed to delete price change monitor: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")



