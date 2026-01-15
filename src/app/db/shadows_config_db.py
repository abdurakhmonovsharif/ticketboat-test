import traceback
from typing import List, Dict, Any
import uuid
import snowflake.connector
from app.database import get_pg_realtime_catalog_database, get_snowflake_connection
from app.model.shadows_config import ShadowsConfigCreate

async def log_config_change_to_snowflake(
    config_id: str,
    exchange: str,
    override_type: str,
    override_value: str | None,
    config_type: str,
    config_value: float,
    updated_by: str
) -> None:
    """Log config change to Snowflake history table"""
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                INSERT INTO shadows_config_change_history 
                (id, exchange, override_type, override_value, config_type, config_value, updated_by)
                VALUES (%(id)s, %(exchange)s, %(override_type)s, %(override_value)s, %(config_type)s, %(config_value)s, %(updated_by)s)
            """
            cur.execute(sql, {
                "id": config_id,
                "exchange": exchange,
                "override_type": override_type,
                "override_value": override_value,
                "config_type": config_type,
                "config_value": config_value,
                "updated_by": updated_by
            })
    except Exception as e:
        # Log the error but don't fail the main operation
        print(f"Error logging to Snowflake history: {e}")
        traceback.print_exc()

async def get_all_configs() -> List[Dict[str, Any]]:
    """Get all shadows configs from database"""
    try:
        query = """
            SELECT id, exchange, override_type, override_value, config_type, config_value, 
                   created_at, updated_at
            FROM shadows_config
            ORDER BY exchange, override_type, config_type
        """
        db = get_pg_realtime_catalog_database()
        results = await db.fetch_all(query=query)
        return [
            {
                "id": row["id"],
                "exchange": row["exchange"],
                "override_type": row["override_type"],
                "override_value": row["override_value"],
                "config_type": row["config_type"],
                "config_value": float(row["config_value"]) if row["config_value"] is not None else None,
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            }
            for row in results
        ]
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error fetching shadows configs") from e

async def get_configs_by_exchange(exchange: str) -> List[Dict[str, Any]]:
    """Get all configs for a specific exchange"""
    try:
        query = """
            SELECT id, exchange, override_type, override_value, config_type, config_value,
                   created_at, updated_at
            FROM shadows_config
            WHERE exchange = :exchange
            ORDER BY override_type, config_type
        """
        db = get_pg_realtime_catalog_database()
        results = await db.fetch_all(query=query, values={"exchange": exchange})
        return [
            {
                "id": row["id"],
                "exchange": row["exchange"],
                "override_type": row["override_type"],
                "override_value": row["override_value"],
                "config_type": row["config_type"],
                "config_value": float(row["config_value"]) if row["config_value"] is not None else None,
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            }
            for row in results
        ]
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error fetching configs for exchange {exchange}") from e

async def get_exchanges() -> List[Dict[str, str]]:
    """Get list of all unique exchanges"""
    try:
        query = """
            SELECT DISTINCT exchange
            FROM shadows_config
            ORDER BY exchange
        """
        db = get_pg_realtime_catalog_database()
        results = await db.fetch_all(query=query)
        return [{"name": row["exchange"]} for row in results]
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error fetching exchanges") from e

async def create_config(config: ShadowsConfigCreate, updated_by: str = "system") -> Dict[str, Any]:
    """Create a new config"""
    try:
        query = """
            INSERT INTO shadows_config (exchange, override_type, override_value, config_type, config_value)
            VALUES (:exchange, :override_type, :override_value, :config_type, :config_value)
            RETURNING id, exchange, override_type, override_value, config_type, config_value, 
                      created_at, updated_at
        """
        db = get_pg_realtime_catalog_database()
        result = await db.fetch_one(
            query=query,
            values={
                "exchange": config.exchange,
                "override_type": config.override_type,
                "override_value": config.override_value,
                "config_type": config.config_type,
                "config_value": config.config_value,
            },
        )
        
        # Log to Snowflake history
        await log_config_change_to_snowflake(
            config_id=str(result["id"]),
            exchange=result["exchange"],
            override_type=result["override_type"],
            override_value=result["override_value"],
            config_type=result["config_type"],
            config_value=float(result["config_value"]) if result["config_value"] is not None else 0.0,
            updated_by=updated_by
        )
        
        return {
            "id": result["id"],
            "exchange": result["exchange"],
            "override_type": result["override_type"],
            "override_value": result["override_value"],
            "config_type": result["config_type"],
            "config_value": float(result["config_value"]) if result["config_value"] is not None else None,
            "created_at": result["created_at"].isoformat() if result["created_at"] else None,
            "updated_at": result["updated_at"].isoformat() if result["updated_at"] else None,
        }
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error creating config") from e

async def update_config(config_id: int, config_value: float, updated_by: str = "system") -> Dict[str, Any]:
    """Update a config's value"""
    try:
        query = """
            UPDATE shadows_config
            SET config_value = :config_value, updated_at = NOW()
            WHERE id = :id
            RETURNING id, exchange, override_type, override_value, config_type, config_value,
                      created_at, updated_at
        """
        db = get_pg_realtime_catalog_database()
        result = await db.fetch_one(query=query, values={"config_value": config_value, "id": config_id})
        if not result:
            raise Exception(f"Config with id {config_id} not found")
        
        # Log to Snowflake history
        await log_config_change_to_snowflake(
            config_id=str(result["id"]),
            exchange=result["exchange"],
            override_type=result["override_type"],
            override_value=result["override_value"],
            config_type=result["config_type"],
            config_value=float(result["config_value"]) if result["config_value"] is not None else 0.0,
            updated_by=updated_by
        )
        
        return {
            "id": result["id"],
            "exchange": result["exchange"],
            "override_type": result["override_type"],
            "override_value": result["override_value"],
            "config_type": result["config_type"],
            "config_value": float(result["config_value"]) if result["config_value"] is not None else None,
            "created_at": result["created_at"].isoformat() if result["created_at"] else None,
            "updated_at": result["updated_at"].isoformat() if result["updated_at"] else None,
        }
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error updating config {config_id}") from e

async def delete_config(config_id: int, updated_by: str = "system") -> None:
    """Delete a config"""
    try:
        # First, get the config details before deletion for logging
        get_query = """
            SELECT id, exchange, override_type, override_value, config_type, config_value
            FROM shadows_config
            WHERE id = :id
        """
        db = get_pg_realtime_catalog_database()
        config = await db.fetch_one(query=get_query, values={"id": config_id})
        
        if config:
            # Log to Snowflake history before deletion
            await log_config_change_to_snowflake(
                config_id=str(config["id"]),
                exchange=config["exchange"],
                override_type=config["override_type"],
                override_value=config["override_value"],
                config_type=config["config_type"],
                config_value=float(config["config_value"]) if config["config_value"] is not None else 0.0,
                updated_by=f"{updated_by} (deleted)"
            )
        
        # Delete the config
        delete_query = "DELETE FROM shadows_config WHERE id = :id"
        await db.execute(query=delete_query, values={"id": config_id})
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error deleting config {config_id}") from e

async def delete_exchange(exchange: str, updated_by: str = "system") -> None:
    """Delete an exchange and all its configs"""
    try:
        # First, get all configs for this exchange for logging
        get_query = """
            SELECT id, exchange, override_type, override_value, config_type, config_value
            FROM shadows_config
            WHERE exchange = :exchange
        """
        db = get_pg_realtime_catalog_database()
        configs = await db.fetch_all(query=get_query, values={"exchange": exchange})
        
        # Log each config deletion to Snowflake
        for config in configs:
            await log_config_change_to_snowflake(
                config_id=str(config["id"]),
                exchange=config["exchange"],
                override_type=config["override_type"],
                override_value=config["override_value"],
                config_type=config["config_type"],
                config_value=float(config["config_value"]) if config["config_value"] is not None else 0.0,
                updated_by=f"{updated_by} (exchange deleted)"
            )
        
        # Delete all configs for the exchange
        delete_query = "DELETE FROM shadows_config WHERE exchange = :exchange"
        await db.execute(query=delete_query, values={"exchange": exchange})
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error deleting exchange {exchange}") from e

async def get_config_history_by_exchange(
    exchange: str, 
    page: int = 1, 
    page_size: int = 20
) -> Dict[str, Any]:
    """Get config change history for a specific exchange with pagination"""
    try:
        offset = (page - 1) * page_size
        
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            count_sql = """
                SELECT COUNT(*) as total
                FROM shadows_config_change_history
                WHERE exchange = %(exchange)s
            """
            cur.execute(count_sql, {"exchange": exchange})
            count_result = cur.fetchone()
            total_count = count_result["TOTAL"] if count_result else 0
            
            # Get paginated history
            history_sql = """
                SELECT id, exchange, override_type, override_value, config_type, 
                       config_value, history_timestamp, updated_by
                FROM shadows_config_change_history
                WHERE exchange = %(exchange)s
                ORDER BY history_timestamp DESC
                LIMIT %(limit)s OFFSET %(offset)s
            """
            cur.execute(history_sql, {
                "exchange": exchange,
                "limit": page_size,
                "offset": offset
            })
            
            results = cur.fetchall()
            
            history_items = [
                {
                    "id": row["ID"],
                    "exchange": row["EXCHANGE"],
                    "override_type": row["OVERRIDE_TYPE"],
                    "override_value": row["OVERRIDE_VALUE"],
                    "config_type": row["CONFIG_TYPE"],
                    "config_value": float(row["CONFIG_VALUE"]) if row["CONFIG_VALUE"] is not None else None,
                    "history_timestamp": row["HISTORY_TIMESTAMP"].isoformat() if row["HISTORY_TIMESTAMP"] else None,
                    "updated_by": row["UPDATED_BY"]
                }
                for row in results
            ]
            
            return {
                "items": history_items,
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_count + page_size - 1) // page_size
            }
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error fetching history for exchange {exchange}") from e

