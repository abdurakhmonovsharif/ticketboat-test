from fastapi import HTTPException

from app.database import get_pg_database
from app.model.log_navigation import LogNavigation
from app.model.user import User


async def create_log_navigation(create_data: LogNavigation, user: User):
    # check for existing entries
    check_query = """
    SELECT 1 FROM user_navigation_logs
    WHERE user_id = :user_id
    AND page_url = :page_url
    AND timestamp >= date_trunc('day', current_timestamp)
    """
    check_values = {"user_id": user.email, "page_url": create_data.page_url}

    existing_entry = await get_pg_database().fetch_one(check_query, check_values)

    if existing_entry:
        # Entry exists; do not insert a new one
        return {"message": "Log entry already exists for today"}
    else:
        # Entry does not exist; insert a new one
        insert_query = """
        INSERT INTO user_navigation_logs (user_id, page_url, page_label)
        VALUES (:user_id, :page_url, :page_label)
        """
        insert_values = {"user_id": user.email, "page_url": create_data.page_url, "page_label": create_data.page_label}
        await get_pg_database().execute(insert_query, insert_values)
        return {"message": "Log entry created successfully"}


async def get_popular_log_navigation():
    query = """
        SELECT page_url, page_label, COUNT(*) AS visit_count
        FROM user_navigation_logs
        WHERE timestamp >= NOW() - INTERVAL '30 days'
        GROUP BY page_url, page_label
        ORDER BY visit_count DESC
        LIMIT 10
    """
    try:
        rows = await get_pg_database().fetch_all(query)
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        print(f"Failed to fetch top pages: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def delete_old_log_navigation():
    query = """
       DELETE FROM user_navigation_logs WHERE timestamp < NOW() - INTERVAL '30 days'
    """
    await get_pg_database().execute(query)
