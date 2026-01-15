from typing import Dict, Optional

from app.database import get_pg_database


async def get_tm_queue_tracking(
        page_size: int = 10,
        page: int = 1,
        search: Optional[str] = None
) -> Dict:
    offset = (page - 1) * page_size

    # Base query
    base_query = """
        SELECT 
            id,
            account_name,
            event_name,
            venue,
            event_date_time,
            queue_position,
            pt_version,
            client_id,
            created_at
        FROM browser_data_capture.tm_queue_tracking
    """

    # Count query for total records
    count_query = "SELECT COUNT(*) FROM browser_data_capture.tm_queue_tracking"

    # Add search condition if search term is provided
    where_clause = ""
    values = {}

    if search:
        where_clause = """
            WHERE 
                LOWER(account_name) LIKE LOWER(:search)
                OR LOWER(event_name) LIKE LOWER(:search)
                OR LOWER(venue) LIKE LOWER(:search)
                OR LOWER(event_date_time::text) LIKE LOWER(:search)
                OR LOWER(queue_position) LIKE LOWER(:search)
                OR created_at::text LIKE :search
        """
        values["search"] = f"%{search}%"

    # Construct final queries with direct interpolation for LIMIT and OFFSET
    final_query = f"""
        {base_query}
        {where_clause}
        ORDER BY created_at DESC
        LIMIT {page_size}
        OFFSET {offset}
    """

    final_count_query = f"{count_query} {where_clause}"

    # Execute queries
    db = get_pg_database()
    results = await db.fetch_all(final_query, values)
    total_count = await db.fetch_val(final_count_query, values)

    # Process results
    processed_results = [
        {
            "id": str(row["id"]),
            "account_name": row["account_name"],
            "event_name": row["event_name"],
            "venue": row["venue"],
            "event_date_time": row["event_date_time"],
            "queue_position": row["queue_position"],
            "pt_version": row["pt_version"],
            "client_id": row["client_id"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None
        }
        for row in results
    ]

    return {
        "items": processed_results,
        "total": total_count
    }


async def get_tm_queue_summary(
        page_size: int = 10,
        page: int = 1,
        search: Optional[str] = None
) -> Dict:
    # Calculate offset for pagination
    offset = (page - 1) * page_size

    # Base summary query with necessary calculations
    base_summary_query = """
        WITH account_events AS (
            SELECT
                account_name,
                event_name,
                queue_position::FLOAT AS queue_position,
                created_at,
                ROW_NUMBER() OVER (PARTITION BY account_name ORDER BY created_at DESC) AS rn
            FROM browser_data_capture.tm_queue_tracking
            WHERE 
                event_name IS NOT NULL AND event_name != '' AND
                queue_position IS NOT NULL AND queue_position != '' AND
                created_at IS NOT NULL
        ),
        latest_events AS (
            SELECT 
                account_name,
                event_name,
                queue_position AS last_queue_position,
                created_at AS last_event_timestamp
            FROM account_events
            WHERE rn = 1
        ),
        max_positions AS (
            SELECT
                event_name,
                MAX(queue_position) AS max_queue_position
            FROM account_events
            GROUP BY event_name
        ),
        last_5_event_percentages AS (
            SELECT
                ae.account_name,
                ae.event_name,
                (ae.queue_position / mp.max_queue_position) * 100 AS event_percentage
            FROM account_events ae
            JOIN max_positions mp ON ae.event_name = mp.event_name
            WHERE ae.rn <= 5
        ),
        average_last_5_percentages AS (
            SELECT
                account_name,
                AVG(event_percentage) AS avg_last_5_percentage
            FROM last_5_event_percentages
            GROUP BY account_name
        ),
        final_summary AS (
            SELECT
                le.account_name,
                le.last_event_timestamp,
                (le.last_queue_position::FLOAT / mp.max_queue_position) * 100 AS last_event_percentage,
                al5.avg_last_5_percentage
            FROM latest_events le
            JOIN max_positions mp ON le.event_name = mp.event_name
            JOIN average_last_5_percentages al5 ON le.account_name = al5.account_name
        )
        SELECT * FROM final_summary
    """

    # Count query to get total record count for pagination
    count_query = "SELECT COUNT(DISTINCT account_name) FROM browser_data_capture.tm_queue_tracking"

    # Prepare where clause for search if applicable
    where_clause = ""
    values = {}

    if search:
        where_clause = """
            WHERE 
                LOWER(account_name) LIKE LOWER(:search)
        """
        values["search"] = f"%{search}%"

    # Finalize the queries with the where clause
    final_summary_query = f"""
        WITH summary AS (
            {base_summary_query}
        )
        SELECT * FROM summary
        {where_clause}
        ORDER BY last_event_percentage ASC, last_event_timestamp DESC
        LIMIT {page_size}
        OFFSET {offset}
    """
    final_count_query = f"{count_query} {where_clause}"

    # Execute queries
    db = get_pg_database()
    results = await db.fetch_all(final_summary_query, values)
    total_count = await db.fetch_val(final_count_query, values)

    # Process results for output
    processed_results = [
        {
            "account_name": row["account_name"],
            "last_event_timestamp": row["last_event_timestamp"].isoformat() if row["last_event_timestamp"] else None,
            "last_event_percentage": row["last_event_percentage"],
            "avg_last_5_percentage": row["avg_last_5_percentage"]
        }
        for row in results
    ]

    return {
        "items": processed_results,
        "total": total_count
    }