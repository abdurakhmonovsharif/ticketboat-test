from typing import Optional
from datetime import datetime
import snowflake

from app.database import get_snowflake_connection, get_pg_database
from app.model.po_queue import POCreateRequest


async def get_purchase_confirmation_data(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None
) -> dict:
    values = {}
    limit_expression = ""
    if page and page_size:
        limit_expression = f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"

    conditions = []
    if search_term:
        conditions.append("""
            (
                (COALESCE(account, '') ||
                COALESCE(card, '') ||
                COALESCE(event, '') ||
                COALESCE(opponent, '') ||
                COALESCE(venue, '') ||
                COALESCE("date", '') ||
                COALESCE("time", '') ||
                COALESCE(CAST(tba AS TEXT), '') ||
                COALESCE(shipping_method, '') ||
                COALESCE(CAST(quantity AS TEXT), '') ||
                COALESCE("section", '') ||
                COALESCE("row", '') ||
                COALESCE(start_seat, '') ||
                COALESCE(end_seat, '') ||
                COALESCE(CAST(total_cost AS TEXT), '') ||
                COALESCE(conf_number, '') ||
                COALESCE(CAST(consecutive AS TEXT), '') ||
                COALESCE(internal_note, '') ||
                COALESCE(external_note, '') ||
                COALESCE(po_note, '') ||
                COALESCE(po_number, '') ||
                COALESCE(CAST(created AS TEXT), '') ||
                COALESCE(status, '') ) ILIKE :search_term
            )
        """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT 
            id AS "id",
            email_id AS "email_id",
            account AS "account",
            card AS "card",
            event AS "event",
            opponent AS "opponent",
            venue AS "venue",
            "date" AS "date",
            "time" AS "time",
            tba AS "tba",
            shipping_method AS "shipping_method",
            quantity AS "quantity",
            "section" AS "section",
            "row" AS "row",
            start_seat AS "start_seat",
            end_seat AS "end_seat",
            total_cost AS "total_cost",
            conf_number AS "conf_number",
            consecutive AS "consecutive",
            internal_note AS "internal_note",
            external_note AS "external_note",
            po_note AS "po_note",
            po_number AS "po_number",
            created AS "created",
            status AS "status"
        FROM email.email_purchase_confirmation
        {where_clause}
        ORDER BY created DESC
        {limit_expression};
    """

    try:
        res = await get_pg_database().fetch_all(sql, values)
        total_count = await get_purchase_confirmation_count(search_term)
        return {
            "total": total_count,
            "items": [dict(r) for r in res]
        }
    except Exception as e:
        print(f"Error fetching purchase confirmation data: {e}")
        return {"error": "Failed to fetch data"}


async def get_purchase_confirmation_count(search_term: Optional[str] = None) -> int:
    values = {}
    conditions = []
    if search_term:
        conditions.append("""
            (
                (COALESCE(account, '') ||
                COALESCE(card, '') ||
                COALESCE(event, '') ||
                COALESCE(opponent, '') ||
                COALESCE(venue, '') ||
                COALESCE("date", '') ||
                COALESCE("time", '') ||
                COALESCE(CAST(tba AS TEXT), '') ||
                COALESCE(shipping_method, '') ||
                COALESCE(CAST(quantity AS TEXT), '') ||
                COALESCE("section", '') ||
                COALESCE("row", '') ||
                COALESCE(start_seat, '') ||
                COALESCE(end_seat, '') ||
                COALESCE(CAST(total_cost AS TEXT), '') ||
                COALESCE(conf_number, '') ||
                COALESCE(CAST(consecutive AS TEXT), '') ||
                COALESCE(internal_note, '') ||
                COALESCE(external_note, '') ||
                COALESCE(po_note, '') ||
                COALESCE(po_number, '') ||
                COALESCE(CAST(created AS TEXT), '') ||
                COALESCE(status, '') ) ILIKE :search_term
            )
        """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT COUNT(1) AS cnt 
        FROM email.email_purchase_confirmation
        {where_clause};
    """

    try:
        res = await get_pg_database().fetch_one(sql, values)
        return res["cnt"]
    except Exception as e:
        print(f"Error counting purchase confirmation data: {e}")
        return 0


async def update_status(po_id: str, status: str) -> dict:
    values = {
        "po_id": po_id,
        "status": status
    }
    # Update status in Snowflake
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(
                """
                UPDATE email_purchase_confirmation
                SET status = %(status)s
                WHERE id = %(po_id)s
                """,
                values
            )
    except Exception as e:
        print(f"Error updating status in Snowflake: {e}")
        return {"error": "Failed to update status in Snowflake"}

    # Update status in PostgreSQL
    try:
        sql = """
            UPDATE email.email_purchase_confirmation
            SET status = :status
            WHERE id = :po_id
        """
        await get_pg_database().execute(sql, values)
    except Exception as e:
        print(f"Error updating status in PostgreSQL: {e}")
        return {"error": "Failed to update status in PostgreSQL"}

    return {"message": "Successfully updated status in both databases"}


async def create_po(po_data: POCreateRequest) -> dict:
    if isinstance(po_data.created, str):
        po_data.created = datetime.strptime(po_data.created, "%Y-%m-%d %H:%M:%S.%f")
    # Insert into Snowflake
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            query = """
            INSERT INTO email_purchase_confirmation (
                id, email_id, account, card, event, opponent, venue, "date", "time", tba,
                shipping_method, quantity, "section", "row", start_seat, end_seat, 
                total_cost, conf_number, consecutive, internal_note, external_note,
                po_note, po_number, created, status
            ) VALUES (
                %(id)s, %(email_id)s, %(account)s, %(card)s, %(event)s, %(opponent)s,
                %(venue)s, %(date)s, %(time)s, %(tba)s, %(shipping_method)s, %(quantity)s,
                %(section)s, %(row)s, %(start_seat)s, %(end_seat)s, %(total_cost)s,
                %(conf_number)s, %(consecutive)s, %(internal_note)s, %(external_note)s,
                %(po_note)s, %(po_number)s, %(created)s, %(status)s
            )
            """
            # Execute the query
            cur.execute(query, po_data.dict())
            cur.connection.commit()
    except Exception as e:
        print(f"Failed to create PO in Snowflake: {e}")
        return {"error": "Failed to create PO in Snowflake"}

    # Insert into PostgreSQL
    try:
        sql = """
        INSERT INTO email.email_purchase_confirmation (
            id, email_id, account, card, event, opponent, venue, "date", "time", tba,
            shipping_method, quantity, "section", "row", start_seat, end_seat, 
            total_cost, conf_number, consecutive, internal_note, external_note,
            po_note, po_number, created, status
        ) VALUES (
            :id, :email_id, :account, :card, :event, :opponent, :venue, :date, :time, :tba,
            :shipping_method, :quantity, :section, :row, :start_seat, :end_seat,
            :total_cost, :conf_number, :consecutive, :internal_note, :external_note,
            :po_note, :po_number, :created, :status
        )
        """
        await get_pg_database().execute(sql, po_data.dict())
    except Exception as e:
        print(f"Failed to create PO in PostgreSQL: {e}")
        return {"error": "Failed to create PO in PostgreSQL"}

    return {"message": "PO created successfully in both databases", "id": po_data.id}
