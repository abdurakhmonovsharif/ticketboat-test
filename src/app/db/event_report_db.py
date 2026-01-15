from datetime import datetime
from typing import Optional
import json

from app.database import get_snowflake_connection
import snowflake

async def get_event_reports(
        search_term: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
        page_size: Optional[int] = 50,
        page: Optional[int] = 1,
        sort_by: Optional[str] = "total_quantity",
        sort_order: Optional[str] = "desc",
        data_type: Optional[str] = "event",
):
    try:
        conditions = []
        values = {}

        if start_date and end_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('day', cp.created) >= %(start_date)s")
            conditions.append("DATE_TRUNC('day', cp.created) <= %(end_date)s")
            values["start_date"] = start_date
            values["end_date"] = end_date

        elif start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('day', cp.created) = %(current_date)s")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM cp.created) >= %(start_hour)s")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM cp.created) <= %(end_hour)s")
                values["end_hour"] = end_hour

        valid_sort_fields = {
            "event_name": "LOWER(event_name)",
            "venue": "LOWER(venue)",
            "total_quantity": "total_quantity",
            "total_cost": "total_cost",
            "total_buyers": "total_buyers",
        }

        if sort_by not in valid_sort_fields:
            sort_by = "total_cost"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "desc"

        base_query = ""
        count_query = ""

        if data_type == "event":
            conditions.extend([
                "cp.event_name IS NOT NULL",
                "cp.event_date_local IS NOT NULL",
                "cp.venue IS NOT NULL",
            ])

            if search_term:
                conditions.append("(cp.event_name ILIKE %(search)s OR cp.venue ILIKE %(search)s)")
                values["search"] = f"%{search_term}%"

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            base_query = f"""
                WITH SALES_DATA AS (
                    SELECT
                    po.external_reference,
                    i.total,
                    (t.cost * i.quantity) AS sold_cost,
                    i.total - (t.cost * i.quantity) AS profit
                FROM inventory_management.TICKET AS t
                LEFT JOIN inventory_management.PURCHASE_ORDER AS po 
                    ON po.purchase_order_id = t.purchase_order_id
                LEFT JOIN inventory_management.INVOICE AS i 
                    ON i.invoice_id = t.invoice_id
                JOIN inventory_management.source_type s 
                    ON s.source_type_id = t.source_type_id
                WHERE t.is_deleted = FALSE and s.is_enabled = true             
                GROUP BY po.external_reference, i.quantity, i.total, t.cost
            ),
                AGG_SALES_DATA AS (
                    SELECT
                        external_reference,
                        SUM(total) AS total_sales,
                        SUM(sold_cost) AS total_sold_cost,
                        SUM(profit) AS total_profit
                    FROM SALES_DATA
                    GROUP BY external_reference
                ),
                user_event_data AS (
                    SELECT
                        event_name,
                        event_date_local,
                        venue,
                        email,
                        SUM(quantity) AS user_quantity,
                        SUM(total_price) AS user_total_cost,
                        SUM(total_sold_cost) AS total_sold_cost,
                        SUM(total_profit) AS total_profit,
                        SUM(total_sales) AS total_sales
                    FROM combined_purchases_view AS cp
                    LEFT JOIN AGG_SALES_DATA asd 
                        ON cp.order_number = asd.external_reference
                    {where_clause}
                    GROUP BY event_name, event_date_local, venue, email
                ),
                event_data AS (
                    SELECT
                        event_name,
                        event_date_local,
                        venue,
                        COUNT(DISTINCT email) AS total_buyers,
                        SUM(user_quantity) AS total_quantity,
                        SUM(user_total_cost) AS total_cost,
                        SUM(total_sold_cost) AS total_sold_cost,
                        SUM(total_profit) AS total_profit,
                        SUM(total_sales) AS total_sales,
                        ARRAY_AGG(
                            OBJECT_CONSTRUCT(
                                'email', email, 
                                'quantity', user_quantity,
                                'total_cost', user_total_cost,
                                'user_total_sold_cost', total_sold_cost,
                                'user_total_profit', total_profit,
                                'user_total_sales', total_sales
                            )
                        ) WITHIN GROUP (ORDER BY user_quantity desc) AS users
                    FROM user_event_data
                    GROUP BY event_name, event_date_local, venue
                )
                SELECT
                    event_name as "event_name",
                    event_date_local as "event_date_local",
                    venue as "venue",
                    total_buyers as "total_buyers",
                    total_quantity as "total_quantity",
                    total_cost as "total_purchase_cost",
                    total_sold_cost as "total_sold_cost",
                    total_profit as "total_profit",
                    total_sales as "total_sales",
                    users as "users"
                FROM event_data
            """

            count_query = f"""
                WITH user_event_data AS (
                    SELECT
                        event_name,
                        event_date_local,
                        venue
                    FROM combined_purchases_view cp
                    {where_clause}
                    GROUP BY event_name, event_date_local, venue
                )
                SELECT COUNT(*) AS total
                FROM (
                    SELECT DISTINCT event_name, event_date_local, venue
                    FROM user_event_data
                ) unique_events
            """
        elif data_type == "tour":
            conditions.extend([
                "cp.event_name IS NOT NULL",
            ])

            if search_term:
                conditions.append("(cp.event_name ILIKE %(search)s)")
                values["search"] = f"%{search_term}%"

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            base_query = f"""
                WITH SALES_DATA AS (
                    SELECT
                        po.external_reference,
                        i.total,
                        (t.cost * i.quantity) AS sold_cost,
                        i.total - (t.cost * i.quantity) AS profit
                    FROM inventory_management.TICKET AS t
                    LEFT JOIN inventory_management.PURCHASE_ORDER AS po 
                        ON po.purchase_order_id = t.purchase_order_id
                    LEFT JOIN inventory_management.INVOICE AS i 
                        ON i.invoice_id = t.invoice_id
                    WHERE t.is_deleted = FALSE
                    GROUP BY
                        po.external_reference,
                        i.quantity,
                        i.total,
                        t.cost
                    ),
                    AGG_SALES_DATA AS (
                    SELECT
                        external_reference,
                        SUM(total) AS total_sales,
                        SUM(sold_cost) AS total_sold_cost,
                        SUM(profit) AS total_profit
                    FROM SALES_DATA
                    GROUP BY external_reference
                ),
                user_event_data AS (
                    SELECT
                        event_name,
                        email,
                        SUM(quantity) AS user_quantity,
                        SUM(total_price) AS user_total_cost,
                        SUM(total_sold_cost) AS total_sold_cost,
                        SUM(total_profit) AS total_profit,
                        SUM(total_sales) AS total_sales
                    FROM combined_purchases_view AS cp
                    LEFT JOIN AGG_SALES_DATA asd 
                        ON cp.order_number = asd.external_reference
                    {where_clause}
                    GROUP BY event_name, email
                ),
                event_data AS (
                    SELECT
                        event_name,
                        COUNT(DISTINCT email) AS total_buyers,
                        SUM(user_quantity) AS total_quantity,
                        SUM(user_total_cost) AS total_cost,
                        SUM(total_sold_cost) AS total_sold_cost,
                        SUM(total_profit) AS total_profit,
                        SUM(total_sales) AS total_sales,
                        ARRAY_AGG(
                            OBJECT_CONSTRUCT(
                                'email', email, 
                                'quantity', user_quantity,
                                'total_cost', user_total_cost,
                                'user_total_sold_cost', total_sold_cost,
                                'user_total_profit', total_profit,
                                'user_total_sales', total_sales
                            )
                        ) WITHIN GROUP (ORDER BY user_quantity desc) AS users
                    FROM user_event_data
                    GROUP BY event_name
                )
                SELECT
                    event_name as "event_name",
                    coalesce(total_buyers, 0) as "total_buyers",
                    coalesce(total_quantity, 0) as "total_quantity",
                    coalesce(total_cost, 0) as "total_purchase_cost",
                    coalesce(total_sold_cost, 0) as "total_sold_cost",
                    coalesce(total_profit, 0) as "total_profit",
                    coalesce(total_sales, 0) as "total_sales",
                    users as "users"
                FROM event_data
            """

            count_query = f"""
                WITH user_event_data AS (
                    SELECT
                        event_name
                    FROM combined_purchases_view cp
                    {where_clause}
                    GROUP BY event_name
                )
                SELECT COUNT(*) AS total
                FROM (
                    SELECT DISTINCT event_name
                    FROM user_event_data
                ) unique_events
            """

        order_by_clause = f"ORDER BY coalesce({valid_sort_fields[sort_by]}, 0) {sort_order}"

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_query, values)
            total = cur.fetchone()['TOTAL']

            # Get paginated data
            if page_size is not None and page is not None:
                data_query = f"""
                    {base_query}
                    {order_by_clause}
                    LIMIT %(page_size)s OFFSET %(offset)s
                """
                values["page_size"] = page_size
                values["offset"] = (page - 1) * page_size
            else:
                data_query = f"""
                    {base_query}
                    {order_by_clause}
                """
            cur.execute(data_query, values)
            results = cur.fetchall()

        parsed_results = []
        for result in results:
            parsed_result = dict(result)
            parsed_result['users'] = json.loads(parsed_result['users']) if parsed_result['users'] else []
            parsed_results.append(parsed_result)

        return {
            "items": parsed_results,
            "total": total
        }

    except Exception as e:
        return {"error": str(e)}

async def get_overall_event_report(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
):
    try:
        conditions = []
        values = {}

        # Filter by day or hour based on parameters
        if start_date and end_date:
            start_date_parsed = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_parsed = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('day', fl.created) >= %(start_date)s")
            conditions.append("DATE_TRUNC('day', fl.created) <= %(end_date)s")
            values["start_date"] = start_date_parsed
            values["end_date"] = end_date_parsed
        elif start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('day', fl.created) = %(current_date)s")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) >= %(start_hour)s")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) <= %(end_hour)s")
                values["end_hour"] = end_hour

        # Only consider valid events (if needed)
        conditions.append("fl.event_name IS NOT NULL")
        conditions.append("fl.event_date_local IS NOT NULL")
        conditions.append("fl.venue IS NOT NULL")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # This query mimics the aggregation in your get_event_reports function,
        # adding the new sales fields.
        query = f"""
            WITH SALES_DATA AS (
                SELECT
                    po.external_reference,
                    i.total,
                    (t.cost * i.quantity) AS sold_cost,
                    i.total - (t.cost * i.quantity) AS profit
                FROM inventory_management.TICKET AS t
                LEFT JOIN inventory_management.PURCHASE_ORDER AS po 
                    ON po.purchase_order_id = t.purchase_order_id
                LEFT JOIN inventory_management.INVOICE AS i 
                    ON i.invoice_id = t.invoice_id
                WHERE t.is_deleted = FALSE
                GROUP BY
                    po.external_reference,
                    i.quantity,
                    i.total,
                    t.cost
            ),
            AGG_SALES_DATA AS (
                SELECT
                    external_reference,
                    SUM(total) AS total_sales,
                    SUM(sold_cost) AS total_sold_cost,
                    SUM(profit) AS total_profit
                FROM SALES_DATA
                GROUP BY external_reference
            ),
            user_data AS (
                SELECT
                    email,
                    SUM(quantity) AS total_quantity,
                    SUM(total_price) AS total_cost,
                    SUM(total_sold_cost) AS total_sold_cost,
                    SUM(total_profit) AS total_profit,
                    SUM(total_sales) AS total_sales
                FROM combined_purchases_view AS fl
                LEFT JOIN AGG_SALES_DATA asd 
                    ON fl.order_number = asd.external_reference
                {where_clause}
                GROUP BY fl.email
            )
            SELECT
                SUM(total_quantity) AS "total_quantity",
                SUM(total_cost) AS "total_cost",
                SUM(total_sold_cost) AS "total_sold_cost",
                SUM(total_profit) AS "total_profit",
                SUM(total_sales) AS "total_sales",
                COUNT(DISTINCT email) AS "total_buyers"
            FROM user_data
        """

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, values)
            result = cur.fetchone()

        # Use .get(...) to safely extract values (Snowflake may return keys in lower case)
        total_quantity = result.get("total_quantity") or 0
        total_purchase_cost = float(result.get("total_purchase_cost") or 0)
        total_sold_cost = float(result.get("total_sold_cost") or 0)
        total_profit = float(result.get("total_profit") or 0)
        total_sales = float(result.get("total_sales") or 0)
        total_buyers = result.get("total_buyers") or 0

        return {
            "total_quantity": total_quantity,
            "total_purchase_cost": total_purchase_cost,
            "total_cost": total_sold_cost,
            "total_profit": total_profit,
            "total_sales": total_sales,
            "total_buyers": total_buyers
        }

    except Exception as e:
        return {"error": str(e)}