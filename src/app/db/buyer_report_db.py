from datetime import datetime
from typing import Optional

import snowflake
from app.database import get_pg_database, get_snowflake_connection


def parse_datetime(dt_str: str) -> datetime:
    """
    Parse a date/time string in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format.
    """
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid date/time format: {dt_str}")


async def get_buyer_reports(
        search_term: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
        page_size: Optional[int] = 50,
        page: Optional[int] = 1,
        sort_by: Optional[str] = "total_quantity",
        sort_order: Optional[str] = "desc"
):
    try:
        conditions = []
        values = {}

        if start_date and end_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', fl.created) >= %(start_date)s")
            conditions.append("DATE_TRUNC('DAY', fl.created) <= %(end_date)s")
            values["start_date"] = start_date
            values["end_date"] = end_date

        elif start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('DAY', fl.created) = %(current_date)s")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) >= %(start_hour)s")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) <= %(end_hour)s")
                values["end_hour"] = end_hour

        valid_sort_fields = {
            "email": "b.email",
            "total_quantity": "b.total_quantity",
            "total_cost": "b.total_cost",
            "total_profit": "b.total_profit",
            "total_orders_purchased": "b.total_orders_purchased",
            "top_performer_percentage": "top_performer_percentage",
            "conversion_rate": "conversion_rate",
        }

        if sort_by not in valid_sort_fields:
            sort_by = "total_cost"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "asc"

        if search_term:
            conditions.append("(fl.email ILIKE %(search)s)")
            values["search"] = f"%{search_term}%"

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        base_cte = f"""
            WITH SALES_DATA AS (
                SELECT
                    i.internal_id AS sale_id,
                    po.external_reference,
                    i.total AS total_price,
                    (t.cost * i.quantity) AS sold_cost,
                    i.total - (t.cost * i.quantity) AS profit
                FROM inventory_management.TICKET AS t
                LEFT JOIN inventory_management.PURCHASE_ORDER AS po 
                    ON po.purchase_order_id = t.purchase_order_id
                LEFT JOIN inventory_management.INVOICE AS i 
                    ON i.invoice_id = t.invoice_id
                JOIN inventory_management.source_type s 
                    ON s.source_type_id = t.source_type_id
                WHERE t.is_deleted = FALSE 
                 AND s.is_enabled = true
                 AND (t.purchase_order_id IS NOT NULL OR t.invoice_id IS NOT NULL)            
                GROUP BY i.internal_id, po.external_reference, i.quantity, i.total, t.cost
            ),
            AGG_SALES_DATA AS (
                SELECT
                    sale_id,
                    external_reference,
                    SUM(total_price) AS total_sales,
                    SUM(sold_cost) AS total_sold_cost,
                    SUM(profit) AS total_profit
                FROM SALES_DATA
                WHERE external_reference IS NOT NULL
                GROUP BY sale_id, external_reference
            ),
            final_sales AS (
                SELECT
                    cp.email,
                    SUM(asd.total_sales) AS total_sales,
                    SUM(asd.total_sold_cost) AS total_sold_cost,
                    SUM(asd.total_profit) AS total_profit
                FROM combined_purchases_view AS cp
                JOIN AGG_SALES_DATA AS asd
                    ON cp.order_number = asd.external_reference
                {where_clause.replace('fl', 'cp')}
                GROUP BY cp.email
            ),
            SALES_DATA_WITHOUT_SALE_ID AS (
                SELECT
                    po.external_reference,
                    i.total AS total_price,
                    (t.cost * i.quantity) AS sold_cost,
                    i.total - (t.cost * i.quantity) AS profit
                FROM inventory_management.TICKET AS t
                LEFT JOIN inventory_management.PURCHASE_ORDER AS po 
                    ON po.purchase_order_id = t.purchase_order_id
                LEFT JOIN inventory_management.INVOICE AS i 
                    ON i.invoice_id = t.invoice_id
                JOIN inventory_management.source_type s 
                    ON s.source_type_id = t.source_type_id
                WHERE t.is_deleted = FALSE 
                 AND s.is_enabled = true
                 AND (t.purchase_order_id IS NOT NULL OR t.invoice_id IS NOT NULL)            
                GROUP BY po.external_reference, i.quantity, i.total, t.cost
            ),
            AGG_SALES_DATA_WITHOUT_SALE_ID AS (
                SELECT external_reference
                FROM SALES_DATA_WITHOUT_SALE_ID
                WHERE external_reference IS NOT NULL
                GROUP BY external_reference
            ),
            final_purchases AS (
                SELECT
                    cp.email,
                    COUNT(DISTINCT cp.order_number) AS total_orders_purchased,
                    SUM(cp.quantity)  AS total_quantity,
                    SUM(cp.total_price) AS total_purchase_cost
                FROM combined_purchases_view AS cp
                JOIN AGG_SALES_DATA_WITHOUT_SALE_ID AS asd
                    ON cp.order_number = asd.external_reference
                {where_clause.replace('fl', 'cp')}
                GROUP BY cp.email
            )
        """

        base_query = f"""
            {base_cte},
            purchases_data AS (
                SELECT 
                    fp.email as email,
                    fp.total_quantity as total_quantity,
                    fp.total_orders_purchased as total_orders_purchased,
                    fp.total_purchase_cost as total_purchase_cost,
                    fs.total_sales AS total_sales,
                    fs.total_sold_cost AS total_sold_cost,
                    fs.total_profit AS total_profit
                FROM final_sales fs
                JOIN final_purchases fp ON fp.email = fs.email
            ),
            cart_metrics AS (
                SELECT
                    c.email AS email,
                    COUNT(*) AS total_carts,
                    COUNT(CASE WHEN c.status = 'approve' THEN 1 END) AS approved_carts
                FROM browser_data_capture_cart c
                {where_clause.replace('fl.', 'c.').replace('c.created', 'TO_TIMESTAMP_NTZ(c.created_str)')}
                GROUP BY c.email
            ),
            base_data AS (
                SELECT
                    COALESCE(c.email, p.email) AS email,
                    COALESCE(p.total_orders_purchased, 0) AS total_orders_purchased,
                    COALESCE(p.total_quantity, 0) AS total_quantity,
                    COALESCE(p.total_purchase_cost, 0) AS total_purchase_cost,
                    COALESCE(p.total_sold_cost, 0) AS total_sold_cost,
                    COALESCE(p.total_sales, 0) AS total_sales,
                    COALESCE(p.total_profit, 0) AS total_profit,
                    COALESCE(c.total_carts, 0) AS total_carts,
                    COALESCE(c.approved_carts, 0) AS approved_carts
                FROM cart_metrics c
                RIGHT JOIN purchases_data p ON c.email = p.email
            ),
            top_performers AS (
                SELECT 
                    AVG(total_quantity) AS top_10_percent_avg
                FROM (
                    SELECT total_quantity, NTILE(10) OVER (ORDER BY total_quantity DESC) AS percentile
                    FROM base_data
                ) top_10
                WHERE percentile = 1
            )
            SELECT
                b.email AS "email",
                b.total_orders_purchased AS "total_orders_purchased",
                b.total_quantity AS "total_quantity",
                b.total_purchase_cost AS "total_purchase_cost",
                b.total_profit AS "total_profit",
                b.total_carts AS "total_carts",
                b.total_sold_cost AS "total_sold_cost",
                b.total_sales AS "total_sales",
                b.approved_carts AS "approved_carts",
                COALESCE(ROUND(CAST(b.total_orders_purchased AS FLOAT) / NULLIF(b.total_carts, 0) * 100, 2), 0) AS "conversion_rate",
                ROUND(CAST(b.total_quantity AS FLOAT) / NULLIF(tp.top_10_percent_avg, 0) * 100, 2) AS "top_performer_percentage"
            FROM base_data b
            CROSS JOIN top_performers tp
        """

        count_query = f"""
           {base_cte}
            SELECT COUNT(*) AS total
            FROM final_purchases
        """

        order_by_clause = f"ORDER BY {valid_sort_fields[sort_by]} {sort_order}"

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

            return {
                "items": [dict(r) for r in results],
                "total": total
            }

    except Exception as e:
        return {"error": str(e)}


async def get_buyer_reports_detail(
        search_term: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None
):
    try:
        conditions = []
        values = {}

        # Date filtering
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', fl.created) >= %(start_date)s")
            values["start_date"] = start_date

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', fl.created) <= %(end_date)s")
            values["end_date"] = end_date

        # Hour filtering
        if start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('DAY', fl.created) = %(current_date)s")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) >= %(start_hour)s")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM fl.created) <= %(end_hour)s")
                values["end_hour"] = end_hour

        # Search filtering
        if search_term:
            conditions.append("(fl.email ILIKE %(search)s)")
            values["search"] = f"%{search_term}%"

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Main Query
        query = f"""
            WITH SALES_DATA AS (
                SELECT
                    po.external_reference,
                    i.total AS total_price,
                    (t.cost * i.quantity) AS cost,
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
                    SUM(total_price) AS total_sales,
                    SUM(cost) AS total_cost,
                    SUM(profit) AS total_profit
                FROM SALES_DATA
                GROUP BY external_reference
            ),
            final AS (
                SELECT
                    cp.*,
                    asd.total_sales,
                    asd.total_cost AS total_sold_cost,
                    asd.total_profit
                FROM combined_purchases_view AS cp
                JOIN AGG_SALES_DATA AS asd
                    ON cp.order_number = asd.external_reference
            )
            SELECT
                fl.source AS "source",
                fl.created AS "created",
                fl.email AS "email",
                fl.order_number AS "order_number",
                fl.event_id AS "event_id",
                fl.event_name AS "event_name",
                fl.event_date_local AS "event_date_local",
                fl.url AS "url",
                fl.venue AS "venue",
                fl.city AS "city",
                fl.region AS "region",
                fl.country AS "country",
                fl.order_fee AS "order_fee",
                fl.section AS "section",
                fl."row" AS "row",
                fl.seat_names AS "seat_names",
                fl.is_general_admission AS "is_general_admission",
                fl.quantity AS "quantity",
                fl.price_per_ticket AS "price_per_ticket",
                fl.taxes_per_ticket AS "taxes_per_ticket",
                fl.service_charges_per_ticket AS "service_charges_per_ticket",
                fl.facility_charges_per_ticket AS "facility_charges_per_ticket",
                fl.total_price AS "total_price",
                fl.total_profit AS "total_profit",
                ROUND((fl.total_profit / NULLIF(fl.total_sold_cost, 0)) * 100, 2) AS "margin"
            FROM final fl
            {where_clause}
            ORDER BY fl.created DESC
        """

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, values)
            results = cur.fetchall()

            return [dict(r) for r in results]

    except Exception as e:
        print("Error executing query: %s", str(e))
        return {"error": str(e)}

async def get_overall_buyer_report(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None
):
    try:
        conditions = []
        values = {}

        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('day', created) >= :start_date")
            values["start_date"] = start_date

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('day', created) <= :end_date")
            values["end_date"] = end_date

        if start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('day', created) = :current_date")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM created) >= :start_hour")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM created) <= :end_hour")
                values["end_hour"] = end_hour

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT
                SUM(quantity) AS total_quantity,
                SUM(total_price) AS total_cost,
                COUNT(DISTINCT order_number) AS total_orders_purchased
            FROM browser_data_capture.combined_purchases_view
            {where_clause}
        """

        result = await get_pg_database().fetch_one(query, values)

        total_quantity = result["total_quantity"] if result["total_quantity"] is not None else 0
        total_orders_purchased = result["total_orders_purchased"] if result["total_orders_purchased"] is not None else 0
        total_cost = float(result["total_cost"]) if result["total_cost"] is not None else 0.0

        return {
            "total_quantity": total_quantity,
            "total_cost": total_cost,
            "total_orders_purchased": total_orders_purchased
        }

    except Exception as e:
        return {"error": str(e)}