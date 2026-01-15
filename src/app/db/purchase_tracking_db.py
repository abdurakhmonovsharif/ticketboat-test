import csv
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional

import snowflake
from starlette.responses import StreamingResponse

from app.database import get_snowflake_connection


def primary_issues_sort(sort: Optional[str]) -> str:
    try:
        field, direction = sort.split(":")
    except ValueError:
        raise ValueError("Invalid sort format. Expected 'field:direction'.")

    field_map = {
        "accountName": "account_name",
        "errorCount": "issue_count",
        "latestErrorDate": "latest_issue_time",
    }

    col = field_map.get(field)

    dir_map = {"ascend": "ASC", "descend": "DESC"}
    dir_sql = dir_map.get(direction, "ASC")

    return f"{col} {dir_sql}"


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


async def get_recent_purchases(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        marketplace: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        purchase_start_date: Optional[str] = None,
        purchase_end_date: Optional[str] = None,
        event_start_date: Optional[str] = None,
        event_end_date: Optional[str] = None,
        statuses: Optional[str] = None,
        company: Optional[str] = None,
):
    try:
        conditions = ["p.order_number IS NOT NULL AND p.order_number != ''"]

        base_sql = f"""
            SELECT
                CONVERT_TIMEZONE('UTC', '{timezone}', p.created) AS "created",
                p.email as "email",
                p.source as "type",
                p.order_number as "order_number",
                p.multilogin_profile as "multilogin_profile",
                tue.company as "company",
                p.event_id as "event_id",
                p.event_name as "event_name",
                p.event_date_local as "event_date_local",
                p.url as "url",
                p.venue as "venue",
                p.city as "city",
                p.region as "region",
                p.country as "country",
                p.order_fee as "order_fee",
                p.section as "seating_section",
                p."row" as "seating_row",
                p.seat_names as "seat_names",
                p.is_general_admission as "is_general_admission",
                p.quantity as "quantity",
                p.price_per_ticket as "price",
                p.taxes_per_ticket as "taxes",
                p.service_charges_per_ticket as "service_charges",
                p.facility_charges_per_ticket as "facility_charges",
                p.total_price as "total_price",
                CASE 
                    WHEN po.external_reference IS NOT NULL THEN 'MATCHED'
                    WHEN p.created >= CURRENT_TIMESTAMP() - INTERVAL '1 hour' THEN 'PENDING'
                    ELSE 'UNMATCHED'
                END as "status",
                ROW_NUMBER() OVER (PARTITION BY p.order_number ORDER BY p.created DESC) as rn
            FROM combined_purchases_view p
            LEFT JOIN inventory_management.purchase_order po 
                ON LEFT(TRIM(p.order_number), 20) = LEFT(TRIM(po.external_reference), 20)
            LEFT JOIN ticketboat_user_email tue
                ON p.multilogin_profile = tue.nickname
        """

        if purchase_start_date:
            start_dt = parse_datetime(purchase_start_date)
            conditions.append(f"p.created >= '{start_dt}'")

        if purchase_end_date:
            end_dt = parse_datetime(purchase_end_date)
            end_dt_plus_one = end_dt + timedelta(days=1)
            conditions.append(f"p.created <= '{end_dt_plus_one}'")

        if event_start_date:
            evt_start_dt = parse_datetime(event_start_date)
            conditions.append(f"p.event_date_local >= '{evt_start_dt}'")

        if event_end_date:
            evt_end_dt = parse_datetime(event_end_date)
            evt_end_dt_plus_one = evt_end_dt + timedelta(days=1)
            conditions.append(f"p.event_date_local <= '{evt_end_dt_plus_one}'")

        if search_term:
            search_term_lower = search_term.lower()
            conditions.append(f"""
                (
                    LOWER(p.event_name) LIKE '%{search_term_lower}%'
                    OR LOWER(p.venue) LIKE '%{search_term_lower}%'
                    OR LOWER(p.email) LIKE '%{search_term_lower}%'
                    OR LOWER(p.order_number) LIKE '%{search_term_lower}%'
                    OR LOWER(p.multilogin_profile) LIKE '%{search_term_lower}%'
                )
            """)

        if marketplace:
            conditions.append(f"LOWER(p.source) LIKE '%{marketplace.lower()}%'")

        if company:
            company_list = ", ".join([f"'{c.strip()}'" for c in company.split(",")])
            conditions.append(f"tue.company IN ({company_list})")

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        query_sql = f"""
            WITH purchases AS (
                {base_sql}
                {where_clause}
            )
            SELECT 
                "created", "email", "type", "order_number", "multilogin_profile", 
                "company", "event_id", "event_name", "event_date_local", "url", 
                "venue", "city", "region", "country", "order_fee", "seating_section", 
                "seating_row", "seat_names", "is_general_admission", "quantity", 
                "price", "taxes", "service_charges", "facility_charges", 
                "total_price", "status"
            FROM purchases 
            WHERE rn = 1
        """

        count_sql = f"""
            WITH purchases AS (
                {base_sql}
                {where_clause}
            )
            SELECT COUNT(*) AS total_count FROM purchases
            WHERE rn = 1
        """

        if statuses:
            status_list = ", ".join([f"'{s.strip().upper()}'" for s in statuses.split(",")])
            query_sql += f" AND \"status\" IN ({status_list})"
            count_sql += f" AND \"status\" IN ({status_list})"

        query_sql += " ORDER BY \"created\" DESC"

        if page and page_size:
            query_sql += f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_sql)
            total = cur.fetchone()['TOTAL_COUNT']

            # Get purchases data
            cur.execute(query_sql)
            purchases = cur.fetchall()

            return {
                "total": total,
                "items": [dict(r) for r in purchases]
            }

    except Exception as e:
        print(f"Error in get_recent_purchases: {str(e)}")
        return {
            "error": str(e)
        }


async def get_primary_issues(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account: Optional[str] = None,
    error: Optional[str] = None,
    primary: Optional[str] = None,
    sort: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
):
    """Gets the logged Primary issues from Snowflake"""
    try:
        clauses = []
        params = {}

        if start_date:
            clauses.append("CREATED >= TO_TIMESTAMP_NTZ(%(start_date)s)")
            params["start_date"] = start_date

        if end_date:
            clauses.append("CREATED <= TO_TIMESTAMP_NTZ(%(end_date)s)")
            params["end_date"] = end_date

        if account:
            clauses.append('DATA:"multilogin_profile"::STRING ILIKE %(account)s')
            params["account"] = f"{account}%"

        if error:
            clauses.append(
                'DATA:"scrapped":"fail_reason":"error_code"::STRING ILIKE %(error)s'
            )
            params["error"] = f"%{error}%"

        if primary:
            primary_values = [p.strip() for p in primary.split(",")]

            clauses.append(
                'DATA:"scrapped":"fail_reason":"primary"::STRING IN (%(primary_list)s)'
            )
            params["primary_list"] = primary_values

        where_sql = "WHERE " + " AND ".join(clauses) if clauses else "WHERE 1=1"

        sort_field, sort_direction = sort.split(":") if sort else (None, None)

        sort_field_lookup = {
            "created": "CREATED",
            "profile": 'DATA:"multilogin_profile"::STRING',
            "errorCode": 'DATA:"scrapped":"fail_reason":"error_code"::STRING',
            "email": "EMAIL",
            "platform": 'DATA:"scrapped":"fail_reason":"primary"::STRING',
        }

        sort_direction = (
            "DESC"
            if sort_direction == "descend"
            else "ASC" if sort_direction == "ascend" else None
        )

        if sort_field and sort_direction:
            order_sql = f"ORDER BY {sort_field_lookup[sort_field]} {sort_direction}"

        if page and limit:
            offset = (page - 1) * limit
            limit_offset_clause = f"LIMIT {limit} OFFSET {offset}"

        if where_sql:
            record_sql = f"""
                SELECT *
                FROM primary_issues
                {where_sql}
            {order_sql if sort_field and sort_direction else ""}
            {limit_offset_clause if limit_offset_clause else ""}
            """

            count_sql = f"""
                SELECT COUNT(*) AS total_count
                FROM primary_issues
                {where_sql}
            """
        else:
            return

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["TOTAL_COUNT"]

            cur.execute(record_sql, params)
            records = cur.fetchall()

            return {"total": total_count, "items": [r for r in records]}
    except Exception as e:
        return {"error": str(e)}


async def get_primary_issues_grouped_by_account(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account: Optional[str] = None,  # prefix match, case-insensitive
    error: Optional[str] = None,  # substring match, case-insensitive
    primary: Optional[str] = None,  # CSV list of exact matches, case-insensitive
    sort: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
):
    """Gets the logged Primary issues from Snowflake grouped by account"""
    order_by_clause = primary_issues_sort(sort) if sort else "latest_issue_time DESC"

    _limit = max(1, min(int(limit or 50), 500))
    _page = max(1, int(page or 1))
    _offset = (_page - 1) * _limit

    print(f"Limit: {_limit}, Page: {_page}, Offset: {_offset}")

    record_sql = f"""
        WITH t AS (
          SELECT
              ID,
              CREATED,
              TYPE,
              EMAIL,
              DATA,
              DATA:"multilogin_profile"::STRING AS multilogin_profile
          FROM primary_issues
          WHERE (%(start_date)s IS NULL OR CREATED >= TO_TIMESTAMP_NTZ(%(start_date)s))
            AND (%(end_date)s   IS NULL OR CREATED <= TO_TIMESTAMP_NTZ(%(end_date)s))
            AND (%(account)s    IS NULL OR DATA:"multilogin_profile"::STRING ILIKE %(account)s)
            AND (%(error)s      IS NULL OR DATA:"scrapped":"fail_reason":"error_code"::STRING ILIKE %(error)s)
            AND (
                 %(primary_csv)s IS NULL
                 OR EXISTS (
                      SELECT 1
                      FROM TABLE(SPLIT_TO_TABLE(%(primary_csv)s, ',')) s
                      WHERE TRIM(UPPER(s.value)) = UPPER(DATA:"scrapped":"fail_reason":"primary"::STRING)
                 )
            )
        ),
        g AS (
          SELECT
            COALESCE(multilogin_profile, 'UNKNOWN') AS account_name,
            ARRAY_AGG(
              OBJECT_CONSTRUCT(
                'ID', ID,
                'CREATED', CREATED,
                'TYPE', TYPE,
                'EMAIL', EMAIL,
                'DATA', DATA
              )
            ) WITHIN GROUP (ORDER BY CREATED DESC) AS records,
            COUNT(*) AS issue_count,
            MAX(CREATED) AS latest_issue_time
          FROM t
          GROUP BY COALESCE(multilogin_profile, 'UNKNOWN')
        ),
        g_total AS (
          SELECT g.*, COUNT(*) OVER() AS total_count
          FROM g
        )
        SELECT *
        FROM g_total
        ORDER BY {order_by_clause}, account_name ASC
        LIMIT {_limit} OFFSET {_offset};
    """

    params = {
        "start_date": start_date,                       # e.g. "2025-09-01 00:00:00" or None
        "end_date": end_date,                           # e.g. "2025-09-12 23:59:59" or None
        "account": f"{account}%" if account else None,  # prefix match
        "error": f"%{error}%" if error else None,       # contains match
        "primary_csv": primary if primary else None,    # pass CSV string as-is
    }

    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(record_sql, params)
            rows = cur.fetchall()
            total = rows[0]["TOTAL_COUNT"] if rows else 0
            items = [
                {
                    "ACCOUNT_NAME": r["ACCOUNT_NAME"],
                    "RECORDS": r["RECORDS"],
                    "ISSUE_COUNT": r["ISSUE_COUNT"],
                    "LATEST_ISSUE_TIME": r["LATEST_ISSUE_TIME"],
                }
                for r in rows
            ]
            return {"total": total, "items": items}
    except Exception as e:
        return {"error": str(e)}


def generate_csv_response(data, headers, filename_prefix):
    output = StringIO()
    writer = csv.writer(output)

    writer.writerow(headers)

    for item in data:
        writer.writerow([item[key] for key in headers])

    output.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
    )


async def get_purchase_report_by_email(
        email: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
        sort_by: Optional[str] = "created",
        sort_order: Optional[str] = "desc",
        page_size: Optional[int] = 50,
        page: Optional[int] = 1,
):
    try:
        if not email:
            return {"error": "Email is required"}

        conditions = ["email = %(email)s"]
        values = {"email": email}

        # Date filtering
        if start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', created) >= %(start_date)s")
            values["start_date"] = start_date

        if end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            conditions.append("DATE_TRUNC('DAY', created) <= %(end_date)s")
            values["end_date"] = end_date

        # Hour filtering
        if start_hour is not None or end_hour is not None:
            current_date = datetime.now().date()
            conditions.append("DATE_TRUNC('DAY', created) = %(current_date)s")
            values["current_date"] = current_date

            if start_hour is not None:
                conditions.append("EXTRACT(HOUR FROM created) >= %(start_hour)s")
                values["start_hour"] = start_hour

            if end_hour is not None:
                conditions.append("EXTRACT(HOUR FROM created) <= %(end_hour)s")
                values["end_hour"] = end_hour

        valid_sort_fields = {
            "created": "created",
            "order_number": "order_number",
            "total_price": "COALESCE(total_price, 0)",
            "event_date_local": "event_date_local"
        }

        if sort_by not in valid_sort_fields:
            sort_by = "created"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "desc"

        offset = (page - 1) * page_size

        base_cte = """
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
                WHERE t.is_deleted = FALSE
                 AND s.is_enabled = true             
                 AND (t.purchase_order_id IS NOT NULL OR t.invoice_id IS NOT NULL)            
                GROUP BY po.external_reference, i.quantity, i.total, t.cost
            ),
            AGG_SALES_DATA AS (
                SELECT
                    external_reference,
                    SUM(total) AS total_sales,
                    SUM(sold_cost) AS total_sold_cost,
                    SUM(profit) AS total_profit
                FROM SALES_DATA
                WHERE external_reference IS NOT NULL
                GROUP BY external_reference
            ),
            final AS (
                SELECT cp.*
                FROM combined_purchases_view AS cp
                JOIN AGG_SALES_DATA AS asd
                    ON cp.order_number = asd.external_reference
            )
        """

        main_query = f"""
            {base_cte}
            SELECT created as purchase_time,
               order_number as confirmation_number,
               venue AS venue,
               total_price AS cost,
               event_name AS performer,
               event_date_local AS date_time,
               multilogin_profile AS multilogin_profile
            FROM final
            WHERE {" AND ".join(conditions)}
            ORDER BY {valid_sort_fields[sort_by]} {sort_order}
            LIMIT %(page_size)s OFFSET %(offset)s
        """

        count_query = f"""
            {base_cte}
            SELECT COUNT(*) AS total
            FROM final
            WHERE {" AND ".join(conditions)}
        """

        values["page_size"] = page_size
        values["offset"] = offset

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:

            cur.execute(count_query, values)
            total = cur.fetchone()["TOTAL"]

            cur.execute(main_query, values)
            results = cur.fetchall()

            return {
                "items": [dict(r) for r in results],
                "total": total
            }

    except Exception as e:
        return {"error": str(e)}


async def get_invoice_report_by_email(
        email: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_hour: Optional[int] = None,
        end_hour: Optional[int] = None,
        sort_by: Optional[str] = "sale_date_time",
        sort_order: Optional[str] = "desc",
        page_size: Optional[int] = 50,
        page: Optional[int] = 1
):
    try:
        if not email:
            return {"error": "Email is required"}

        # only sold ones
        conditions = [
            "fl.email = %(email)s",
            "fl.sale_id IS NOT NULL",
        ]
        values = {"email": email}

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

        # Valid sort fields
        valid_sort_fields = {
            "performer": "fl.event_name",
            "date_time": "fl.event_date_local",
            "venue": "fl.venue",
            "cost": "COALESCE(fl.total_sold_cost, 0)",
            "profit": "COALESCE(fl.total_profit, 0)",
            "margin": "COALESCE(margin, 0)",
            "sale_date_time": "fl.created"
        }

        if sort_by not in valid_sort_fields:
            sort_by = "sale_date_time"

        sort_order = sort_order.lower()
        if sort_order not in {"asc", "desc"}:
            sort_order = "desc"

        # Pagination
        offset = (page - 1) * page_size

        # Main Query
        cte_block = f"""
            WITH SALES_DATA AS (
                SELECT
                    i.internal_id AS sale_id,
                    po.external_reference,
                    i.total AS total_price,
                    (t.cost * i.quantity) AS sold_cost,
                    i.total - (t.cost * i.quantity) AS profit,
                    i.invoice_date as sale_date
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
                GROUP BY i.internal_id, po.external_reference, i.quantity, i.total, t.cost, i.invoice_date
            ),
            AGG_SALES_DATA AS (
                SELECT
                    sale_id,
                    external_reference,
                    SUM(total_price) AS total_sales,
                    SUM(sold_cost) AS total_sold_cost,
                    SUM(profit) AS total_profit,
                    sale_date
                FROM SALES_DATA
                WHERE external_reference IS NOT NULL
                GROUP BY sale_id, external_reference, sale_date
            ),
            final AS (
                SELECT
                    cp.*,
                    asd.sale_id,
                    asd.total_sales,
                    asd.total_sold_cost,
                    asd.total_profit,
                    asd.sale_date
                FROM combined_purchases_view AS cp
                JOIN AGG_SALES_DATA AS asd
                    ON cp.order_number = asd.external_reference
            )
        """

        query = f"""
            {cte_block}
            SELECT
                fl.sale_id AS sale_id,
                fl.event_name AS performer,
                fl.event_date_local AS date_time,
                fl.venue AS venue,
                fl.total_sold_cost AS cost,
                fl.total_profit AS profit,
                ROUND((fl.total_profit / NULLIF(fl.total_sold_cost, 0)) * 100, 2) AS margin,
                fl.sale_date AS sale_date_time
            FROM final fl
            WHERE {" AND ".join(conditions)}
            ORDER BY {valid_sort_fields[sort_by]} {sort_order}
            LIMIT %(page_size)s OFFSET %(offset)s
        """

        count_query = f"""
            {cte_block}
            SELECT COUNT(*) AS total
            FROM final fl
            WHERE {" AND ".join(conditions)}
        """

        values["page_size"] = page_size
        values["offset"] = offset

        # Get total count

        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            # Get total count
            cur.execute(count_query, values)
            total = cur.fetchone()["TOTAL"]

            # Get paginated data
            cur.execute(query, values)
            results = cur.fetchall()

            return {
                "items": [dict(r) for r in results],
                "total": total
            }

    except Exception as e:
        return {"error": str(e)}
