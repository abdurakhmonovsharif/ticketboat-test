from typing import Optional, List, Tuple, Dict

import snowflake.connector

from app.database import get_snowflake_connection


async def get_onsale_email_details(
        timezone: str = "America/Chicago",
        page: int = 1,
        page_size: int = 50,
        search_term: Optional[str] = None,
        venues: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> dict:
    search_condition = create_search_condition_for_onsale_email_details(
        timezone, search_term, venues, start_date, end_date
    )
    limit_expression = ""
    if page and page_size:
        offset = (page - 1) * page_size
        limit_expression = f"LIMIT {page_size} OFFSET {offset}"

    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(
            f"""
                WITH filtered_emails AS (
                    SELECT *
                    FROM email_onsale
                    WHERE 1=1
                    {search_condition}
                ),
                event_discovery_dates AS (
                    SELECT
                        event_name,
                        event_datetime,
                        MIN(created) AS discovery_date
                    FROM filtered_emails
                    GROUP BY event_name, event_datetime
                ),
                latest_emails AS (
                    SELECT
                        fe.*,
                        ROW_NUMBER() OVER (PARTITION BY fe.event_name, fe.event_datetime ORDER BY fe.created DESC) AS rn
                    FROM filtered_emails fe
                )
                SELECT
                    le.id as "id",
                    le.venue as "venue",
                    le.performer as "performer",
                    le.promoter as "promoter",
                    le.discount_code as "discount_code",
                    le.presale_code as "presale_code",
                    le.price as "price",
                    le.email_id as "email_id",
                    CONVERT_TIMEZONE('UTC', '{timezone}', le.created) as "last_received",
                    le.event_name as "event_name",
                    le.event_datetime as "event_datetime",
                    le.onsale_or_presale_ts as "onsale_or_presale_ts",
                    CONVERT_TIMEZONE('UTC', '{timezone}', edd.discovery_date ) as "discovery_date",
                    le.event_url as "event_url"
                FROM latest_emails le
                JOIN event_discovery_dates edd ON le.event_name = edd.event_name AND le.event_datetime = edd.event_datetime
                WHERE le.rn = 1
                ORDER BY le.created DESC
                {limit_expression}
            """
        )

        email_details = cur.fetchall()
        email_details_total = await get_onsale_email_details_count(
            timezone, search_term, venues, start_date, end_date
        )

    return {"items": email_details, "total": email_details_total}


def create_search_condition_for_onsale_email_details(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        venues: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> str:
    search_conditions = []

    if search_term:
        search_conditions.append(
            f"""
            (LOWER(ifnull(event_name,'')) || LOWER(ifnull(venue,'')) || 
            LOWER(ifnull(performer,'')) || LOWER(ifnull(promoter,'')) || 
            LOWER(ifnull(discount_code,'')) || LOWER(ifnull(presale_code,'')) ||
            LOWER(ifnull(price::text,'')) || LOWER(ifnull(CONVERT_TIMEZONE('UTC', '{timezone}', event_datetime)::text,'')))
            LIKE CONCAT('%%', '{search_term.lower()}', '%%')
        """
        )
    if venues:
        venue_list = ", ".join([f"'{v.lower()}'" for v in venues])
        search_conditions.append(f"LOWER(venue) IN ({venue_list})")
    if start_date:
        search_conditions.append(f"event_datetime >= '{start_date}'")
    if end_date:
        search_conditions.append(f"event_datetime <= '{end_date}'")

    if search_conditions:
        return " AND " + " AND ".join(search_conditions)
    return ""


async def get_onsale_email_details_count(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        venues: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> int:
    search_condition = create_search_condition_for_onsale_email_details(
        timezone, search_term, venues, start_date, end_date
    )
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(
            f"""
                SELECT COUNT(1) AS cnt 
                FROM (
                    WITH filtered_emails AS (
                        SELECT *
                        FROM email_onsale
                        WHERE 1=1
                        {search_condition}
                    ),
                    event_discovery_dates AS (
                        SELECT
                            event_name,
                            event_datetime,
                            MIN(created) AS discovery_date
                        FROM filtered_emails
                        GROUP BY event_name, event_datetime
                    ),
                    latest_emails AS (
                        SELECT
                            fe.*,
                            ROW_NUMBER() OVER (PARTITION BY fe.event_name, fe.event_datetime ORDER BY fe.created DESC) AS rn
                        FROM filtered_emails fe
                    )
                    SELECT *
                    FROM latest_emails le
                    JOIN event_discovery_dates edd ON le.event_name = edd.event_name AND le.event_datetime = edd.event_datetime
                    WHERE le.rn = 1
                )
            """
        )
        return cur.fetchone()["CNT"]


async def get_onsale_email_venues() -> dict:
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT DISTINCT venue as "venue_name" FROM email_onsale;
            """
        )
        venues = cur.fetchall()
        venue_names = [v["venue_name"] for v in venues if v["venue_name"]]
        return {"items": venue_names, "total": len(venue_names)}


async def mark_emails_as_handled(event_ids: List[str]) -> Tuple[List[str], List[str]]:
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        try:
            id_list_str = ", ".join(map(str, event_ids))

            update_query = f"""
                UPDATE email_onsale
                SET is_handled = TRUE
                WHERE id IN ({id_list_str})
            """
            cur.execute(update_query)
            updated_count = cur.rowcount

            if updated_count == len(event_ids):
                return event_ids, []
            else:
                select_query = f"""
                    SELECT id FROM email_onsale
                    WHERE id IN ({id_list_str}) AND is_handled = TRUE
                """
                cur.execute(select_query)
                results = cur.fetchall()
                updated_ids = [row["id"] for row in results]
                failed_ids = list(set(event_ids) - set(updated_ids))
                return updated_ids, failed_ids
        except Exception as e:
            raise e


 
