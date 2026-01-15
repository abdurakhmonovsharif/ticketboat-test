import os
from datetime import datetime

from app.aws.dynamo_manager import get_dynamodb_manager
from app.database import get_snowflake_connection
from app.model.super_priority_req import SuperPriorityEventRequest


async def get_all_super_priority_list():
    with get_snowflake_connection().cursor() as cur:
        sql = """
        SELECT *
        FROM ticketmaster_super_priority
        """
        cur.execute(sql)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        result = [dict(zip(column_names, row)) for row in rows]
        return result


async def get_super_priority_event_seats(event_code):
    res = get_dynamodb_manager().get_items_with_id_and_sub_id_prefix(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                                                     f"ticketmaster_event#{event_code}", "section")
    return res


async def get_super_priority_event_listings(event_code):
    res = get_dynamodb_manager().get_items_with_id_and_sub_id_prefix(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                                                     f"ticketmaster_event#{event_code}",
                                                                     "viagogo_listing")
    return res


async def delete_super_priority_event(event_code):
    with get_snowflake_connection().cursor() as cur:
        sql = """
        DELETE FROM ticketmaster_super_priority
        WHERE event_code = %s
        """
        cur.execute(sql, (event_code,))

        return {"status": "success"}


async def create_super_priority_event(sp_input: SuperPriorityEventRequest):
    with get_snowflake_connection().cursor() as cur:
        count = _get_sp_count(cur)
        if count >= 5:
            return {"status": "failure", "message": "Cannot create more than 5 super priority events."}

        _sql = """
                INSERT INTO ticketmaster_super_priority (event_code, event_url, start_time)
                VALUES (%s, %s, %s)
                """

        cur.execute(_sql, (sp_input.event_code, sp_input.event_url, sp_input.start_time))
        return {"status": "success"}


def _get_sp_count(cur):
    sql = """
        SELECT COUNT(1) AS cnt
        FROM ticketmaster_super_priority
        """
    cur.execute(sql)

    return cur.fetchall()[0][0]
