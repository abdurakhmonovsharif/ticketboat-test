import time
import traceback
from datetime import datetime, timedelta
from os import environ
from typing import Optional, List, Dict, Any, Set

import snowflake.connector
from fastapi import HTTPException
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from pydantic import UUID4

from app.cache import invalidate_cache
from app.database import get_pg_readonly_database, get_snowflake_connection, get_pg_database, get_opensearch_client
from app.model.create_user_email import UserEmailUpdateRequest


async def update_tags(email_id: str, tags: List[str], page_size: int, page: int):
    tags_str = ",".join(tags)

    delete_sql = """
        DELETE FROM email.email_tag 
        WHERE email_id = :email_id
        AND tag NOT IN (SELECT unnest(string_to_array(:tags_str, ',')));
    """

    delete_values = {"email_id": email_id, "tags_str": tags_str}
    await get_pg_database().execute(delete_sql, delete_values)

    insert_sql = """
        INSERT INTO email.email_tag (email_id, tag)
        SELECT :email_id, unnest(string_to_array(:tags_str, ','))
        ON CONFLICT DO NOTHING;
    """

    insert_values = {"email_id": email_id, "tags_str": tags_str}
    await get_pg_database().execute(insert_sql, insert_values)

    await get_pg_database().execute(
        "UPDATE email.email SET updated_at = NOW() WHERE id = :email_id",
        {"email_id": email_id},
    )

    cache_key_prefix = f"email_list_v2/{page_size}/{page}/*"
    invalidate_cache(cache_key_prefix)


async def update_flags(email_id: str, flags_ids: List[str], has_archived: bool, user: str):
    edited_by = user if user and user.strip() else "UNK"
    flags_ids_str = ",".join(flags_ids)

    delete_sql = """
           DELETE FROM email.email_flag 
           WHERE email_id = :email_id
           AND flag_id NOT IN (SELECT unnest(string_to_array(:flags_ids, ',')));
       """

    delete_values = {"email_id": email_id, "flags_ids": flags_ids_str}
    await get_pg_database().execute(delete_sql, delete_values)

    insert_sql = """
        INSERT INTO email.email_flag (email_id, flag_id, edited_by)
        SELECT :email_id, unnest(string_to_array(:flags_ids, ',')), :edited_by
        ON CONFLICT DO NOTHING;
    """

    insert_values = {
        "email_id": email_id,
        "flags_ids": flags_ids_str,
        "edited_by": edited_by,
    }
    await get_pg_database().execute(insert_sql, insert_values)

    await get_pg_database().execute(
        "UPDATE email.email SET is_archived = :has_archived, updated_at = NOW() WHERE duplication_id = :email_id",
        {"email_id": email_id, "has_archived": has_archived},
    )

    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_flags_bulk(flags_ids: List[str], email_ids: List[str], user: str):
    edited_by = user if user and user.strip() else "UNK"
    flags_ids_str = ",".join(flags_ids)
    email_ids_str = ",".join(email_ids)

    update_sql = """
        INSERT INTO email.email_flag (email_id, flag_id, edited_by)
        SELECT e.email_id, f.flag_id, :edited_by
        FROM unnest(string_to_array(:email_ids, ',')) AS e(email_id)
        CROSS JOIN unnest(string_to_array(:flags_ids, ',')) AS f(flag_id)
        LEFT JOIN email.email_flag ef ON ef.email_id = e.email_id AND ef.flag_id = f.flag_id
        WHERE ef.email_id IS NULL
        ON CONFLICT DO NOTHING;
    """

    replace_values = {
        "email_ids": email_ids_str,
        "flags_ids": flags_ids_str,
        "edited_by": edited_by,
    }

    await get_pg_database().execute(update_sql, replace_values)

    archived_flag_query = """
        SELECT flag_id FROM email.flag WHERE flag_name = 'archived' LIMIT 1;
    """
    archived_flag_result = await get_pg_database().fetch_one(archived_flag_query)

    if archived_flag_result:
        archived_flag_id = archived_flag_result["flag_id"]

        if archived_flag_id in flags_ids:
            update_archived_sql = """
                UPDATE email.email
                SET is_archived = TRUE
                WHERE duplication_id = ANY(string_to_array(:email_ids, ','));
            """
            await get_pg_database().execute(update_archived_sql, {"email_ids": email_ids_str})

    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_flags_bulk_v2(flags_ids: List[str], email_ids: List[str], user: str):
    opensearch_client: OpenSearch = get_opensearch_client()
    edited_by = user if user and user.strip() else "UNK"

    archived_flag_query = """
        SELECT flag_id FROM email.flag WHERE flag_name = 'archived' LIMIT 1;
    """
    archived_flag_result = await get_pg_database().fetch_one(archived_flag_query)
    archived_flag_id = archived_flag_result["flag_id"]

    is_archived = archived_flag_id in flags_ids if archived_flag_id else False

    # Prepare bulk update operations
    bulk_operations = []
    for email_id in email_ids:
        # Get current document to ensure we're not overwriting existing flags
        try:
            current_doc = opensearch_client.get(index="emails", id=email_id)
            current_flags = current_doc["_source"].get("flags", [])

            # Add new flags (avoid duplicates)
            updated_flags = list(set(current_flags + flags_ids))

            # Prepare update operation
            update_op = {
                "update": {
                    "_index": "emails",
                    "_id": email_id
                }
            }

            doc = {
                "flags": updated_flags,
                "last_edited_by": edited_by,
                "last_updated": datetime.utcnow().isoformat()
            }

            # Set is_archived if the archived flag is included
            if is_archived:
                doc["is_archived"] = True

            bulk_operations.append(update_op)
            bulk_operations.append({"doc": doc})

        except Exception as e:
            # Log error but continue processing other emails
            print(f"Error updating email {email_id}: {str(e)}")

    # Execute bulk update if we have operations
    if bulk_operations:
        try:
            bulk(opensearch_client, bulk_operations)
        except Exception as e:
            print(f"Bulk update failed: {str(e)}")
            raise

    # Invalidate cache after updates
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_assigned_users(email_id: str, user_ids: List[str]):
    # Convert user_ids list to comma-separated string
    user_ids_str = ",".join(user_ids)

    # Delete existing assignments that are not in the new user_ids list
    delete_sql = """
        DELETE FROM email.email_assign_user 
        WHERE email_id = :email_id
        AND user_id NOT IN (SELECT unnest(string_to_array(:user_ids, ',')));
    """

    delete_values = {"email_id": email_id, "user_ids": user_ids_str}
    await get_pg_database().execute(delete_sql, delete_values)

    # Insert new assignments, ignoring any that already exist
    insert_sql = """
        INSERT INTO email.email_assign_user (email_id, user_id)
        SELECT :email_id, unnest(string_to_array(:user_ids, ','))
        ON CONFLICT DO NOTHING;
    """

    insert_values = {
        "email_id": email_id,
        "user_ids": user_ids_str,
    }
    await get_pg_database().execute(insert_sql, insert_values)

    # Update the email's updated_at timestamp
    await get_pg_database().execute(
        "UPDATE email.email SET updated_at = NOW() WHERE id = :email_id",
        {"email_id": email_id},
    )

    # Invalidate cache
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_email_star(email_id: str):
    toggle_query = """
    UPDATE email.email 
    SET is_starred = NOT is_starred
    WHERE duplication_id = :email_id
    """
    await get_pg_database().execute(query=toggle_query, values={"email_id": email_id})

    # Invalidate cache
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_individual_email_star_status(email_id: str, is_starred: bool):
    """
    Update star status for an individual email by email_id.
    This endpoint updates OpenSearch directly.
    """
    # Import EmailService here to avoid circular imports
    from app.service.email_service import EmailService
    
    email_service = EmailService()
    await getattr(email_service, 'update_individual_email_starred_opensearch')(email_id, is_starred)
    
    # Invalidate cache
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)
    
    return {
        "message": "Email star status updated successfully",
        "email_id": email_id,
        "is_starred": is_starred
    }


async def update_email_task(email_id: str):
    toggle_query = """
    UPDATE email.email 
    SET is_task_complete = NOT is_task_complete
    WHERE duplication_id = :email_id
    """
    await get_pg_database().execute(query=toggle_query, values={"email_id": email_id})

    # Invalidate cache
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def update_individual_email_task_status(email_id: str, is_task_complete: bool):
    """
    Update task completion status for an individual email by email_id.
    This endpoint updates OpenSearch directly.
    """
    # Import EmailService here to avoid circular imports
    from app.service.email_service import EmailService
    
    email_service = EmailService()
    await getattr(email_service, 'update_individual_email_task_complete_opensearch')(email_id, is_task_complete)
    
    # Invalidate cache
    cache_key_pattern = "email_list_v2/*"
    invalidate_cache(cache_key_pattern)
    
    return {
        "message": "Email task status updated successfully",
        "email_id": email_id,
        "is_task_complete": is_task_complete
    }


async def get_flags():
    sql = """
        SELECT flag_id as id, flag_name as name from email.flag order by flag_name 
    """
    res = await get_pg_database().fetch_all(sql)
    return [dict(r) for r in res]


def construct_sql_query(
        timezone: str = "America/Chicago",
        search_term: Optional[str] = None,
        filter_flags_array: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> str:
    flag_condition = ""
    if filter_flags_array:
        flag_conditions = [f"e.flags:{flag} = true" for flag in filter_flags_array]
        flag_condition = "AND (" + " OR ".join(flag_conditions) + ")"

    date_condition = ""
    if start_date and end_date:
        date_condition = f"AND e.created BETWEEN '{start_date}' AND '{end_date}'"

    base_query = f"""
    SELECT 
        e.from_email AS from_email,
        e.subject AS subject,
        convert_timezone('UTC', '{timezone}', e.created) as created,
        e.tags as tags,
        e.flags as flags,
        e.to_email as to_email
    FROM email e
    LEFT JOIN ticketboat_user_email te
        ON lower(e.to_email) = lower(te.gmail_login)
    """

    where_clause = "WHERE 1=1 "
    if search_term:
        search_term_condition = f"""
        AND (LOWER(ifnull(e.subject,'')) || LOWER(ifnull(e.body:plain::text,'')) ||
             LOWER(ifnull(e.summary,'')) || LOWER(ifnull(e.from_email,'')) ||
             LOWER(ifnull(e.to_email,'')) || LOWER(ifnull(e.flags::text,'')) ||
             LOWER(ifnull(e.tags::text,'')))
            LIKE CONCAT('%%', LOWER('{search_term}'), '%%')
        """
        where_clause += search_term_condition

    where_clause += f"{flag_condition} {date_condition}"

    order_by_clause = "ORDER BY e.created DESC"

    limit = "LIMIT 100000"

    final_query = f"{base_query} {where_clause} {order_by_clause} {limit}"

    return final_query


async def get_email_list_v2(
        timezone: str = "America/Chicago",
        page: int = 1,
        page_size: int = 100,
        search_term: Optional[str] = None,
        filter_flags: Optional[List[str]] = None,
        filter_users: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        from_email: Optional[str] = None,
        to_email: Optional[str] = None,
        subject: Optional[str] = None,
        search_in: Optional[str] = 'inbox'
):
    if search_term or from_email or to_email or subject:
        return await get_email_list_v3(
            timezone,
            page,
            page_size,
            search_term,
            filter_flags,
            filter_users,
            start_date,
            end_date,
            from_email,
            to_email,
            subject,
            search_in
        )
    offset = (page - 1) * page_size if page and page_size else 0

    params = {"timezone": timezone}
    group_conditions = []
    email_conditions = []
    search_conditions = []
    search_params = {}

    if search_in != 'all':
        if search_in == 'inbox' or not search_in:
            group_conditions.append("e.is_archived = FALSE")
        elif search_in == 'starred':
            group_conditions.append("e.is_starred = TRUE")
        elif search_in == 'archived':
            group_conditions.append("e.is_archived = TRUE")
    # get users from postgres table called users
    if from_email:
        group_conditions.append("e.from_email ILIKE :from_email")
        params["from_email"] = f"%{from_email}%"

    if subject:
        group_conditions.append("e.subject ILIKE :subject")
        params["subject"] = f"%{subject}%"

    if filter_flags:
        if "no_flags" in filter_flags:
            # Handle both no_flags and other flags together
            other_flags = [flag for flag in filter_flags if flag != "no_flags"]
            
            if other_flags:
                # Include both emails with no flags AND emails with specific flags
                group_conditions.append("""
                    (NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)
                     OR e.duplication_id IN (
                         SELECT DISTINCT email_id
                         FROM email.email_flag ef
                         JOIN email.flag f ON ef.flag_id = f.flag_id
                         WHERE f.flag_name = ANY(:flag_names)
                     ))
                """)
                params["flag_names"] = other_flags
                cte_start = """
                       WITH flagged_emails AS (
                           SELECT DISTINCT email_id
                           FROM email.email_flag ef
                           JOIN email.flag f ON ef.flag_id = f.flag_id
                           WHERE f.flag_name = ANY(:flag_names)
                       ),"""
                join_condition = " LEFT JOIN flagged_emails fe ON e.duplication_id = fe.email_id "
            else:
                # Only no_flags specified
                group_conditions.append(
                    "NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)"
                )
                cte_start = " WITH "
                join_condition = " "
        else:
            params["flag_names"] = filter_flags
            cte_start = """
                   WITH flagged_emails AS (
                       SELECT DISTINCT email_id
                       FROM email.email_flag ef
                       JOIN email.flag f ON ef.flag_id = f.flag_id
                       WHERE f.flag_name = ANY(:flag_names)
                   ),"""
            join_condition = " JOIN flagged_emails fe ON e.duplication_id = fe.email_id "
    else:
        cte_start = " WITH "
        join_condition = " "

    if filter_users:
        if "no_users" in filter_users:
            group_conditions.append(
                "NOT EXISTS (SELECT 1 FROM email.email_assign_user WHERE email_id = e.duplication_id)"
            )
        else:
            group_conditions.append("""
                e.duplication_id IN (
                    SELECT DISTINCT email_id 
                    FROM email.email_assign_user eu
                    JOIN "user" u ON eu.user_id = u.email
                    WHERE u.email = ANY(:user_emails)
                )""")
            params["user_emails"] = filter_users

    if start_date and end_date:
        email_conditions.append("e.created BETWEEN :start_date AND :end_date")
        params["start_date"] = datetime.strptime(start_date, "%Y-%m-%d")
        params["end_date"] = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    if to_email:
        email_conditions.append("e.to_email ILIKE :to_email")
        params["to_email"] = f"%{to_email}%"

    if search_term:
        search_conditions.extend([
            "e.subject ILIKE :search_term",
            "e.from_email ILIKE :search_term",
            "e.to_email ILIKE :search_term",
            "tue.nickname ILIKE :search_term",
            "eb.body_tsvector @@ plainto_tsquery('english', :search_term)",
        ])
        search_params["search_term"] = f"%{search_term}%"

    group_where_clause = " AND ".join(group_conditions) if group_conditions else "TRUE"
    email_where_clause = " AND ".join(email_conditions) if email_conditions else "TRUE"

    if search_conditions:
        all_joins = []
        if any('tsvector' in condition for condition in search_conditions):
            all_joins.append("LEFT JOIN email.email_body eb ON e.id = eb.email_id")
        if any('tue.nickname' in condition for condition in search_conditions):
            all_joins.append("LEFT JOIN email.ticketboat_user_email tue ON e.to_email = tue.gmail_login")

        join_clause = " ".join(all_joins)

        # Combine all search conditions with OR
        combined_conditions = " OR ".join(f"({condition})" for condition in search_conditions)

        # Generate single query
        base_email_data_sql = f"""
        SELECT
            e.from_email AS "from",
            e.duplication_id AS "id",
            e.subject AS "subject",
            timezone(:timezone, MAX(e.created)) AS "last_received"
        FROM email.email e
        {join_condition} 
        {join_clause}
        WHERE {group_where_clause} 
        AND ({combined_conditions})
        AND {email_where_clause}
        GROUP BY e.duplication_id, e.from_email, e.subject, e.is_starred 
        ORDER BY MAX(e.created) DESC
        LIMIT :limit OFFSET :offset
        """
    else:
        base_email_data_sql = f"""
        SELECT
            e.from_email AS "from",
            e.duplication_id AS "id",
            e.subject AS "subject", 
            timezone(:timezone, MAX(e.created)) AS "last_received"
        FROM email.email e
        {join_condition}
        WHERE {group_where_clause} AND {email_where_clause}
        GROUP BY e.duplication_id, e.from_email, e.subject, e.is_starred 
        ORDER BY MAX(e.created) DESC
        LIMIT :limit OFFSET :offset
        """

    # Count query
    count_sql = f"""
    {cte_start}
    base_email_data AS ({base_email_data_sql})
    SELECT COUNT(DISTINCT "id") FROM base_email_data
    """

    total_count = await get_pg_readonly_database().fetch_val(
        query=count_sql,
        values={**params, **search_params}
    )

    # Main query
    main_sql = f"""
    {cte_start}
    base_email_data AS ({base_email_data_sql}),
    email_data AS (
        SELECT
            b."from",
            b."id",
            e."is_starred",
            b."subject",
            b."last_received",
            ARRAY_AGG(DISTINCT jsonb_build_object(
                'id', e.id,
                'to', e.to_email,
                'created', timezone(:timezone, e.created),
                'summary', e.summary,
                'is_read', e.is_read,
                'comment_count', (SELECT COUNT(*) FROM email.comments WHERE email_id IN (e.id, b."id")),
                'to_nickname', tue.nickname
            )) AS "emails",
            COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                'filter_id', af.filter_id,
                'archive', efi.archive,
                'mark_as_read', efi.mark_as_read,
                'star', efi.star,
                'add_comment', efi.add_comment,
                'flags', efi.flags,
                'users', efi.users,
                'from', efi."from",
                'to', efi."to",
                'subject', efi.subject,
                'does_not_have', efi.does_not_have,
                'search_term', efi.search_term
            )) FILTER (WHERE efi.id IS NOT NULL), '[]'::jsonb) AS "applied_filters",
            COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                'flag_id', ef.flag_id,
                'flag_name', f.flag_name,
                'edited_by', ef.edited_by
            )) FILTER (WHERE f.flag_name IS NOT NULL), '[]'::jsonb) AS "flags",
            COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                'user_id', u.email,
                'assigned_user', u.name
            )) FILTER (WHERE u.email IS NOT NULL), '[]'::jsonb) AS "assigned_users",
            (SELECT COUNT(*) FROM email.comments WHERE email_id = b."id") AS "total_comment_count"
        FROM base_email_data b
        JOIN email.email e ON e.duplication_id = b."id" AND {group_where_clause} AND {email_where_clause}
        LEFT JOIN email.email_flag ef ON b."id" = ef.email_id
        LEFT JOIN email.flag f ON ef.flag_id = f.flag_id
        LEFT JOIN email.email_assign_user eau ON b."id" = eau.email_id
        LEFT JOIN "user" u ON eau.user_id = u.email
        LEFT JOIN email.ticketboat_user_email tue ON e.to_email = tue.gmail_login
        LEFT JOIN email.applied_filter af ON b."id" = af.email_duplication_id
        LEFT JOIN email_filter efi ON efi.id = af.filter_id
        GROUP BY b."from", b."id", e."is_starred", b."subject", b."last_received"
    )
    SELECT 
        "from",
        "id",
        "applied_filters",
        "subject",
        "is_starred",
        "last_received",
        "emails",
        "flags" AS "flags",
        "assigned_users" AS "assigned_users",
        "total_comment_count"
    FROM email_data
    ORDER BY "last_received" DESC
    """

    params["limit"] = page_size
    params["offset"] = offset

    async with get_pg_readonly_database().transaction():
        await get_pg_readonly_database().execute("SET work_mem = '256MB';")
        try:
            print('MAIN_SQL: ', {'MAIN_SQL': main_sql, 'PARAMS': {**params, **search_params}})
            start_db_time = time.time()
            results = await get_pg_database().fetch_all(
                query=main_sql,
                values={**params, **search_params}
            )
            db_duration = time.time() - start_db_time
        except Exception as e:
            db_duration = time.time() - start_db_time
            print(f"DB fetch time (error out): {db_duration:.3f} seconds")
            raise
        finally:
            await get_pg_readonly_database().execute("RESET work_mem;")

    start_transform_time = time.time()
    items = [dict(r) for r in results]
    transform_duration = time.time() - start_transform_time

    print(f"DB fetch time: {db_duration:.3f} seconds")
    print(f"Transform time: {transform_duration:.3f} seconds")
    return {"total": total_count, "items": items}


async def get_email_list_v3(
        timezone: str = "America/Chicago",
        page: int = 1,
        page_size: int = 100,
        search_term: Optional[str] = None,
        filter_flags: Optional[List[str]] = None,
        filter_users: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        from_email: Optional[str] = None,
        to_email: Optional[str] = None,
        subject: Optional[str] = None,
        search_in: Optional[str] = 'inbox'
):
    try:
        search_term = search_term.lower()
        subject = subject.lower()
        print({"search_term": search_term, "from_email": from_email, "to_email": to_email, "subject": subject})
        offset = (page - 1) * page_size if page and page_size else 0

        params = {"timezone": timezone}
        group_conditions = []
        email_conditions = []
        search_conditions = []
        cte_conditions = []
        cte_joins = []
        search_params = {}
        print("start_date", start_date)
        print("end_date", end_date)

        if start_date:
            start_date = f"{start_date}T00:00:00Z"
        if end_date:
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            end_date = end_date_dt.strftime("%Y-%m-%dT00:00:00Z")

        duplication_ids: List[str] = await get_duplication_ids_from_opensearch(
            search_in,
            subject,
            from_email,
            to_email,
            start_date,
            end_date,
            search_term
        )

        if not duplication_ids:
            return {"total": 0, "items": []}

        params["available_duplication_ids"] = duplication_ids

        if search_in != 'all':
            if search_in == 'inbox' or not search_in:
                group_conditions.append("e.is_archived = FALSE")
            elif search_in == 'starred':
                group_conditions.append("e.is_starred = TRUE")
            elif search_in == 'archived':
                group_conditions.append("e.is_archived = TRUE")

        if from_email:
            group_conditions.append("e.from_email ILIKE :from_email")
            params["from_email"] = f"%{from_email}%"

        if to_email:
            email_conditions.append("e.to_email ILIKE :to_email")
            params["to_email"] = f"%{to_email}%"

        if start_date and end_date:
            email_conditions.append("e.created >= :start_date AND e.created < :end_date")
            params["start_date"] = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
            params["end_date"] = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

        if subject:
            group_conditions.append("e.subject ILIKE :subject")
            params["subject"] = f"%{subject}%"

        if filter_flags:
            if "no_flags" in filter_flags:
                # Handle both no_flags and other flags together
                other_flags = [flag for flag in filter_flags if flag != "no_flags"]
                
                if other_flags:
                    # Include both emails with no flags AND emails with specific flags
                    group_conditions.append("""
                        (NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)
                         OR f.flag_name = ANY(:flag_names))
                    """)
                    cte_conditions.append("""
                        (NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)
                         OR f.flag_name = ANY(:flag_names))
                    """)
                    params["flag_names"] = other_flags
                    cte_joins.append(
                        "LEFT JOIN email.email_flag ef ON e.duplication_id = ef.email_id "
                        "LEFT JOIN email.flag f ON ef.flag_id = f.flag_id"
                    )
                else:
                    # Only no_flags specified
                    group_conditions.append(
                        "NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)"
                    )
                    cte_conditions.append(
                        "NOT EXISTS (SELECT 1 FROM email.email_flag WHERE email_id = e.duplication_id)"
                    )
            else:
                params["flag_names"] = filter_flags
                group_conditions.append("f.flag_name = ANY(:flag_names)")
                cte_conditions.append("f.flag_name = ANY(:flag_names)")
                cte_joins.append(
                    "LEFT JOIN email.email_flag ef ON e.duplication_id = ef.email_id "
                    "LEFT JOIN email.flag f ON ef.flag_id = f.flag_id"
                )

        if filter_users:
            if "no_users" in filter_users:
                group_conditions.append(
                    "NOT EXISTS (SELECT 1 FROM email.email_assign_user WHERE email_id = e.duplication_id)"
                )
                cte_conditions.append(
                    "NOT EXISTS (SELECT 1 FROM email.email_assign_user WHERE email_id = e.duplication_id)"
                )
            else:
                group_conditions.append(
                    """
                    e.duplication_id IN (
                        SELECT DISTINCT email_id 
                        FROM email.email_assign_user eu
                        JOIN "user" u ON eu.user_id = u.email
                        WHERE u.email = ANY(:user_emails)
                    )"""
                )
                cte_conditions.append(
                    """
                    e.duplication_id IN (
                        SELECT DISTINCT email_id 
                        FROM email.email_assign_user eu
                        JOIN "user" u ON eu.user_id = u.email
                        WHERE u.email = ANY(:user_emails)
                    )"""
                )
                params["user_emails"] = filter_users

        search_join = ''
        if search_term:
            search_conditions.extend([
                "e.subject ILIKE :search_term",
                "e.from_email ILIKE :search_term",
                "e.to_email ILIKE :search_term",
                "tue.nickname ILIKE :search_term",
                "eb.body_tsvector @@ plainto_tsquery('english', :search_term)",
            ])
            search_join = "LEFT JOIN email.email_body eb ON e.id = eb.email_id"
            cte_joins.extend([
                "LEFT JOIN email.ticketboat_user_email tue ON e.to_email = tue.gmail_login",
                search_join
            ])
            search_params["search_term"] = f"%{search_term}%"

        search_condition_clause = " ( " + " OR ".join(search_conditions) + " ) " if search_conditions else "TRUE"

        email_conditions.append(search_condition_clause)
        email_conditions.extend(group_conditions)
        email_where_clause = " AND ".join(email_conditions) if email_conditions else "TRUE"

        cte_combined_conditions = (
            "WHERE " + " AND ".join(f"({condition})" for condition in cte_conditions)
            if cte_conditions
            else ""
        )
        cte_combined_joins = " ".join(cte_joins)

        base_email_data_sql = f"""
        SELECT
            e.from_email AS "from",
            e.duplication_id AS "id",
            e.subject AS "subject",
            timezone(:timezone, MAX(e.created)) AS "last_received"
        FROM available_emails e
        {cte_combined_joins} 
        {cte_combined_conditions} 
        GROUP BY e.duplication_id, e.from_email, e.subject, e.is_starred, e.is_task_complete 
        ORDER BY MAX(e.created) DESC
        LIMIT :limit OFFSET :offset
        """

        total_count = len(duplication_ids)

        main_sql = f"""
        WITH available_emails AS (
            SELECT * FROM email.email where duplication_id = ANY(:available_duplication_ids)
        ),
        base_email_data AS ({base_email_data_sql}),
        email_data AS (
            SELECT
                b."from",
                b."id",
                e."is_starred",
                e."is_task_complete",
                b."subject",
                b."last_received",
                ARRAY_AGG(DISTINCT jsonb_build_object(
                    'id', e.id,
                    'to', e.to_email,
                    'created', timezone(:timezone, e.created),
                    'summary', e.summary,
                    'is_read', e.is_read,
                    'comment_count', (SELECT COUNT(*) FROM email.comments WHERE email_id IN (e.id, b."id")),
                    'to_nickname', tue.nickname
                )) AS "emails",
                COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                    'filter_id', af.filter_id,
                    'archive', efi.archive,
                    'mark_as_read', efi.mark_as_read,
                    'star', efi.star,
                    'add_comment', efi.add_comment,
                    'flags', efi.flags,
                    'users', efi.users,
                    'from', efi."from",
                    'to', efi."to",
                    'subject', efi.subject,
                    'does_not_have', efi.does_not_have,
                    'search_term', efi.search_term
                )) FILTER (WHERE efi.id IS NOT NULL), '[]'::jsonb) AS "applied_filters",
                COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                    'flag_id', ef.flag_id,
                    'flag_name', f.flag_name,
                    'edited_by', ef.edited_by
                )) FILTER (WHERE f.flag_name IS NOT NULL), '[]'::jsonb) AS "flags",
                COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
                    'user_id', u.email,
                    'assigned_user', u.name
                )) FILTER (WHERE u.email IS NOT NULL), '[]'::jsonb) AS "assigned_users",
                (SELECT COUNT(*) FROM email.comments WHERE email_id = b."id") AS "total_comment_count"
            FROM base_email_data b
            JOIN email.email e ON e.duplication_id = b."id"
            LEFT JOIN email.email_flag ef ON b."id" = ef.email_id
            LEFT JOIN email.flag f ON ef.flag_id = f.flag_id
            LEFT JOIN email.email_assign_user eau ON b."id" = eau.email_id
            LEFT JOIN "user" u ON eau.user_id = u.email
            LEFT JOIN email.ticketboat_user_email tue ON e.to_email = tue.gmail_login
            LEFT JOIN email.applied_filter af ON b."id" = af.email_duplication_id
            LEFT JOIN email_filter efi ON efi.id = af.filter_id
            {search_join}
            WHERE {email_where_clause}
            GROUP BY b."from", b."id", e."is_starred", e."is_task_complete", b."subject", b."last_received"
        )
        SELECT 
            "from",
            "id",
            "applied_filters",
            "subject",
            "is_starred",
            "is_task_complete",
            "last_received",
            "emails",
            "flags" AS "flags",
            "assigned_users" AS "assigned_users",
            "total_comment_count"
        FROM email_data
        ORDER BY "last_received" DESC
        """

        params["limit"] = page_size
        params["offset"] = offset

        async with get_pg_readonly_database().transaction():
            await get_pg_readonly_database().execute("SET work_mem = '256MB';")
            try:
                print('MAIN_SQL: ', {'MAIN_SQL': main_sql, 'PARAMS': {**params, **search_params}})
                start_db_time = time.time()
                results = await get_pg_database().fetch_all(
                    query=main_sql,
                    values={**params, **search_params}
                )
                db_duration = time.time() - start_db_time
            except Exception as e:
                db_duration = time.time() - start_db_time
                print(f"DB fetch time (error out): {db_duration:.3f} seconds")
                raise
            finally:
                await get_pg_readonly_database().execute("RESET work_mem;")

        start_transform_time = time.time()
        items = [dict(r) for r in results]
        transform_duration = time.time() - start_transform_time

        print(f"DB fetch time: {db_duration:.3f} seconds")
        print(f"Transform time: {transform_duration:.3f} seconds")
        return {"total": total_count, "items": items}
    except Exception as e:
        print("Exception in get_email_list_v3:", str(e))
        traceback.print_exc()


async def get_duplication_ids_from_opensearch(
        search_in: str = 'inbox',
        subject: Optional[str] = None,
        from_email: Optional[str] = None,
        to_email: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_term: Optional[str] = None,
        page_size: int = 10000
) -> List[str]:
    must_conditions = []
    if search_in == 'inbox':
        must_conditions.append({"term": {"is_archived": False}})
    elif search_in == 'starred':
        must_conditions.append({"term": {"is_starred": True}})
    elif search_in == 'archived':
        must_conditions.append({"term": {"is_archived": True}})

    if subject:
        must_conditions.append({"match_phrase": {"subject": subject}})
    if from_email:
        must_conditions.append({"match_phrase": {"from_email": from_email}})
    if to_email:
        must_conditions.append({"match_phrase": {"to_email": to_email}})
    if start_date or end_date:
        range_filter = {}
        if start_date:
            range_filter["gte"] = start_date
        if end_date:
            range_filter["lt"] = end_date
        must_conditions.append({"range": {"created": range_filter}})

    query_body = {
        "query": {
            "bool": {
                "must": must_conditions,
                "must_not": [
                    {"term": {"is_deleted": True}}
                ]
            }
        },
        "aggs": {
            "unique_duplication_ids": {
                "composite": {
                    "size": page_size,
                    "sources": [
                        {"duplication_id": {"terms": {"field": "duplication_id.keyword"}}}
                    ]
                },
                "aggs": {}
            }
        },
        "size": 0
    }

    if search_term:
        query_body["aggs"]["unique_duplication_ids"]["aggs"] = {
            "matched_search": {
                "filter": {
                    "bool": {
                        "should": [
                            {"match_phrase": {"subject": search_term}},
                            {"match_phrase": {"from_email": search_term}},
                            {"match_phrase": {"to_email": search_term}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
        }

    main_ids: Set[str] = set()
    metadata_ids: Set[str] = set()
    after = None
    while True:
        if after:
            query_body["aggs"]["unique_duplication_ids"]["composite"]["after"] = after

        response_json = await execute_opensearch_query("email", query_body)
        buckets = response_json["aggregations"]["unique_duplication_ids"]["buckets"]
        for bucket in buckets:
            dup_id = bucket["key"]["duplication_id"]
            main_ids.add(dup_id)
            if search_term:
                if bucket.get("matched_search", {}).get("doc_count", 0) > 0:
                    metadata_ids.add(dup_id)

        if "after_key" in response_json["aggregations"]["unique_duplication_ids"]:
            after = response_json["aggregations"]["unique_duplication_ids"]["after_key"]
        else:
            break

    body_ids: Set[str] = set()
    if search_term:
        query_body_body = {
            "query": {
                "match_phrase": {"body": search_term}
            },
            "aggs": {
                "unique_duplication_ids": {
                    "composite": {
                        "size": page_size,
                        "sources": [
                            {"duplication_id": {"terms": {"field": "duplication_id.keyword"}}}
                        ]
                    }
                }
            },
            "size": 0
        }
        after = None
        while True:
            if after:
                query_body_body["aggs"]["unique_duplication_ids"]["composite"]["after"] = after

            body_response = await execute_opensearch_query("email_body", query_body_body)
            buckets_body = body_response["aggregations"]["unique_duplication_ids"]["buckets"]
            for bucket in buckets_body:
                body_ids.add(bucket["key"]["duplication_id"])
            if "after_key" in body_response["aggregations"]["unique_duplication_ids"]:
                after = body_response["aggregations"]["unique_duplication_ids"]["after_key"]
            else:
                break

    if search_term:
        final_ids = main_ids.intersection(metadata_ids.union(body_ids))
    else:
        final_ids = main_ids

    return list(final_ids)


_opensearch_client = None


async def execute_opensearch_query(index: str, body: Dict[str, Any]) -> Dict[str, Any]:
    global _opensearch_client
    if not _opensearch_client:
        _opensearch_client = OpenSearch(
            hosts=[{'host': environ["OPENSEARCH_ENDPOINT"], 'port': 443}],
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=30
        )
    return _opensearch_client.search(index=index, body=body)


async def email_by_id(email_id: str):
    sql = """
        SELECT
            COALESCE((e_body.body::jsonb)->>'html', (e_body.body::jsonb)->>'plain') as "html"
        FROM email.email e
        JOIN email.email_body e_body ON e.id = e_body.email_id
        WHERE e.id = :email_id;
    """
    values = {"email_id": email_id}

    res = await get_pg_readonly_database().fetch_one(sql, values)

    if res:
        return res["html"]
    else:
        return "Email not found"


def get_email_list_count(search_term: Optional[str] = None) -> int:
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        if search_term:
            search_term = search_term.lower()
            cur.execute(
                """
                SELECT COUNT(1) AS cnt
                FROM email e
                    left join (
                        select e.id
                              ,object_agg(f.key, f.value) true_flags
                        from email e, lateral flatten(input => e.flags) f
                        where e.flags is not null
                          and f.value = true
                        group by 1
                    ) f on f.id = e.id
                WHERE CONCAT(LOWER(e.subject), LOWER(e.body:plain::text), LOWER(e.summary), LOWER(e.from_email), LOWER(e.to_email), LOWER(ifnull(f.true_flags::text,'')), LOWER(ifnull(e.tags::text,'')))
                        LIKE CONCAT('%%', %(search_term)s, '%%')
                """,
                {"search_term": search_term},
            )
        else:
            cur.execute("SELECT COUNT(1) AS cnt FROM email")  # No search filter

        return cur.fetchall()[0]["CNT"]


async def mark_read(email_id: str, is_read: bool):
    update_sql = """
        UPDATE email.email
        SET is_read = :is_read, updated_at = NOW()
        WHERE id = :email_id
    """

    update_values = {"is_read": is_read, "email_id": email_id}
    await get_pg_database().execute(update_sql, update_values)

    cache_key_pattern = f"email_list_v2/*"
    invalidate_cache(cache_key_pattern)


async def add_comment(group_id: str, text: str, current_user_email: str):
    query = """
        INSERT INTO email.comments (id, email_id, text, current_user_email, created_at)
        VALUES (uuid_generate_v4(),:group_id, :text, :current_user_email, current_timestamp)
    """

    values = {
        "group_id": group_id,
        "text": text,
        "current_user_email": current_user_email,
    }

    try:
        await get_pg_database().execute(query, values)
        cache_key_pattern = "email_list_v2/*"
        invalidate_cache(cache_key_pattern)
        return True
    except Exception as e:
        print(f"Failed to add comment: {e}")
        return False


async def get_comments_by_group_ids(group_ids: List[str]) -> list[dict]:
    query = """
           SELECT id, email_id, text, current_user_email, created_at
           FROM email.comments
           WHERE email_id = ANY(:group_ids)
           ORDER BY created_at ASC  
       """
    values = {"group_ids": group_ids}
    try:
        rows = await get_pg_database().fetch_all(query, values)
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        print(f"Failed to fetch comments: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


def create_search_condition_for_onsale_email_details(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        venues: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> str:
    search_conditions = []

    if search_term:
        # Escape single quotes in search term to prevent SQL injection
        escaped_search_term = search_term.replace("'", "''")
        search_conditions.append(
            f"""
            (LOWER(COALESCE(event_name,'')) || LOWER(COALESCE(venue,'')) || 
            LOWER(COALESCE(performer,'')) || LOWER(COALESCE(promoter,'')) || 
            LOWER(COALESCE(discount_code,'')) || LOWER(COALESCE(presale_code,'')) ||
            LOWER(COALESCE(price::text,'')))
            ILIKE '%{escaped_search_term.lower()}%'
        """
        )
    if venues:
        # Filter out None and empty string values and escape quotes
        valid_venues = [v.replace("'", "''") for v in venues if v is not None and v.strip()]
        if valid_venues:
            venue_list = ", ".join([f"'{v.lower()}'" for v in valid_venues])
            search_conditions.append(f"LOWER(venue) IN ({venue_list})")
    if start_date:
        # Basic validation for date format (YYYY-MM-DD)
        if start_date and len(start_date) == 10 and start_date.count('-') == 2:
            search_conditions.append(f"event_datetime >= '{start_date}'")
    if end_date:
        # Basic validation for date format (YYYY-MM-DD)
        if end_date and len(end_date) == 10 and end_date.count('-') == 2:
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
        show_empty_onsale: bool = True,
        is_added: bool = False,
        is_ignored: bool = False,
) -> int:
    search_condition = create_search_condition_for_onsale_email_details(
        timezone, search_term, venues, start_date, end_date
    )
    
    # Add additional filtering conditions (same logic as main function)
    additional_filters = []
    
    if not show_empty_onsale:
        additional_filters.append("onsale_or_presale_ts IS NOT NULL")
    
    # Always exclude duplicate records
    additional_filters.append("is_duplicate = FALSE")
    
    # Filter by added status - always apply the filter
    additional_filters.append(f"is_added = {is_added}")
    
    # Filter by ignored status - always apply the filter
    additional_filters.append(f"is_ignored = {is_ignored}")
    
    if additional_filters:
        additional_filter_clause = " AND " + " AND ".join(additional_filters)
    else:
        additional_filter_clause = ""
    
    query = f"""
        SELECT COUNT(1) AS cnt 
        FROM (
            WITH filtered_emails AS (
                SELECT *
                FROM email.email_onsales
                WHERE 1=1
                AND IS_DUPLICATE = FALSE
                AND EMAIL_ID IS NOT NULL
                {search_condition}
                {additional_filter_clause}
            )
            SELECT eo.event_name, eo.event_datetime, eo.venue, eo.onsale_or_presale_ts
            FROM filtered_emails eo
            GROUP BY eo.event_name, eo.event_datetime, eo.venue, eo.onsale_or_presale_ts
        ) grouped_results
    """
    
    result = await get_pg_readonly_database().fetch_one(query)
    return result["cnt"] if result else 0


async def get_onsale_email_details(
        timezone: str = "America/Chicago",
        page: int = 1,
        page_size: int = 50,
        search_term: Optional[str] = None,
        venues: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sort_by: str = "created",
        sort_order: str = "desc",
        show_empty_onsale: bool = False,
        is_added: bool = False,
        is_ignored: bool = False,
) -> dict:
    search_condition = create_search_condition_for_onsale_email_details(
        timezone, search_term, venues, start_date, end_date
    )
    
    # Add additional filtering conditions
    additional_filters = []
    
    # Filter out empty onsale_or_presale_ts fields if show_empty_onsale is False
    if not show_empty_onsale:
        additional_filters.append("onsale_or_presale_ts IS NOT NULL")
    
    # Always exclude duplicate records
    additional_filters.append("is_duplicate = FALSE")
    
    # Filter by added status - always apply the filter
    additional_filters.append(f"is_added = {is_added}")
    
    # Filter by ignored status - always apply the filter
    additional_filters.append(f"is_ignored = {is_ignored}")
    
    # Combine additional filters with search condition
    if additional_filters:
        additional_filter_clause = " AND " + " AND ".join(additional_filters)
    else:
        additional_filter_clause = ""
    
    limit_expression = ""
    if page and page_size:
        offset = (page - 1) * page_size
        limit_expression = f"LIMIT {page_size} OFFSET {offset}"
    
    # Validate sort parameters - use column expressions for ORDER BY in CTE
    valid_sort_columns = {
        "created": f"timezone('{timezone}', MAX(created_at))",
        "venue": "venue", 
        "performer": "performer",
        "event_name": "event_name",
        "event_datetime": "event_datetime",
        "onsale_or_presale_ts": "onsale_or_presale_ts",
        "discovery_date": f"timezone('{timezone}', MIN(created_at))"
    }
    
    if sort_by not in valid_sort_columns:
        sort_by = "onsale_or_presale_ts"
    
    sort_order = sort_order.lower()
    if sort_order not in ["asc", "desc"]:
        sort_order = "asc"
    
    # Build ORDER BY clause with secondary sort by event_name for consistency
    if sort_by == "onsale_or_presale_ts" and sort_order == "asc":
        # Special case: onsale_or_presale_ts ASC, then event_name ASC
        order_clause = f"ORDER BY {valid_sort_columns[sort_by]} ASC, event_name ASC"
    elif sort_by == "onsale_or_presale_ts" and sort_order == "desc":
        # Special case: onsale_or_presale_ts DESC, then event_name DESC
        order_clause = f"ORDER BY {valid_sort_columns[sort_by]} DESC, event_name DESC"
    else:
        # For all other fields, use the specified sort order with event_name as secondary sort
        secondary_sort_order = "ASC" if sort_order == "asc" else "DESC"
        order_clause = f"ORDER BY {valid_sort_columns[sort_by]} {sort_order.upper()}, event_name {secondary_sort_order}, event_datetime ASC, onsale_or_presale_ts ASC"

    query = f"""
        WITH filtered_emails AS (
            SELECT 
                id, 
                is_added, 
                added_at, 
                added_by, 
                is_ignored, 
                ignored_at, 
                ignored_by, 
                created_at, 
                updated_at, 
                event_name, 
                event_datetime, 
                REGEXP_REPLACE(venue, '\\s*\\([^)]*\\)', '', 'g') as "venue",
                performer, 
                promoter, 
                discount_code, 
                presale_code, 
                price, 
                onsale_or_presale_ts, 
                event_url, 
                city, 
                state, 
                email_id, 
                is_duplicate
            FROM email.email_onsales
            WHERE 1=1
            AND IS_DUPLICATE = FALSE
            AND EMAIL_ID IS NOT NULL
            {search_condition}
            {additional_filter_clause}
        )
        SELECT
            MAX(eo.id) as "id",
            eo.venue as "venue",
            MAX(eo.performer) as "performer",
            MAX(eo.promoter) as "promoter",
            STRING_AGG(DISTINCT CASE WHEN eo.discount_code IS NOT NULL AND eo.discount_code != '' THEN eo.discount_code END, ', ') as "discount_code",
            STRING_AGG(DISTINCT CASE WHEN eo.presale_code IS NOT NULL AND eo.presale_code != '' THEN eo.presale_code END, ', ') as "presale_code",
            MIN(eo.price) as "price",
            MAX(eo.email_id) as "email_id",
            timezone('{timezone}', MAX(eo.created_at)) as "last_received",
            MAX(eo.event_name) as "event_name",
            eo.event_datetime as "event_datetime",
            eo.onsale_or_presale_ts as "onsale_or_presale_ts",
            timezone('{timezone}', MIN(eo.created_at)) as "discovery_date",
            MAX(eo.event_url) as "event_url",
            MAX(eo.city) as "city",
            MAX(eo.state) as "state",
            CASE WHEN MAX(os.eventvenue) IS NULL THEN false ELSE true END as "is_matched",
            BOOL_OR(eo.is_added) as "is_added",
            MAX(eo.added_at) as "added_at",
            MAX(eo.added_by) as "added_by",
            BOOL_OR(eo.is_ignored) as "is_ignored",
            MAX(eo.ignored_at) as "ignored_at",
            MAX(eo.ignored_by) as "ignored_by",
            MAX(eo.updated_at) as "updated_at"
        FROM filtered_emails eo
        LEFT JOIN daily_onsales os ON os.eventvenue = eo.venue
            AND os.eventcitystate = COALESCE(eo.city, '') || ', ' || COALESCE(eo.state, '')
            AND os.eventdatetime = eo.event_datetime
            AND (os.eventpresale = eo.onsale_or_presale_ts OR os.eventpubsale = eo.onsale_or_presale_ts)
        GROUP BY eo.event_datetime, eo.venue, eo.onsale_or_presale_ts
        {order_clause}
        {limit_expression}
    """

    email_details = await get_pg_readonly_database().fetch_all(query)
    email_details_total = await get_onsale_email_details_count(timezone, search_term, venues, start_date, end_date, show_empty_onsale, is_added, is_ignored)

    # Convert Record objects to dictionaries for easier manipulation
    email_details = [dict(item) for item in email_details]

    return {"items": email_details, "total": email_details_total}


async def get_onsale_email_venues() -> dict:
    query = """
        SELECT DISTINCT venue as "venue_name" 
        FROM email.email_onsales
        WHERE venue IS NOT NULL
        ORDER BY venue
    """
    
    venues = await get_pg_readonly_database().fetch_all(query)
    venue_names = [v["venue_name"] for v in venues if v["venue_name"]]
    return {"items": venue_names, "total": len(venue_names)}


async def get_all_user_emails(page: int = 1, page_size: int = 50):
    offset = (page - 1) * page_size

    total_query = "SELECT COUNT(*) FROM email.ticketboat_user_email"
    total = await get_pg_readonly_database().fetch_val(total_query)

    items_query = """
        SELECT id, company, nickname, gmail_login, created_at
        FROM email.ticketboat_user_email
        ORDER BY created_at DESC
        LIMIT :page_size OFFSET :offset
    """
    items = await get_pg_readonly_database().fetch_all(
        items_query, {"page_size": page_size, "offset": offset}
    )

    return {"total": total, "items": items}


async def get_all_user_emails_for_export():
    query = """
        SELECT id, company, nickname, gmail_login, created_at
        FROM email.ticketboat_user_email
        ORDER BY created_at DESC
    """
    items = await get_pg_readonly_database().fetch_all(query)
    return items


async def get_all_user_companies():
    query = """
        SELECT DISTINCT company
        FROM email.ticketboat_user_email
        WHERE company IS NOT NULL
        ORDER BY company ASC
    """
    return await get_pg_readonly_database().fetch_all(query)


async def create_user_email(company: str, nickname: str, gmail_login: str):
    query = """
        INSERT INTO email.ticketboat_user_email (company, nickname, gmail_login)
        VALUES (:company, :nickname, :gmail_login)
    """
    await get_pg_database().execute(
        query, {"company": company, "nickname": nickname, "gmail_login": gmail_login}
    )


async def update_user_email(id: UUID4, request: UserEmailUpdateRequest):
    update_values = request.model_dump(exclude_unset=True)
    set_clause = ", ".join([f"{key} = :{key}" for key in update_values])
    query = f"""
        UPDATE email.ticketboat_user_email
        SET {set_clause}, updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
    """
    update_values["id"] = id
    await get_pg_database().execute(query, update_values)


async def delete_user_email(id: UUID4):
    query = """
        DELETE FROM email.ticketboat_user_email
        WHERE id = :id
    """
    await get_pg_database().execute(query, {"id": id})


async def get_inactive_accounts(
        timezone: str = "America/Chicago", page: int = 1, page_size: int = 10
):
    offset = (page - 1) * page_size

    count_sql = """
    WITH latest_emails AS (
        SELECT 
            tue.nickname,
            MAX(e.created) AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_email_time
        FROM email.ticketboat_user_email tue
        LEFT JOIN email.email e ON tue.gmail_login = e.to_email
        GROUP BY tue.nickname
    )
    SELECT COUNT(*)
    FROM latest_emails
    WHERE last_email_time < (CURRENT_TIMESTAMP AT TIME ZONE :timezone - INTERVAL '24 hours')
    """

    total_count = await get_pg_database().fetch_val(
        query=count_sql, values={"timezone": timezone}
    )

    sql = """
    WITH latest_emails AS (
        SELECT 
            tue.nickname,
            MAX(e.created) AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_email_time
        FROM email.ticketboat_user_email tue
        LEFT JOIN email.email e ON tue.gmail_login = e.to_email
        GROUP BY tue.nickname
    )
    SELECT 
        nickname,
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP AT TIME ZONE :timezone - last_email_time)) AS days_inactive
    FROM latest_emails
    WHERE last_email_time < (CURRENT_TIMESTAMP AT TIME ZONE :timezone - INTERVAL '24 hours')
    ORDER BY last_email_time
    LIMIT :limit OFFSET :offset
    """

    results = await get_pg_database().fetch_all(
        query=sql, values={"timezone": timezone, "limit": page_size, "offset": offset}
    )

    items = [
        {"account": r["nickname"], "days": int(r["days_inactive"] or 0)}
        for r in results
    ]

    return {"total": total_count, "items": items}


async def get_not_setup_accounts(page: int = 1, page_size: int = 10):
    offset = (page - 1) * page_size

    count_sql = """
    SELECT COUNT(*)
    FROM email.ticketboat_user_email tue
    LEFT JOIN email.email e ON tue.gmail_login = e.to_email
    WHERE e.created IS NULL
    """

    total_count = await get_pg_readonly_database().fetch_val(query=count_sql)

    sql = """
    SELECT 
        tue.nickname
    FROM email.ticketboat_user_email tue
    LEFT JOIN email.email e ON tue.gmail_login = e.to_email
    WHERE e.created IS NULL
    ORDER BY tue.nickname
    LIMIT :limit OFFSET :offset
    """

    results = await get_pg_readonly_database().fetch_all(
        query=sql, values={"limit": page_size, "offset": offset}
    )

    items = [{"account": r["nickname"], "days": None} for r in results]

    return {"total": total_count, "items": items}


async def upsert_email_onsales_defaults(onsale_ids: List[str]) -> None:
    """
    Insert default records into email.email_onsales for onsale IDs that don't exist.
    Sets default values: is_ignored=false, is_added=false
    
    Args:
        onsale_ids: List of onsale IDs to ensure exist in PostgreSQL
    """
    if not onsale_ids:
        return
    
    try:
        # Create placeholders for the INSERT statement
        placeholders = ','.join([f'(:id_{i}, false, false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)' for i in range(len(onsale_ids))])
        params = {f'id_{i}': onsale_id for i, onsale_id in enumerate(onsale_ids)}
        
        query = f"""
            INSERT INTO email.email_onsales (id, is_ignored, is_added, created_at, updated_at)
            VALUES {placeholders}
            ON CONFLICT (id) DO NOTHING
        """
        
        await get_pg_database().execute(query=query, values=params)
        
    except Exception as e:
        print(f"Error upserting email onsales defaults: {e}")
        # Don't raise the exception to avoid breaking the main query


async def get_email_onsales_status(onsale_ids: List[str]) -> Dict[str, Dict]:
    """
    Get the status (is_added, is_ignored) for a list of onsale IDs from PostgreSQL.
    
    Args:
        onsale_ids: List of onsale IDs to check
        
    Returns:
        Dictionary mapping onsale_id to status information
    """
    if not onsale_ids:
        return {}
    
    try:
        # Create placeholders for the IN clause
        placeholders = ','.join([f':id_{i}' for i in range(len(onsale_ids))])
        params = {f'id_{i}': onsale_id for i, onsale_id in enumerate(onsale_ids)}
        
        query = f"""
            SELECT 
                id,
                is_added,
                added_at,
                added_by,
                is_ignored,
                ignored_at,
                ignored_by
            FROM email.email_onsales 
            WHERE id IN ({placeholders})
        """
        
        results = await get_pg_database().fetch_all(query=query, values=params)
        
        # Convert results to dictionary
        status_dict = {}
        for row in results:
            status_dict[row["id"]] = {
                "is_added": row["is_added"],
                "added_at": row["added_at"],
                "added_by": row["added_by"],
                "is_ignored": row["is_ignored"],
                "ignored_at": row["ignored_at"],
                "ignored_by": row["ignored_by"]
            }
        
        return status_dict
        
    except Exception as e:
        print(f"Error fetching email onsales status: {e}")
        return {}



async def update_onsale_ignore_status(onsale_id: str, ignored_by: str, is_ignored: bool) -> bool:
    """
    Update the ignored status of an onsale in PostgreSQL.
    
    Args:
        onsale_id: The ID of the onsale to update
        ignored_by: The user who is setting the status
        is_ignored: Whether to set as ignored (true) or not ignored (false)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        update_query = """
            UPDATE email.email_onsales 
            SET is_ignored = :is_ignored,
                ignored_at = CASE WHEN :is_ignored = true THEN CURRENT_TIMESTAMP ELSE NULL END,
                ignored_by = CASE WHEN :is_ignored = true THEN :ignored_by ELSE NULL END,
                updated_at = CURRENT_TIMESTAMP
            WHERE EXISTS (
                SELECT 1 FROM email.email_onsales eo 
                WHERE eo.id = :onsale_id
                    AND eo.venue = email.email_onsales.venue
                    AND eo.city = email.email_onsales.city
                    AND eo.state = email.email_onsales.state
                    AND COALESCE(eo.event_datetime, CURRENT_DATE) = COALESCE(email.email_onsales.event_datetime, CURRENT_DATE)
                    AND COALESCE(eo.onsale_or_presale_ts, CURRENT_DATE) = COALESCE(email.email_onsales.onsale_or_presale_ts, CURRENT_DATE)  
                    AND eo.event_name = email.email_onsales.event_name);
        """
        
        await get_pg_database().execute(query=update_query, values={
            "onsale_id": onsale_id,
            "ignored_by": ignored_by,
            "is_ignored": is_ignored
        })
        
        return True
        
    except Exception as e:
        print(f"Error updating onsale ignore status: {e}")
        print(f"Error type: {type(e)}")
        print(f"Onsale ID: {onsale_id}")
        print(f"Ignored by: {ignored_by}")
        print(f"Is ignored: {is_ignored}")
        print(f"SQL Error details: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return False


async def update_onsale_added_status(onsale_id: str, added_by: str, is_added: bool) -> bool:
    """
    Update the added status of an onsale in PostgreSQL.
    
    Args:
        onsale_id: The ID of the onsale to update
        added_by: The user who is setting the status
        is_added: Whether to set as added (true) or not added (false)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if record exists
        check_query = """
            SELECT id FROM email.email_onsales WHERE id = :onsale_id
        """
        
        existing_record = await get_pg_database().fetch_one(
            query=check_query, 
            values={"onsale_id": onsale_id}
        )
        
        if existing_record:
            # Record exists, update it
            if is_added:
                update_query = """
                    UPDATE email.email_onsales 
                    SET is_added = true,
                        added_at = CURRENT_TIMESTAMP,
                        added_by = :added_by,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :onsale_id
                """
                await get_pg_database().execute(query=update_query, values={
                    "onsale_id": onsale_id,
                    "added_by": added_by
                })
            else:
                update_query = """
                    UPDATE email.email_onsales 
                    SET is_added = false,
                        added_at = NULL,
                        added_by = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :onsale_id
                """
                await get_pg_database().execute(query=update_query, values={
                    "onsale_id": onsale_id
                })
        else:
            # Record doesn't exist, insert it
            if is_added:
                insert_query = """
                    INSERT INTO email.email_onsales (id, is_added, added_at, added_by, created_at, updated_at)
                    VALUES (:onsale_id, true, CURRENT_TIMESTAMP, :added_by, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await get_pg_database().execute(query=insert_query, values={
                    "onsale_id": onsale_id,
                    "added_by": added_by
                })
            else:
                insert_query = """
                    INSERT INTO email.email_onsales (id, is_added, added_at, added_by, created_at, updated_at)
                    VALUES (:onsale_id, false, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                await get_pg_database().execute(query=insert_query, values={
                    "onsale_id": onsale_id
                })
        
        return True
        
    except Exception as e:
        print(f"Error updating onsale added status: {e}")
        print(f"Error type: {type(e)}")
        print(f"Onsale ID: {onsale_id}")
        print(f"Added by: {added_by}")
        print(f"Is added: {is_added}")
        print(f"SQL Error details: {str(e)}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return False


async def get_total_message_count(user_email: str):
    try:
        query = """
            SELECT COUNT(*)
            FROM email.email_assign_user eau 
            WHERE eau.user_id = :user_email AND eau.assigned_is_read = FALSE
            """

        result = await get_pg_database().fetch_val(
            query=query,
            values={"user_email": user_email}
        )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_message_status(user_email: str):
    try:
        query = """
            UPDATE email.email_assign_user eau
            SET assigned_is_read = TRUE
            WHERE eau.user_id = :user_email
        """
        result = await get_pg_database().execute(
            query=query,
            values={"user_email": user_email}
        )
        return {"message": "Update successful", "result": result}
    except Exception as e:
        raise Exception(f"An error occurred while updating: {e}")


async def validate_emails_exist(emails: List[str]) -> List[str]:
    """Validate that emails exist in the user table"""
    try:
        if not emails:
            return []
        
        # Create placeholders for the IN clause
        placeholders = ','.join([f':email_{i}' for i in range(len(emails))])
        
        query = f"""
            SELECT email 
            FROM "user" 
            WHERE email IN ({placeholders})
        """
        
        values = {f'email_{i}': email for i, email in enumerate(emails)}
        
        results = await get_pg_database().fetch_all(query=query, values=values)
        existing_emails = [row['email'] for row in results]
        
        return existing_emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate emails: {e}")


async def replace_daily_emails(emails: List[str]):
    """Replace all daily emails with the new list"""
    try:
        # Start a transaction
        async with get_pg_database().transaction():
            # Delete all existing daily emails
            delete_query = "DELETE FROM email.daily_emails"
            await get_pg_database().execute(query=delete_query)
            
            # Insert new emails
            if emails:
                insert_query = """
                    INSERT INTO email.daily_emails (email)
                    VALUES (:email)
                """
                
                for email in emails:
                    await get_pg_database().execute(
                        query=insert_query, 
                        values={"email": email}
                    )
        
        return {"message": f"Successfully replaced daily emails with {len(emails)} emails"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to replace daily emails: {e}")


async def get_daily_emails() -> List[str]:
    """Get all daily emails from the daily_emails table"""
    try:
        query = """
            SELECT email 
            FROM email.daily_emails 
            ORDER BY created_at ASC
        """
        
        results = await get_pg_database().fetch_all(query=query)
        emails = [row['email'] for row in results]
        
        return emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get daily emails: {e}")
