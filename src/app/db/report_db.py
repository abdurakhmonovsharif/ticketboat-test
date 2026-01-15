import uuid
from typing import Optional, List

from app.database import get_pg_database
from app.model.report import ReportPayload


async def get_reports(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        category_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        user_roles: Optional[List[str]] = None
) -> dict:
    values = {"timezone": timezone}
    limit_expression = ""
    if page and page_size:
        limit_expression = f" limit {page_size} offset {(page - 1) * page_size}"

    conditions = []
    if category_id:
        conditions.append(f"rp.category_id = :category_id")
        values["category_id"] = category_id

    user_role_condition = ""
    if user_roles and "admin" not in user_roles:
        role_placeholders = ", ".join([f":user_role_{i}" for i in range(len(user_roles))])
        user_role_condition = f"WHERE r.name IN ({role_placeholders})"
        for i, role in enumerate(user_roles):
            values[f"user_role_{i}"] = role

    role_condition = ""
    if roles:
        role_placeholders = ", ".join([f":role_{i}" for i in range(len(roles))])
        role_condition = f"WHERE r.name IN ({role_placeholders})"
        for i, role in enumerate(roles):
            values[f"role_{i}"] = role

    if search_term:
        conditions.append("""
            ( rp.title ILIKE :search_term or
                rp.description ILIKE :search_term)
            """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    sql_query = f"""
    with accessible_reports as (
            select distinct rp.id as id
            from report rp
            left join report_role_mapping rrm on rp.id = rrm.report_id 
            left join role r on r.id = rrm.role_id
            {user_role_condition}
        ),
        base_report as (
            select distinct ar.id as id
            from accessible_reports ar
            left join report_role_mapping rrm on ar.id = rrm.report_id 
            left join role r on r.id = rrm.role_id
            {role_condition}
        )
        select 
            rp.id as id,
            rp.title as title,
            rp.description as description,
            rp.link as link,
            rp.created AT TIME ZONE 'UTC' AT TIME ZONE :timezone as created,
            array_agg(r.name) as roles
        from report rp
        join base_report br on rp.id = br.id
        left join report_role_mapping rrm on rp.id = rrm.report_id 
        left join role r on rrm.role_id = r.id
        {where_clause}
        group by rp.id
        order by created desc
        {limit_expression}
    """

    rows = await get_pg_database().fetch_all(query=sql_query, values=values)
    total_count = await get_report_count()
    return {
        "total": total_count,
        "items": [dict(r) for r in rows]
    }


async def get_report_count(
        search_term: Optional[str] = None,
        category_id: Optional[str] = None,
        roles: Optional[List[str]] = None
) -> int:
    values = {}
    conditions = []
    if category_id:
        conditions.append(f"category_id = :category_id")
        values["category_id"] = f"{category_id}"

    if roles:
        role_placeholders = ", ".join([f":role_{i}" for i in range(len(roles))])
        conditions.append(f"WHERE r.name IN ({role_placeholders})")
        for i, role in enumerate(roles):
            values[f"role_{i}"] = role

    if search_term:
        conditions.append("""
            ( title ILIKE :search_term or
            description ILIKE :search_term or
            link ILIKE :search_term )
        """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
    sql_query = f"""
        select COUNT(distinct rp.id) 
        from report rp
        left join report_role_mapping rrm on rp.id = rrm.report_id 
        left join role r on r.id = rrm.role_id
        {where_clause}
    """
    return await get_pg_database().fetch_val(query=sql_query, values=values)


async def create_report(payload: ReportPayload) -> None:
    sql_query = f"""
        INSERT INTO report (id, title, link, description, category_id)
        VALUES(:report_id, :title, :link, :description, :category_id)
    """
    values = {
        "report_id": uuid.uuid4(),
        "title": payload.title,
        "description": payload.description,
        "link": payload.link,
        "category_id": payload.category_id,
    }
    db = get_pg_database()
    await db.execute(query=sql_query, values=values)
    print("new report has been inserted")

    if payload.roles:
        get_roles_query = f"""
                select id from role where name = ANY(:role_names)
           """
        role_rows = await db.fetch_all(query=get_roles_query, values={"role_names": payload.roles})

        insert_roles_query = """
                    INSERT INTO report_role_mapping (report_id, role_id)
                    VALUES
                """

        role_values = ", ".join([f"(:report_id, :role_id_{i})" for i in range(len(role_rows))])
        insert_roles_query += role_values
        print(insert_roles_query)

        role_insert_values = {"report_id": values["report_id"]}
        role_insert_values.update({f"role_id_{i}": role["id"] for i, role in enumerate(role_rows)})
        print(role_insert_values)

        await db.execute(query=insert_roles_query, values=role_insert_values)


async def delete_report(report_id: str) -> None:
    sql_query = """
       delete from report
       where id = :report_id 
    """
    values = {"report_id": report_id}
    await get_pg_database().execute(query=sql_query, values=values)


async def update_report(report_id: str, payload: ReportPayload) -> None:
    sql_query = """
        update report
        set title = :title, 
        description = :description,
        link = :link,
        category_id = :category_id
        where id = :report_id
    """
    values = {
        "report_id": report_id,
        "title": payload.title,
        "description": payload.description,
        "link": payload.link,
        "category_id": payload.category_id,
    }

    db = get_pg_database()
    await db.execute(query=sql_query, values=values)

    fetch_roles_query = """
           SELECT role_id FROM report_role_mapping WHERE report_id = :report_id
       """
    existing_roles = await db.fetch_all(query=fetch_roles_query, values={"report_id": report_id})
    existing_roles_set = set(role['role_id'] for role in existing_roles)
    print("existing_roles_set", existing_roles_set)

    new_roles_set = set()
    if payload.roles:
        get_roles_query = f"""
                select id from role where name = ANY(:role_names)
           """
        role_rows = await db.fetch_all(query=get_roles_query, values={"role_names": payload.roles})
        new_roles_set = set(role['id'] for role in role_rows)
    print("new_roles_set", new_roles_set)

    roles_to_add = new_roles_set - existing_roles_set
    roles_to_remove = existing_roles_set - new_roles_set
    print("roles_to_add", roles_to_add)
    print("roles_to_remove", roles_to_remove)

    if roles_to_remove:
        delete_roles_query = """
                   DELETE FROM report_role_mapping
                   WHERE report_id = :report_id AND role_id IN ({})
               """.format(", ".join([f":role_id_{i}" for i in range(len(roles_to_remove))]))
        delete_values = {"report_id": report_id}
        delete_values.update({f"role_id_{i}": role for i, role in enumerate(roles_to_remove)})

        await db.execute(query=delete_roles_query, values=delete_values)

    if roles_to_add:
        add_roles_query = """
               INSERT INTO report_role_mapping (report_id, role_id)
               VALUES
           """
        add_roles_query += ", ".join([f"(:report_id, :role_id_{i})" for i in range(len(roles_to_add))])
        add_values = {"report_id": report_id}
        add_values.update({f"role_id_{i}": role for i, role in enumerate(roles_to_add)})

        await db.execute(query=add_roles_query, values=add_values)
