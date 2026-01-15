import uuid
from typing import Optional, List

from app.database import get_pg_database
from app.db.user_db import get_roles_for_email
from app.model.report import CategoryPayload
from app.model.user import User


async def get_categories(
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        user_roles: Optional[List[str]] = None
) -> dict:
    values = {"timezone": timezone}
    limit_expression = ""
    if page and page_size:
        limit_expression = f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"

    conditions = []
    if user_roles and "admin" not in user_roles:
        role_placeholders = ", ".join([f":user_role_{i}" for i in range(len(user_roles))])
        conditions.append(f"r.name IN ({role_placeholders})")
        for i, role in enumerate(user_roles):
            values[f"user_role_{i}"] = role

    if search_term:
        conditions.append("""
                (title ILIKE :search_term OR
                     description ILIKE :search_term)
               """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
    select 
        rc.id, 
        rc.title, 
        rc.description, 
        rc.created AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS created
    from report_category rc
    left join report rp on rc.id = rp.category_id
    left join report_role_mapping rrm on rp.id = rrm.report_id 
    left join role r on r.id = rrm.role_id
    {where_clause} 
    group by rc.id
    order by rc.created desc
    {limit_expression}
    """
    rows = await get_pg_database().fetch_all(query=sql, values=values)
    total_count = await get_category_count()
    return {
        "total": total_count,
        "items": [dict(r) for r in rows]
    }


async def get_categories_by_user_role(
        user: User,
        timezone: Optional[str] = "America/Chicago",
        search_term: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
) -> dict:
    user_roles: list[str] = await get_roles_for_email(user.email)
    if "admin" in user_roles:
        categories = await get_categories(timezone, search_term, page, page_size)
    else:
        categories = await get_categories(timezone, search_term, page, page_size, user_roles)

    return categories


async def get_category_count(
        search_term: Optional[str] = None,
        user_roles: Optional[List[str]] = None

) -> int:
    values = {}
    conditions = []
    if user_roles and "admin" not in user_roles:
        role_placeholders = ", ".join([f":user_role_{i}" for i in range(len(user_roles))])
        conditions.append(f"r.name IN ({role_placeholders})")
        for i, role in enumerate(user_roles):
            values[f"user_role_{i}"] = role

    if search_term:
        conditions.append("""
                   (title ILIKE :search_term OR
                        description ILIKE :search_term)
                  """)
        values["search_term"] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql_query = f"""
        select COUNT(*)
        from report_category rc
        left join report rp on rc.id = rp.category_id
        left join report_role_mapping rrm on rp.id = rrm.report_id 
        left join role r on r.id = rrm.role_id
        {where_clause}
        group by rc.id
        """
    return await get_pg_database().fetch_val(query=sql_query, values=values)


async def create_category(payload: CategoryPayload) -> None:
    sql_query = f"""
        INSERT INTO report_category (id, title, description)
        VALUES(:category_id, :title, :description)
    """
    values = {
        "category_id": uuid.uuid4(),
        "title": payload.title,
        "description": payload.description
    }
    await get_pg_database().execute(query=sql_query, values=values)


async def delete_category(category_id: str) -> None:
    sql_query = """
       delete from report_category
       where id = :category_id
    """
    values = {"category_id": category_id}
    await get_pg_database().execute(query=sql_query, values=values)


async def update_category(category_id: str, payload: CategoryPayload) -> None:
    sql_query = """
        update report_category
        set title = :title, 
        description = :description
        where id = :category_id
    """
    values = {
        "category_id": category_id,
        "title": payload.title,
        "description": payload.description
    }
    await get_pg_database().execute(query=sql_query, values=values)
