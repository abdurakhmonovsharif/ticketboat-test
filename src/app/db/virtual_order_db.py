import typing
import uuid

from databases.interfaces import Record

from app.database import get_pg_database
from app.enums.virtual_order_enums import VirtualOrderXUserAssignedStatus
from app.model.virtual_order import VirtualOrderDto


async def post_virtual_order(vo_input: VirtualOrderDto, email):
    db = get_pg_database()
    async with db.transaction():
        sql = """
        INSERT INTO virtual_order (id, created, created_by, event_url, section_list, restrictions, max_buyers, priority_level, status, deleted)
        VALUES (:id, current_timestamp, :email, :event_url, :section_list, :restrictions, :max_buyers, :priority_level, :status, false)
        """
        vo_id = str(uuid.uuid4())
        values = {
            "id": vo_id,
            "email": email,
            "event_url": vo_input.event_url,
            "section_list": vo_input.section_list,
            "restrictions": vo_input.restrictions,
            "max_buyers": vo_input.max_buyers,
            "priority_level": vo_input.priority_level,
            "status": vo_input.status.value,
        }
        await _assign_to_buyers(vo_id, vo_input, db)
        await db.execute(query=sql, values=values)


async def update_virtual_order(vo_input: VirtualOrderDto, vo_id):
    db = get_pg_database()
    async with db.transaction():
        sql = """
        UPDATE virtual_order
        SET event_url = :event_url,
            section_list = :section_list,
            restrictions = :restrictions,
            max_buyers = :max_buyers,
            priority_level = :priority_level,
            status = :status
        WHERE id = :id;
        """
        values = {
            "id": vo_id,
            "event_url": vo_input.event_url,
            "section_list": vo_input.section_list,
            "restrictions": vo_input.restrictions,
            "max_buyers": vo_input.max_buyers,
            "priority_level": vo_input.priority_level,
            "status": vo_input.status.value,
        }
        await _assign_to_buyers(vo_id, vo_input, db)
        await db.execute(query=sql, values=values)


async def get_all_virtual_orders(page, size):
    where_clause = " WHERE deleted=false"

    sql = f"""
    SELECT
     vo.id
    ,vo.created
    ,vo.created_by
    ,vo.event_url
    ,vo.section_list
    ,vo.restrictions
    ,vo.max_buyers
    ,vo.priority_level
    ,vo.status
    ,array_agg(voa.user_email) AS assigned_buyers
    FROM VIRTUAL_ORDER vo
    join virtual_order_x_user_assigned voa on vo.id = voa.virtual_order_id
    {where_clause}
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY vo.created DESC
    LIMIT :limit 
    OFFSET :offset
    """
    count_query = f"""
    SELECT count(1) FROM VIRTUAL_ORDER {where_clause}
    """

    values = {"limit": size, "offset": (page - 1) * size}
    db = get_pg_database()
    count = await db.fetch_one(query=count_query)
    rows = await db.fetch_all(query=sql, values=values)
    result = [dict(row) for row in rows]

    return {
        "total": count[0] if count else 0,
        "page": page,
        "size": size,
        "data": result
    }


async def get_virtual_order_by_id(vo_id):
    assigned_buyers_sql = """
    SELECT user_email FROM virtual_order_x_user_assigned where virtual_order_id=:id
    """
    db = get_pg_database()
    assigned_buyer_rows = await db.fetch_all(query=assigned_buyers_sql, values={"id": vo_id})
    assigned_buyers = [row[0] for row in assigned_buyer_rows]
    row = await _get_virtual_order_by_id(db, vo_id)
    result = {
        "id": row["id"],
        "event_url": row["event_url"],
        "section_list": row["section_list"],
        "restrictions": row["restrictions"],
        "max_buyers": row["max_buyers"],
        "priority_level": row["priority_level"],
        "assigned_buyers": assigned_buyers
    }
    return result


async def _get_virtual_order_by_id(db, vo_id) -> typing.Optional[Record]:
    sql = """
    SELECT * FROM virtual_order where id=:id
    """
    row = await db.fetch_one(query=sql, values={"id": vo_id})
    return row


async def _assign_to_buyers(vo_id, vo_input: VirtualOrderDto, db):
    delete_virtual_order_x_user_assigned_by_vo_id = """
    DELETE FROM virtual_order_x_user_assigned
    WHERE virtual_order_id=:vo_id AND assigned_by_captain=true
    """
    await db.execute(query=delete_virtual_order_x_user_assigned_by_vo_id, values={"vo_id": vo_id})

    for _buyer_email in vo_input.assigned_buyers:
        values = {
            "virtual_order_id": vo_id,
            "user_email": _buyer_email,
            "status": VirtualOrderXUserAssignedStatus.PENDING.value
        }
        _sql = f"""
    INSERT INTO virtual_order_x_user_assigned (id, created, virtual_order_id, user_email, status, assigned_by_captain)
    VALUES (uuid_generate_v4(), current_timestamp, :virtual_order_id, :user_email, :status, true)
        """
        await db.execute(query=_sql, values=values)


async def delete_virtual_order_by_id(vo_id):
    db = get_pg_database()
    sql = """
    UPDATE virtual_order
    SET deleted = true
    WHERE id = :id;
    """
    await db.execute(query=sql, values={"id": vo_id})


async def take_on_virtual_order(vo_id, email):
    db = get_pg_database()
    vo = await _get_virtual_order_by_id(db, vo_id)
    count = await _count_virtual_order_x_user_assigned_by_vo_id(db, vo_id)
    if (vo['max_buyers'] is not None and vo['max_buyers'] > count) or vo['max_buyers'] is None:
        _sql = f"""
    INSERT INTO virtual_order_x_user_assigned (id, created, virtual_order_id, user_email, status, assigned_by_captain)
    VALUES (uuid_generate_v4(), current_timestamp, :virtual_order_id, :user_email, :status, true)
    ON CONFLICT (virtual_order_id, user_email)
    DO UPDATE SET status = :status;
        """
        values = {"virtual_order_id": vo_id, "user_email": email,
                  "status": VirtualOrderXUserAssignedStatus.IN_PROGRESS.value}
        await db.execute(query=_sql, values=values)


async def get_buyer_virtual_orders(email, size, page):
    db = get_pg_database()

    count_query = """
        SELECT count(vo.id)
    FROM virtual_order vo
    LEFT JOIN virtual_order_x_user_assigned voa
        ON vo.id=voa.virtual_order_id
    WHERE deleted=false
        AND (max_buyers is null or coalesce(voa.user_email,'custom_email')=:email)
    """

    select_query = f"""
    SELECT vo.id
        ,vo.created
        ,vo.created_by
        ,vo.event_url
        ,vo.section_list
        ,vo.restrictions
        ,vo.priority_level
        ,vo.status
        ,coalesce(voa.status,'PENDING') as buyer_status
    FROM virtual_order vo
    LEFT JOIN virtual_order_x_user_assigned voa
        ON vo.id=voa.virtual_order_id
    WHERE deleted=false
        AND (max_buyers is null or coalesce(voa.user_email,'custom_email')=:email)
    GROUP BY 1,2,3,4,5,6,7,8,9
    ORDER BY vo.created
    LIMIT {size} OFFSET {(page - 1) * size}
    """
    count_row = await db.fetch_one(query=count_query, values={"email": email})
    rows = await db.fetch_all(select_query, values={"email": email})
    result = [dict(row) for row in rows]
    return {
        "total": count_row[0] if count_row else 0,
        "page": page,
        "size": size,
        "data": result
    }


async def _count_virtual_order_x_user_assigned_by_vo_id(db, vo_id):
    sql = """
    SELECT COUNT(1) 
    FROM virtual_order_x_user_assigned
    WHERE virtual_order_id=:virtual_order_id
    """
    count_row = await db.fetch_one(query=sql, values={"virtual_order_id": vo_id})
    return count_row[0] if count_row else 0
