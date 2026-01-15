import logging

from fastapi import HTTPException
from starlette import status

from app.database import get_pg_database


async def get_all_roles(use_cache=True) -> list[str]:
    sql = """
    select name
    from "role"
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql)
    return [row[0] for row in rows]


async def set_all_roles(roles: list[str]):
    db = get_pg_database()
    async with db.transaction():
        # Fetch all current roles from the database
        current_roles_query = "SELECT name FROM role"
        current_roles_rows = await db.fetch_all(query=current_roles_query)
        current_roles = {row["name"] for row in current_roles_rows}

        # Determine roles to add and roles to remove
        roles_to_add = set(roles) - current_roles
        roles_to_remove = current_roles - set(roles)

        # Add new roles
        for role in roles_to_add:
            add_role_query = """
            INSERT INTO role (id, created, name)
            VALUES (uuid_generate_v4(), current_timestamp, :name)
            """
            values = {"name": role}
            await db.execute(query=add_role_query, values=values)

        # Remove roles that are not in the provided list
        for role in roles_to_remove:
            remove_role_query = "DELETE FROM role WHERE name = :name"
            await db.execute(query=remove_role_query, values={"name": role})


async def assign_menu_item(role_id: str, menu_item_id: str):
    db = get_pg_database()

    role_exists_query = 'SELECT 1 FROM "role" WHERE id = :role_id'
    menu_item_exists_query = 'SELECT 1 FROM role_menu_items WHERE role_id = :role_id AND menu_item_id = :menu_item_id'

    role_exists = await db.fetch_one(query=role_exists_query, values={"role_id": role_id})
    if not role_exists:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Role with ID {role_id} does not exist")

    menu_item_assigned = await db.fetch_one(query=menu_item_exists_query,
                                            values={"role_id": role_id, "menu_item_id": menu_item_id})
    if menu_item_assigned:
        return {"message": f"Menu item {menu_item_id} is already assigned to role {role_id}"}

    assign_query = """
    INSERT INTO role_menu_items (role_id, menu_item_id)
    VALUES (:role_id, :menu_item_id)
    """
    await db.execute(query=assign_query, values={"role_id": role_id, "menu_item_id": menu_item_id})
    return {"message": "Menu item assigned to role successfully"}


async def add_menu_item_to_beta(menu_item_id: str):
    db = get_pg_database()
    insert_query = """
       INSERT INTO beta_menu_items (menu_item_id)
       VALUES (:menu_item_id)
       ON CONFLICT (menu_item_id) DO NOTHING
       """
    await db.execute(query=insert_query, values={"menu_item_id": menu_item_id})
    return {"message": f"Menu item {menu_item_id} has been marked as beta"}


async def remove_beta_menu(menu_item_id: str):
    db = get_pg_database()
    delete_query = """
          DELETE FROM beta_menu_items WHERE menu_item_id = :menu_item_id
    """
    await db.execute(query=delete_query, values={"menu_item_id": menu_item_id})
    return {"message": f"Menu item {menu_item_id} has been unmarked"}


async def get_menu_items_with_roles():
    db = get_pg_database()
    query = """
        SELECT 
            rmi.menu_item_id,
            ARRAY_AGG(r.name ORDER BY r.name) AS accessible_roles,
            CASE WHEN bmi.menu_item_id IS NOT NULL THEN TRUE ELSE FALSE END AS beta
        FROM 
            role_menu_items rmi
        JOIN 
            role r ON rmi.role_id = r.id
        LEFT JOIN 
            beta_menu_items bmi ON rmi.menu_item_id = bmi.menu_item_id
        GROUP BY 
            rmi.menu_item_id, beta;
    """
    try:
        rows = await db.fetch_all(query=query)
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")

    return [
        {
            "menu_item_id": row["menu_item_id"],
            "beta": row["beta"],
            "accessibleRoles": row["accessible_roles"] or []
        } for row in rows
    ]


async def remove_role_from_menu_item(role_id: str, menu_item_id: str):
    db = get_pg_database()
    try:
        delete_query = """
        DELETE FROM role_menu_items 
        WHERE role_id = :role_id AND menu_item_id = :menu_item_id
        """
        await db.execute(query=delete_query, values={"role_id": role_id, "menu_item_id": menu_item_id})
    except Exception as e:
        raise e


async def get_menu_roles():
    db = get_pg_database()
    query = """
        SELECT id, name
        FROM "role"
    """
    rows = await db.fetch_all(query=query)
    return [{"id": row["id"], "name": row["name"]} for row in rows]
