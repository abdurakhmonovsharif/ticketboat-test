from typing import Dict, List, Optional

from fastapi import HTTPException
from firebase_admin import auth

from app.database import get_pg_database
from app.model.user import User


async def upsert_user(user: User):
    sql = """
    INSERT INTO "user" (id, created, name, email, email_verified, roles, providers)
    VALUES (uuid_generate_v4(), current_timestamp, :name, :email, :email_verified, :roles, :providers)
    ON CONFLICT (email)
    DO UPDATE SET
        name = EXCLUDED.name,
        email_verified = EXCLUDED.email_verified,
        roles = EXCLUDED.roles,
        providers = EXCLUDED.providers
    """
    values = {
        "name": user.name,
        "email": user.email,
        "email_verified": user.email_verified,
        "roles": user.roles,
        "providers": user.providers,
    }
    await get_pg_database().execute(query=sql, values=values)


async def get_roles_for_email(user_email: str) -> list[str]:
    sql = """
    select roles
    from "user"
    where email = :user_email
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql, values={"user_email": user_email})
    return [row[0] for row in rows][0] if rows else []


async def get_buyers() -> list[dict]:
    sql = """
    select id,name,email
    from "user"
    WHERE 'buyer' = ANY(roles)
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql)
    return [{"id": row["id"], "name": row["name"], "email": row["email"]} for row in rows] if rows else []


async def set_roles_for_email(user_email: str, roles: list[str]):
    sql = """
    INSERT INTO "user" (id, created, name, email, roles)
    VALUES (uuid_generate_v4(), current_timestamp, :email, :email, :roles)
    ON CONFLICT (email)
    DO UPDATE SET roles = EXCLUDED.roles
    """
    values = {"email": user_email, "roles": roles}
    await get_pg_database().execute(query=sql, values=values)

    # Update firebase
    for user_id in await get_user_ids_for_email(user_email):
        custom_claims = {"roles": roles}
        try:
            auth.set_custom_user_claims(user_id, custom_claims)
        except Exception as e:
            # todo: gracefully handle non-existant user ids
            await remove_firebase_user_ids_for_email(user_email, [user_id])


async def upsert_providers_for_email(user_email: str, providers: list[str]):
    current_providers = await get_providers_for_email(user_email)
    new_providers = list(set(current_providers + providers))
    sql = """
    update "user"
    set providers = :providers
    where email = :email
    """
    values = {"email": user_email, "providers": new_providers}
    await get_pg_database().execute(query=sql, values=values)


async def upsert_firebase_user_ids_for_email(
        user_email: str, firebase_user_ids: list[str]
):
    current_firebase_user_ids = await get_user_ids_for_email(user_email)
    new_firebase_user_ids = list(set(current_firebase_user_ids + firebase_user_ids))
    sql = """
    update "user"
    set firebase_user_ids = :firebase_user_ids
    where email = :email
    """
    values = {"email": user_email, "firebase_user_ids": new_firebase_user_ids}
    await get_pg_database().execute(query=sql, values=values)


async def remove_firebase_user_ids_for_email(
        user_email: str, firebase_user_ids_to_remove: list[str]
):
    current_firebase_user_ids = await get_user_ids_for_email(user_email)
    new_firebase_user_ids = [
        user_id
        for user_id in current_firebase_user_ids
        if user_id not in firebase_user_ids_to_remove
    ]
    sql = """
    update "user"
    set firebase_user_ids = :firebase_user_ids
    where email = :email
    """
    values = {"email": user_email, "firebase_user_ids": new_firebase_user_ids}
    await get_pg_database().execute(query=sql, values=values)


async def get_user_ids_for_email(user_email: str) -> list[str]:
    sql = """
    select firebase_user_ids
    from "user"
    where email = :user_email
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql, values={"user_email": user_email})
    return [row[0] for row in rows][0] if rows else []


async def get_providers_for_email(user_email: str) -> list[str]:
    sql = """
    select providers
    from "user"
    where email = :user_email
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql, values={"user_email": user_email})
    return [row[0] for row in rows][0] if rows else []


async def set_user_ids_for_email(user_email: str, user_ids: list[str]):
    sql = """
    update "user"
    set firebase_user_ids = :user_ids
    where email = :email
    """
    values = {"email": user_email, "user_ids": user_ids}
    await get_pg_database().execute(query=sql, values=values)


async def delete_user(user_email: str):
    sql = """
    delete from "user"
    where email = :email
    """
    values = {"email": user_email}
    await get_pg_database().execute(query=sql, values=values)


async def get_all_users() -> dict[str, dict]:
    sql = """
    select *
    from "user"
    order by name
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql)
    result = {row["email"]: dict(row) for row in rows}
    return result


async def get_user_for_email(email: str) -> dict:
    sql = """
    select * from "user" where email = :email
    """
    db = get_pg_database()
    rows = await db.fetch_all(query=sql, values={"email": email})
    return dict(rows[0]) if rows else {}


async def update_user_email_and_name_for_email(email: str, new_email: str, new_name: str):
    sql = """
    update "user"
    set email = :new_email, 
    name = :new_name
    where :email = email
    """
    db = get_pg_database()
    values = {"new_email": new_email, "new_name": new_name, "email": email}
    await db.execute(query=sql, values=values)


