from dotenv import load_dotenv
import pytest
from app.database import close_pg_database, init_pg_database
from app.db.user_db import get_all_users, get_roles_for_email

load_dotenv()


@pytest.mark.asyncio
async def test_get_roles_for_email():
    await init_pg_database()
    roles = await get_roles_for_email("someone@somewhere.com")
    await close_pg_database()


@pytest.mark.asyncio
async def test_get_all_users():
    await init_pg_database()
    result = await get_all_users()
    await close_pg_database()
