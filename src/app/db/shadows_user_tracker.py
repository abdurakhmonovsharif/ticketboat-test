import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_user_tracker import *
from app.model.user import User


async def create_user_tracker_entry(user_tracker: ShadowsUserTrackerModel) -> None:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                insert into shadows_user_tracker (id, operation, module, user, data, created)
                values (%(id)s, %(operation)s, %(module)s, %(user)s, %(data)s, %(created)s)
            """, user_tracker.model_dump())
    except Exception as e:
        traceback.print_exc()
