import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_tessitura_whitelist import (
    ShadowsTessituraWhitelist
)


async def get_items(page: int, page_size: int) -> List[ShadowsTessituraWhitelist]:
    try:
        offset = (page - 1) * page_size
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select * from tessitura_event_list_v
                limit %(page_size)s offset %(offset)s
            """
            cur.execute(sql, {"page_size": page_size, "offset": offset})
            results = cur.fetchall()
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                results_list.append(ShadowsTessituraWhitelist(**normalized_data))
        return results_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
