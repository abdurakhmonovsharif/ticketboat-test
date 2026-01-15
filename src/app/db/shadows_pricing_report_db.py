import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_pricing_report import (
    ShadowsPricingReport
)
from app.model.user import User


async def get_items() -> List[ShadowsPricingReport]:
    try:
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select
                    id,
                    event_name,
                    event_start_date,
                    venue,
                    price_override || '%' as percentage,
                    created_at
                from ticketmaster_pricing_override
                order by created_at desc
            """
            cur.execute(sql)
            results = cur.fetchall()
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                results_list.append(ShadowsPricingReport(**normalized_data))
        return results_list
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
