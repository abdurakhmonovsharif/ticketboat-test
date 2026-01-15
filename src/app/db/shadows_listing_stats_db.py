import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_snowflake_connection
from app.model.shadows_listings_stats import (
    ShadowsListingStats
)
from app.model.user import User


async def get_accounts() -> List:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            sql = """
                select viagogo_account_id from viagogo_account
            """
            cur.execute(sql)
            results = cur.fetchall()
            accounts = [
                result.get("VIAGOGO_ACCOUNT_ID") # type: ignore
                for result in results
            ]
            return accounts
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_items() -> List[ShadowsListingStats]:
    try:
        accounts = await get_accounts()
        results_list = []
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            for account in accounts: # type: ignore
                sql = """
                    select viagogo_account_id, count(*) as max_listings from viagogo_listings
                    where viagogo_account_id = %(account)s group by 1
                """
                cur.execute(sql, {"account": account})
                result = cur.fetchone()
                if result is not None:
                    normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                    print(normalized_data)
                    results_list.append(ShadowsListingStats(**normalized_data))
        return results_list
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
