import concurrent.futures as cf
import os
import time
import traceback
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any

import boto3
import botocore
from boto3.dynamodb.conditions import Key

from app.database import get_snowflake_connection, get_pg_realtime_catalog_database

PK = "id"
SK = "sub_id"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(f"shadows-catalog-{os.getenv('ENVIRONMENT')}")

# Get all config key/values
def get_all_autopricing_config() -> List[Dict[str, Any]]:
    try:
        query = "SELECT key, value FROM shadows_autopricing_config ORDER BY key"
        with get_snowflake_connection().cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            return [{"key": row[0], "value": row[1]} for row in results]
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error fetching autopricing config") from e


# Update/insert a config key and append to history, both in a transaction
async def upsert_autopricing_config(key: str, value: str, updated_by: str) -> None:
    try:
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            try:
                cur.execute("BEGIN")
                # Upsert into config
                cur.execute(
                    """
                    MERGE INTO shadows_autopricing_config t
                    USING (SELECT %s AS key, %s AS value) s
                    ON t.key = s.key
                    WHEN MATCHED THEN UPDATE SET t.value = s.value
                    WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)
                    """,
                    (key, value)
                )
                # Insert into history
                cur.execute(
                    """
                INSERT INTO shadows_autopricing_config_history (key, value, updated_by, history_timestamp)
                VALUES (%s, %s, %s, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
                """,
                    (key, value, updated_by)
                )

                insert_query = """
                    INSERT INTO shadows_autopricing_config (key,value)
                    VALUES (:key, :value)
                    ON CONFLICT (key)
                    DO UPDATE SET
                    value = EXCLUDED.value
                   """
                await get_pg_realtime_catalog_database().execute(query=insert_query, values={"key": key, "value": value})
                if key.endswith('_fee_pct'):
                    acc = key.removesuffix("_fee_pct")
                    update_all_under_id(f'{acc}_account', value)
                cur.execute("COMMIT")
            except Exception as e:
                cur.execute("ROLLBACK")
                raise
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error updating autopricing config") from e


def update_all_under_id(id_value: str, fee_pct: str, max_workers: int = 8) -> int:
    # 1) Query all items in the partition
    items = []
    resp = table.query(KeyConditionExpression=Key(PK).eq(id_value))
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.query(
            KeyConditionExpression=Key(PK).eq(id_value),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    if not items:
        return 0

    for item in items:
        _update_one(fee_pct, item)


def _update_one(fee_pct: str, item):
    d = Decimal(fee_pct.strip())
    percent_decimal = (d * Decimal('100')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    # id + sub_id are guaranteed to exist because we got them from the table
    for attempt in range(5):  # simple retry for throttling etc.
        try:
            table.update_item(
                Key={PK: item[PK], SK: item[SK]},
                UpdateExpression="SET #st = :v",
                ExpressionAttributeNames={"#st": "fee_pct"},
                ExpressionAttributeValues={":v": percent_decimal},
                # optional guard to avoid useless writes:
                # ConditionExpression="attribute_not_exists(#st) OR #st <> :v",
            )
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                time.sleep(2 ** attempt / 5.0)
                continue
            raise
    return False

def get_autopricing_config_history() -> List[Dict[str, Any]]:
    try:
        query = """
            SELECT key, value, updated_by, history_timestamp
            FROM shadows_autopricing_config_history
            ORDER BY history_timestamp DESC
        """
        with get_snowflake_connection().cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            return [
                {
                    "key": row[0],
                    "value": row[1],
                    "updated_by": row[2],
                    "history_timestamp": row[3].isoformat() if row[3] else None
                }
                for row in results
            ]
    except Exception as e:
        traceback.print_exc()
        raise Exception("Error fetching autopricing config history") from e