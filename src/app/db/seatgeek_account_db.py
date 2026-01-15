import os
import logging
import json
from fastapi import HTTPException
import boto3
from app.database import get_pg_database

def get_seat_geek_account_data(sub_id: str = ''):
    """
    Retrieve a single SeatGeek account from DynamoDB using the composite key (sub_id).
    - sub_id: the provided account_id
    Returns the item as a dict if found, otherwise raises HTTPException 404.
    Handles environment, and logs errors for troubleshooting.
    """
    env = os.getenv("ENVIRONMENT")
    if not env:
        logging.error("[get_seat_geek_account_data] ENVIRONMENT variable is not set.")
        return None
    table_name = f"shadows-catalog-{env}"
    try:
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table(table_name)
        response = table.get_item(Key={"id": 'seatgeek_account', "sub_id": sub_id})
        item = response.get('Item')
        if not item:
            logging.warning(f"SeatGeek account '{sub_id}' not found in DynamoDB.")
            raise HTTPException(status_code=404, detail=f"SeatGeek account not found.")
        return item
    except Exception as e:
        logging.error(f"Unexpected error fetching SeatGeek account: {str(e)}")
        raise HTTPException(status_code=500, detail="Something went wrong")

def get_all_seat_geek_accounts(acc_id: str = "seatgeek_account"):
    """
    Fetch all SeatGeek accounts from DynamoDB.
    Returns a list of dictionaries, each representing a SeatGeek account.
    """
    env = os.getenv("ENVIRONMENT")
    if not env:
        logging.error(f"[get_all_seat_geek_accounts] ENVIRONMENT variable is not set.")
        return []
    table_name = f"shadows-catalog-{env}"
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression="id = :id",
        ExpressionAttributeValues={":id": acc_id }
    )
    return response.get("Items", [])


def send_seat_geek_purge_message(token: str):
    queue_url = os.getenv("SEATGEEK_DELETE_SQS_QUEUE")
    if not queue_url:
        logging.error(f"SeatGeek SQS queue URL not set in env variables.")
        return None
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    message = {
        "seatgeek_authorization_token": token,
        "event_type": "seatgeek_full_purge",
    }
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        return response.get("MessageId")
    except Exception as e:
        logging.error(f"Error sending SeatGeek purge message to SQS: {e}")
        return None

def update_seat_geek_account_status(sub_id: str, blocked_status: bool, blocked_at: str):
    """
    Update the 'blocked_status' (as a string) and 'blocked_at' fields for a SeatGeek account in DynamoDB.
    Only updates existing account_data fields.
    """
    env = os.getenv("ENVIRONMENT")
    if not env:
        logging.error("[update_seat_geek_account_purge_status] ENVIRONMENT variable is not set.")
        return None
    table_name = f"shadows-catalog-{env}"
    key = {"id": 'seatgeek_account', "sub_id": sub_id}
    update_expression = "SET blocked_status = :blocked_status, blocked_at = :blocked_at"
    expression_attribute_values = {
        ":blocked_status": blocked_status,
        ":blocked_at": blocked_at
    }
    try:
        dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        return response
    except Exception as e:
        logging.error(f"Error updating SeatGeek account purge status: {e}")
        raise HTTPException(status_code=500, detail="Failed to update purge status.")

async def insert_manage_accounts_history(account_id: str, account_name: str, user_name: str, user_role: str, change_event_type: str):
    """
    Insert a record into public.manage_accounts_history for SeatGeek account events.
    """
    try:
        query = """
            INSERT INTO public.manage_accounts_history (account_id, account_name, user_name, user_role, change_event_type, created_at)
            VALUES (:account_id, :account_name, :user_name, :user_role, :change_event_type, NOW())
        """
        values = {
            "account_id": account_id,
            "account_name": account_name,
            "user_name": user_name,
            "user_role": user_role,
            "change_event_type": change_event_type
        }
        db = get_pg_database()
        await db.execute(query=query, values=values)
    except Exception as exc:
        logging.error(f"Exception adding change history record: {exc}")
        return
