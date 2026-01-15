import csv
import os
from ftplib import FTP_TLS

import boto3
import snowflake
from starlette.responses import JSONResponse

from app.aws.dynamo_manager import get_dynamodb_manager
from app.database import get_snowflake_connection


def get_vivid_csv_by_account_(account_id, marketplace):
    s3_client = boto3.client("s3")
    with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT * 
            FROM {marketplace}_POSSIBLE_LISTINGS_DT
            WHERE {marketplace}_account_id = %(account_id)s
            """,
            {'account_id': account_id}
        )
        file_path = f"/tmp/{account_id}.csv"
        columns = [desc[0] for desc in cur.description]
        with open(file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(columns)  # Write header

            # Write data rows with values in correct order
            for row in cur:
                writer.writerow([row[col] for col in columns])
        if marketplace == 'vivid':
            _upload_vivid_csv_using_ftp(file_path, account_id)
        elif marketplace == 'gotickets':
            _upload_gotickets_csv_using_ftp(file_path, account_id)
        s3_client.upload_file(file_path, "ticketboat-event-data", f"csv/{account_id}.csv")
        return JSONResponse({"status": "ok"})


def _upload_gotickets_csv_using_ftp(file_path, account):
    res = get_dynamodb_manager().get_items_with_id_and_sub_id_prefix(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                                                     f"gotickets_ftp_account",
                                                                     f"gotickets_account_id#{account}/")[0]
    FTP_HOST = "ftp.gotickets.com"
    FTP_PORT = 21
    FTP_USER = res.get('user')
    FTP_PASS = res.get('password')
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()

    with open(file_path, "rb") as file:
        ftps.storbinary(f"STOR {account}.csv", file)

    print(f"Successfully uploaded {file_path} to {account}.csv")
    ftps.quit()


def _upload_vivid_csv_using_ftp(file_path, account):
    res = get_dynamodb_manager().get_items_with_id_and_sub_id_prefix(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                                                     f"vivid_ftp_account",
                                                                     f"vivid_account_id#{account}/")[0]
    FTP_HOST = "ftp.vividseats.com"
    FTP_PORT = 21
    FTP_USER = res.get('user')
    FTP_PASS = res.get('password')
    ftps = FTP_TLS()
    ftps.connect(FTP_HOST, FTP_PORT)
    ftps.login(FTP_USER, FTP_PASS)
    ftps.prot_p()

    with open(file_path, "rb") as file:
        ftps.storbinary(f"STOR {account}.csv", file)

    print(f"Successfully uploaded {file_path} to {account}.csv")

    ftps.quit()


def get_csv_accounts(marketplace):
    accounts = get_dynamodb_manager().get_items_with_id_and_sub_id_prefix(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                                                          f"{marketplace}_ftp_account",
                                                                          f"{marketplace}_account_id")
    acc = []
    for account in accounts:
        acc.append(
            {"account": account.get("account_id"), "is_realtime_stopped": account.get("stop_realtime")})
    return acc


def stop_realtime(marketplace, account, new_value):
    res = get_dynamodb_manager().update_item(f"shadows-catalog-{os.getenv('ENVIRONMENT')}",
                                             {"id": f"{marketplace}_ftp_account",
                                              "sub_id": f"{marketplace}_account_id#{account}/"},
                                             "SET stop_realtime = :val",
                                             {
                                                 ':val': new_value
                                             }
                                             )
    return res
