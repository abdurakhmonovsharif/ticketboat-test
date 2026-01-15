import json
import os
import boto3
from fastapi import Depends
from nrdtech_aws_utils.secrets_helper import SecretsHelper
import firebase_admin
from firebase_admin import credentials, App
import firebase_admin.exceptions


def initialize_firebase():
    get_firebase_app()


def get_firebase_app() -> App:
    if not firebase_admin._apps:
        with SecretsHelper(
            boto3.client("secretsmanager", region_name="us-east-1")
        ) as secrets_helper:
            firebase_credentials_secret = secrets_helper.get_secret(
                os.environ["FIREBASE_AWS_SECRET_NAME"]
            )
            firebase_credentials = json.loads(
                firebase_credentials_secret.get("firebase_credentials")
            )
            cred_certificate = credentials.Certificate(firebase_credentials)
            firebase_app = firebase_admin.initialize_app(
                cred_certificate,
                {"databaseURL": os.environ["FIREBASE_REALTIME_DATABASE_URL"]},
            )
    else:
        firebase_app = firebase_admin.get_app()
    return firebase_app
