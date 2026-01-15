import os
import time

import boto3
from databases import Database
import snowflake.connector
from opensearchpy import OpenSearch, AsyncOpenSearch, AWSV4SignerAsyncAuth, AsyncHttpConnection

_snowflake_connection = None
_pg_database = None
_pg_buylist_database = None
_pg_buylist_readonly_database = None
_pg_realtime_catalog_database = None
_pg_open_distribution_database = None
_pg_open_distribution_readonly_database = None
_opensearch_client = None

environment = os.getenv("ENVIRONMENT", "dev")
aws_region = os.getenv("AWS_REGION", "us-east-1")


# Snowflake connection setup
def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    global _snowflake_connection
    if _snowflake_connection:
        return _snowflake_connection
    _snowflake_connection = _create_snowflake_connection()
    return _snowflake_connection


def _create_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    creds = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "client_session_keep_alive": True,
    }
    
    # Check if all required credentials are present
    required_fields = ["account", "user", "password", "warehouse", "database", "schema", "role"]
    missing_fields = [field for field in required_fields if not creds.get(field)]
    
    if missing_fields:
        raise ValueError(f"Missing required Snowflake environment variables: {', '.join(missing_fields)}")

    connection = snowflake.connector.connect(**creds)

    return connection


# DynamoDB connection setup
def get_dynamodb(region_name: str = aws_region) -> boto3.resource:
    """
    Get a DynamoDB resource.

    :param region_name: AWS region name.
    :return: DynamoDB resource.
    """
    if environment == "dev":
        return boto3.resource(
            "dynamodb",
            endpoint_url=f"http://127.0.0.1:{os.getenv('LOCAL_DYNAMODB_PORT', 8000)}/",
            region_name=region_name,  # Default region
            aws_access_key_id="fakeMyKeyId",  # Fake credentials for local DynamoDB
            aws_secret_access_key="fakeSecretAccessKey",
        )
    return boto3.resource("dynamodb", region_name=region_name)


# PostgreSQL connection setup
def get_pg_database() -> Database:
    database_url = os.getenv("POSTGRES_URL")
    global _pg_database
    if _pg_database:
        return _pg_database
    _pg_database = _create_pg_database(database_url)
    return _pg_database


def get_pg_readonly_database() -> Database:
    database_url = os.getenv("POSTGRES_READONLY_URL")
    global _pg_database
    if _pg_database:
        return _pg_database
    _pg_database = _create_pg_database(database_url)
    return _pg_database


def get_pg_buylist_database() -> Database:
    database_url = os.getenv("POSTGRES_URL_BUYLIST")
    global _pg_buylist_database
    if _pg_buylist_database:
        return _pg_buylist_database
    _pg_buylist_database = _create_pg_database(database_url)
    return _pg_buylist_database


def get_pg_buylist_readonly_database() -> Database:
    database_url = os.getenv("POSTGRES_URL_BUYLIST_READONLY")
    global _pg_buylist_readonly_database
    if _pg_buylist_readonly_database:
        return _pg_buylist_readonly_database
    _pg_buylist_readonly_database = _create_pg_database(database_url)
    return _pg_buylist_readonly_database


def get_pg_realtime_catalog_database() -> Database:
    database_url = os.getenv("POSTGRES_REALTIME_CATALOG")
    global _pg_realtime_catalog_database
    if _pg_realtime_catalog_database:
        return _pg_realtime_catalog_database
    _pg_realtime_catalog_database = _create_pg_database(database_url)
    print(_pg_realtime_catalog_database)
    return _pg_realtime_catalog_database

def get_pg_open_distribution_database() -> Database:
    database_url = os.getenv("POSTGRES_URL_OD")
    global _pg_open_distribution_database
    if _pg_open_distribution_database:
        return _pg_open_distribution_database
    _pg_open_distribution_database = _create_pg_database(database_url)
    return _pg_open_distribution_database


def get_pg_open_distribution_readonly_database() -> Database:
    database_url = os.getenv("POSTGRES_READONLY_URL_OD")
    global _pg_open_distribution_readonly_database
    if _pg_open_distribution_readonly_database:
        return _pg_open_distribution_readonly_database
    _pg_open_distribution_readonly_database = _create_pg_database(database_url)
    return _pg_open_distribution_readonly_database


def _create_pg_database(database_url: str) -> Database:
    return Database(database_url, min_size=5, max_size=20)


# Initialization of the PostgreSQL connection
async def init_pg_database():
    for database in (
            get_pg_database(),
            get_pg_readonly_database(),
            get_pg_buylist_database(),
            get_pg_buylist_readonly_database(),
            get_pg_realtime_catalog_database(),
            get_pg_open_distribution_database(),
            get_pg_open_distribution_readonly_database(),
    ):
        if not database.is_connected:
            await database.connect()


# Clean up the PostgreSQL connection
async def close_pg_database():
    for database in (
            get_pg_database(),
            get_pg_readonly_database(),
            get_pg_buylist_database(),
            get_pg_buylist_readonly_database(),
            get_pg_realtime_catalog_database(),
            get_pg_open_distribution_database(),
            get_pg_open_distribution_readonly_database(),
    ):
        if database.is_connected:
            await database.disconnect()


def get_opensearch_client() -> OpenSearch:
    global _opensearch_client
    if not _opensearch_client:
        _opensearch_client = OpenSearch(
            hosts=[{"host": os.environ["OPENSEARCH_ENDPOINT"], "port": 443}],
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=30,
        )
    return _opensearch_client


def get_aws_auth():
    session = boto3.Session()
    credentials = session.get_credentials()
    if not credentials:
        raise ValueError("AWS credentials not found. Ensure Lambda has proper IAM role.")
    return AWSV4SignerAsyncAuth(credentials, aws_region)


def get_async_opensearch_client() -> AsyncOpenSearch:
    conn_start_time = time.time()
    auth = get_aws_auth()
    client = AsyncOpenSearch(
        hosts=[{"host": os.getenv("OPENSEARCH_ENDPOINT"), "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        timeout=20,
        connection_class=AsyncHttpConnection,
    )
    print("Initializing OpenSearch connection...")
    conn_duration = time.time() - conn_start_time
    print(f"Connection initialization time: {conn_duration:.3f} seconds")
    return client
