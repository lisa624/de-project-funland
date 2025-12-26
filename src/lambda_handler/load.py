import os
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

import pandas as pd
import awswrangler as wr

from pg8000.native import Connection, identifier, literal, InterfaceError, DatabaseError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Load parquet data from processed S3 into the warehouse database.

    Expected env vars:
      - S3_PROCESSED_BUCKET
      - DB_SECRET_NAME   (Secrets Manager secret containing connection JSON)

    Event:
      We accept either:
        - event["timestamp"] (your new transform.py returns this)
        - event["timestamp_to_transform"] (older pattern)
    """

    processed_bucket = os.environ.get("S3_PROCESSED_BUCKET")
    if not processed_bucket:
        raise ValueError("Missing environment variable S3_PROCESSED_BUCKET")

    # Accept both shapes (so your pipeline doesn’t break if you tweak transform output)
    last_checked = event.get("timestamp") or event.get("timestamp_to_transform")
    if not last_checked:
        raise ValueError("Missing timestamp in event (expected 'timestamp' or 'timestamp_to_transform')")

    sm_client = boto3.client("secretsmanager")

    db_credentials = get_db_credentials(sm_client)
    conn = create_db_connection(db_credentials)

    try:
        loaded = []

        # Load all normal tables that are written as:
        # s3://processed_bucket/<prefix>/<last_checked>.parquet
        for prefix, target_table in TABLE_CONFIG:
            parquet_key = f"{prefix}/{last_checked}.parquet"
            s3_path = f"s3://{processed_bucket}/{parquet_key}"

            if not s3_object_exists(processed_bucket, parquet_key):
                logger.info(f"Skipping {target_table} (no file found): {s3_path}")
                continue

            df = wr.s3.read_parquet(path=s3_path)
            logger.info(f"Read {len(df)} rows from {s3_path}")

            # For first-time pipelines: easiest reliable approach
            # - wipe table
            # - insert fresh
            truncate_table(conn, target_table)
            insert_dataframe(conn, target_table, df)

            loaded.append(target_table)

        # Load dim_date (always present / fixed path in your transform.py)
        dim_date_s3_path = f"s3://{processed_bucket}/{DIM_DATE_KEY}"
        if s3_object_exists(processed_bucket, DIM_DATE_KEY):
            df_date = wr.s3.read_parquet(path=dim_date_s3_path)
            logger.info(f"Read {len(df_date)} rows from {dim_date_s3_path}")

            truncate_table(conn, "dim_date")
            insert_dataframe(conn, "dim_date", df_date)
            loaded.append("dim_date")
        else:
            logger.info(f"Skipping dim_date (no file found): {dim_date_s3_path}")

        return {
            "message": "load_success",
            "timestamp_loaded": last_checked,
            "tables_loaded": loaded,
        }

    finally:
        try:
            conn.close()
            logger.info("Warehouse connection closed")
        except Exception:
            logger.warning("Failed to close warehouse connection cleanly")





TABLE_CONFIG = [
    # parquet_key_prefix_in_processed_bucket, target_table_name_in_warehouse
    ("dim_currency", "dim_currency"),
    ("dim_location", "dim_location"),
    ("dim_design", "dim_design"),
    ("dim_staff", "dim_staff"),
    ("dim_counterparty", "dim_counterparty"),
    ("fact_sales_order", "fact_sales_order"),
]


DIM_DATE_KEY = "dim_date/dim_date.parquet"


# ---------------------------------------------------------
# Secrets + DB connection
# ---------------------------------------------------------

def get_db_credentials(sm_client) -> dict:
    """
    Reads a Secrets Manager secret whose name is in env var DB_SECRET_NAME.

    Secret JSON should look like (example keys):
      {
        "host": "...",
        "port": 5432,
        "database": "...",
        "user": "...",
        "password": "..."
      }
    """
    secret_name = os.environ.get("DB_SECRET_NAME")
    if not secret_name:
        raise ValueError("Missing environment variable DB_SECRET_NAME")

    try:
        resp = sm_client.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])
    except sm_client.exceptions.ResourceNotFoundException as e:
        logger.error(f"Secret not found: {secret_name}")
        raise
    except ClientError as e:
        logger.error(f"Secrets Manager ClientError: {str(e)}")
        raise


def create_db_connection(db_credentials: dict) -> Connection:
    """
    Creates a pg8000.native Connection to your warehouse DB.
    """
    try:
        return Connection(
            host=db_credentials["host"],
            port=int(db_credentials["port"]),
            database=db_credentials["database"],
            user=db_credentials["user"],
            password=db_credentials["password"],
        )
    except KeyError as e:
        raise KeyError(f"Missing key in DB secret JSON: {e}") from e
    except InterfaceError as e:
        logger.error(f"Cannot connect to DB (InterfaceError): {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected DB connection error: {e}")
        raise


# ---------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------

def s3_object_exists(bucket: str, key: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ---------------------------------------------------------
# Warehouse helpers
# ---------------------------------------------------------

def truncate_table(conn: Connection, table_name: str) -> None:
    """
    TRUNCATE table before loading (simple + deterministic for a first pipeline).
    """
    sql = f"TRUNCATE TABLE {identifier(table_name)};"
    try:
        conn.run(sql)
        logger.info(f"Truncated table: {table_name}")
    except DatabaseError as e:
        logger.error(f"DatabaseError truncating {table_name}: {e}")
        raise


def insert_dataframe(conn: Connection, table_name: str, df: pd.DataFrame) -> None:
    """
    Inserts a pandas DataFrame into a Postgres table using pg8000.native.

    Notes:
    - This does row-by-row INSERTs (fine for coursework / small-ish loads).
    - Converts NaN to None so Postgres accepts nulls.
    """

    if df is None or df.empty:
        logger.info(f"No rows to insert for table={table_name}")
        return

    # Convert NaN to None (important for Postgres)
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    col_sql = ", ".join(identifier(c) for c in cols)

    try:
        row_count = 0
        for row in df.itertuples(index=False, name=None):
            values_sql = ", ".join(literal(v) for v in row)
            sql = f"INSERT INTO {identifier(table_name)} ({col_sql}) VALUES ({values_sql});"
            conn.run(sql)
            row_count += 1

        logger.info(f"Inserted {row_count} rows into {table_name}")

    except DatabaseError as e:
        logger.error(f"DatabaseError inserting into {table_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting into {table_name}: {e}")
        raise