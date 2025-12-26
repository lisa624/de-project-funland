import os
import json
import pytest
import boto3
import pandas as pd
import awswrangler as wr

from moto import mock_aws
from botocore.exceptions import ClientError
from unittest.mock import patch

from src.lambda_handler.load import (
    lambda_handler,
    get_db_credentials,
    create_db_connection,
    s3_object_exists,
    truncate_table,
    insert_dataframe,
    TABLE_CONFIG,
    DIM_DATE_KEY,
)


# -----------------------------
# Helpers / fakes
# -----------------------------

class FakeConn:
    """Simple fake pg8000 connection that records SQL queries."""
    def __init__(self):
        self.queries = []
        self.closed = False

    def run(self, sql: str):
        self.queries.append(sql)
        return None

    def close(self):
        self.closed = True


def _create_bucket(s3_client, name: str, region="eu-west-2"):
    s3_client.create_bucket(
        Bucket=name,
        CreateBucketConfiguration={"LocationConstraint": region},
    )


# -----------------------------
# Fixtures
# -----------------------------

@pytest.fixture(autouse=True)
def aws_env():
    """Set fake AWS creds + region for moto + awswrangler."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["AWS_REGION"] = "eu-west-2"


@pytest.fixture()
def s3_client():
    return boto3.client("s3", region_name="eu-west-2")


@pytest.fixture()
def sm_client():
    return boto3.client("secretsmanager", region_name="eu-west-2")


# -----------------------------
# Unit tests: Secrets + S3
# -----------------------------

@mock_aws
def test_get_db_credentials_reads_secret(sm_client):
    os.environ["DB_SECRET_NAME"] = "warehouse_creds"

    secret_value = {
        "host": "example",
        "port": 5432,
        "database": "db",
        "user": "u",
        "password": "p",
    }
    sm_client.create_secret(Name="warehouse_creds", SecretString=json.dumps(secret_value))

    creds = get_db_credentials(sm_client)
    assert creds == secret_value


@mock_aws
def test_get_db_credentials_missing_env_raises(sm_client):
    if "DB_SECRET_NAME" in os.environ:
        del os.environ["DB_SECRET_NAME"]

    with pytest.raises(ValueError, match="DB_SECRET_NAME"):
        get_db_credentials(sm_client)


@mock_aws
def test_s3_object_exists_true_and_false(s3_client):
    bucket = "processed-bucket"
    _create_bucket(s3_client, bucket)

    # Key doesn't exist
    assert s3_object_exists(bucket, "missing.parquet") is False

    # Put any object
    s3_client.put_object(Bucket=bucket, Key="exists.parquet", Body=b"x")
    assert s3_object_exists(bucket, "exists.parquet") is True


# -----------------------------
# Unit tests: DB helpers
# -----------------------------

def test_truncate_table_generates_sql():
    conn = FakeConn()
    truncate_table(conn, "dim_currency")
    assert any("TRUNCATE TABLE" in q.upper() for q in conn.queries)


def test_insert_dataframe_inserts_rows():
    conn = FakeConn()
    df = pd.DataFrame(
        [
            [1, "GBP"],
            [2, "USD"],
        ],
        columns=["currency_id", "currency_code"],
    )

    insert_dataframe(conn, "dim_currency", df)

    # Should contain 2 INSERT statements
    inserts = [q for q in conn.queries if q.strip().upper().startswith("INSERT INTO")]
    assert len(inserts) == 2


def test_insert_dataframe_skips_empty_df():
    conn = FakeConn()
    df = pd.DataFrame(columns=["a", "b"])
    insert_dataframe(conn, "any", df)
    assert conn.queries == []


# -----------------------------
# Integration-ish test: lambda_handler
# -----------------------------

@mock_aws
def test_lambda_handler_loads_tables_from_s3(s3_client, sm_client):
    """
    Full flow:
    - Create processed bucket
    - Create required parquet files
    - Create secret
    - Patch create_db_connection to return FakeConn
    - Run lambda_handler
    - Assert it TRUNCATE + INSERT for each table that exists
    """
    processed_bucket = "processed-bucket"
    _create_bucket(s3_client, processed_bucket)

    os.environ["S3_PROCESSED_BUCKET"] = processed_bucket
    os.environ["DB_SECRET_NAME"] = "warehouse_creds"

    # Create secret
    sm_client.create_secret(
        Name="warehouse_creds",
        SecretString=json.dumps(
            {
                "host": "example",
                "port": 5432,
                "database": "db",
                "user": "u",
                "password": "p",
            }
        ),
    )

    last_checked = "1995-01-01 00:00:00.000000"

    # Create parquet files for a couple tables (not necessarily all)
    # We'll create 2 tables from TABLE_CONFIG + dim_date
    df_currency = pd.DataFrame([[1, "GBP", "GBP_Name"]], columns=["currency_id", "currency_code", "currency_name"])
    wr.s3.to_parquet(df=df_currency, path=f"s3://{processed_bucket}/dim_currency/{last_checked}.parquet")

    df_location = pd.DataFrame(
        [[1, "addr1", None, None, "city", "12345", "country", "phone"]],
        columns=["location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"],
    )
    wr.s3.to_parquet(df=df_location, path=f"s3://{processed_bucket}/dim_location/{last_checked}.parquet")

    df_date = pd.DataFrame({"date_id": pd.date_range("2022-01-01", "2022-01-03")})
    df_date["year"] = df_date.date_id.dt.year
    df_date["month"] = df_date.date_id.dt.month
    df_date["day"] = df_date.date_id.dt.day
    df_date["day_of_week"] = df_date.date_id.dt.dayofweek
    df_date["day_name"] = df_date.date_id.dt.day_name()
    df_date["month_name"] = df_date.date_id.dt.month_name()
    df_date["quarter"] = df_date.date_id.dt.quarter
    wr.s3.to_parquet(df=df_date, path=f"s3://{processed_bucket}/{DIM_DATE_KEY}")

    fake_conn = FakeConn()

    # Patch ONLY create_db_connection so we don't need a real DB
    with patch("src.lambda_handler.load.create_db_connection", return_value=fake_conn):
        result = lambda_handler({"timestamp": last_checked}, None)

    assert result["message"] == "load_success"
    assert result["timestamp_loaded"] == last_checked

    # We created dim_currency, dim_location, dim_date only
    assert set(result["tables_loaded"]) == {"dim_currency", "dim_location", "dim_date"}

    # Verify TRUNCATE + INSERT happened for those tables
    # dim_currency: 1 row => 1 INSERT
    # dim_location: 1 row => 1 INSERT
    # dim_date: 3 rows => 3 INSERTS
    truncates = [q for q in fake_conn.queries if q.strip().upper().startswith("TRUNCATE TABLE")]
    inserts = [q for q in fake_conn.queries if q.strip().upper().startswith("INSERT INTO")]

    # We should have 3 truncates
    assert len(truncates) == 3

    # Total inserts = 1 + 1 + 3 = 5
    assert len(inserts) == 5

    assert fake_conn.closed is True


@mock_aws
def test_lambda_handler_missing_env_raises(sm_client):
    # Missing processed bucket env var
    if "S3_PROCESSED_BUCKET" in os.environ:
        del os.environ["S3_PROCESSED_BUCKET"]

    with pytest.raises(ValueError, match="S3_PROCESSED_BUCKET"):
        lambda_handler({"timestamp": "x"}, None)


@mock_aws
def test_lambda_handler_missing_timestamp_raises(s3_client, sm_client):
    processed_bucket = "processed-bucket"
    _create_bucket(s3_client, processed_bucket)
    os.environ["S3_PROCESSED_BUCKET"] = processed_bucket

    with pytest.raises(ValueError, match="timestamp"):
        lambda_handler({}, None)
