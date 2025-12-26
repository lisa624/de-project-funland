
import os
import json
import pytest
from moto import mock_aws
import boto3
from botocore.config import Config
from datetime import datetime, timezone

import awswrangler as wr

from src.lambda_handler.extract import (
    DEFAULT_LAST_CHECKED,
    get_last_checked,
    get_db_credentials,
    create_db_connection,
    update_last_checked,
    extract_new_rows,
    convert_new_rows_to_df_and_upload_to_s3_as_csv,
    get_bucket_name,
)

@pytest.fixture(scope="function")
def aws_config():
    return Config(region_name="eu-west-2")


@pytest.fixture(scope="function")
def ssm_client(aws_config):
    return boto3.client("ssm", config=aws_config)


@pytest.fixture(scope="function")
def sm_client(aws_config):
    return boto3.client("secretsmanager", config=aws_config)


@pytest.fixture(scope="function")
def s3_client(aws_config):
    return boto3.client("s3", config=aws_config)


# -----------------------------
# Fake DB Connection (unit-test safe)
# -----------------------------

class FakeDBConnection:
    """
    Minimal fake for pg8000.native.Connection behaviour used in extract_new_rows:
      - run(query) returns rows
      - columns provides column metadata
    """
    def __init__(self, rows, column_names):
        self._rows = rows
        self.columns = [{"name": c} for c in column_names]
        self.last_query = None

    def run(self, query):
        self.last_query = query
        return self._rows


# -----------------------------
# get_last_checked tests
# -----------------------------

@mock_aws
class TestGetLastChecked:
    def test_get_last_checked_obtains_correct_value_when_present(self, ssm_client):
        new_date = "2025-01-01T00:00:00+00:00"
        ssm_client.put_parameter(Name="last_checked", Value=new_date, Type="String")

        last_checked = get_last_checked(ssm_client)
        assert last_checked["last_checked"] == new_date

    def test_get_last_checked_returns_default_if_missing(self, ssm_client):
        last_checked = get_last_checked(ssm_client)
        assert last_checked["last_checked"] == DEFAULT_LAST_CHECKED


# -----------------------------
# get_db_credentials tests
# -----------------------------

@mock_aws
class TestGetDBCredentials:
    def test_get_db_credentials_fetches_secret_using_env_var(self, sm_client):
        # Arrange
        os.environ["DB_SECRET_NAME"] = "db_creds_secret"

        secret_dict = {
            "user": "user",
            "password": "password",
            "host": "host",
            "port": 5432,
            "database": "test_db",
        }

        sm_client.create_secret(
            Name="db_creds_secret",
            SecretString=json.dumps(secret_dict)
        )

        # Act
        result = get_db_credentials(sm_client)

        # Assert
        assert result == secret_dict

        # Cleanup
        del os.environ["DB_SECRET_NAME"]

    def test_get_db_credentials_raises_if_env_var_missing(self, sm_client):
        if "DB_SECRET_NAME" in os.environ:
            del os.environ["DB_SECRET_NAME"]

        with pytest.raises(ValueError):
            get_db_credentials(sm_client)

    def test_get_db_credentials_raises_if_secret_missing(self, sm_client):
        os.environ["DB_SECRET_NAME"] = "missing_secret"

        with pytest.raises(Exception):
            get_db_credentials(sm_client)

        del os.environ["DB_SECRET_NAME"]


# -----------------------------
# create_db_connection tests (unit-style)
# -----------------------------
# Note: We do NOT actually connect to a real DB here (CI-friendly).
# We just check that missing keys cause KeyError (good signal).

class TestDBConnection:
    def test_create_db_connection_raises_keyerror_if_missing_required_keys(self):
        bad_creds = {"user": "u"}  # missing most keys
        with pytest.raises(KeyError):
            create_db_connection(bad_creds)


# -----------------------------
# get_bucket_name tests
# -----------------------------

class TestGetBucketName:
    def test_get_bucket_name_returns_bucket_string(self):
        os.environ["S3_INGESTION_BUCKET"] = "funland-ingestion-bucket-11"

        result = get_bucket_name()
        assert result == "funland-ingestion-bucket-11"

        del os.environ["S3_INGESTION_BUCKET"]

    def test_get_bucket_name_raises_if_missing(self):
        if "S3_INGESTION_BUCKET" in os.environ:
            del os.environ["S3_INGESTION_BUCKET"]

        with pytest.raises(ValueError):
            get_bucket_name()


# -----------------------------
# update_last_checked tests
# -----------------------------

@mock_aws
class TestUpdateLastChecked:
    def test_update_last_checked_updates_parameter(self, ssm_client):
        # Arrange
        old = "2020-01-01T00:00:00+00:00"
        ssm_client.put_parameter(Name="last_checked", Value=old, Type="String")

        new_value = datetime.now(timezone.utc).isoformat()

        # Act
        updated_value = update_last_checked(ssm_client, new_value)

        # Assert
        stored = ssm_client.get_parameter(Name="last_checked")["Parameter"]["Value"]
        assert updated_value == new_value
        assert stored == new_value


# -----------------------------
# extract_new_rows tests (fake DB)
# -----------------------------

class TestExtractNewRows:
    def test_extract_new_rows_returns_rows_and_columns(self):
        db = FakeDBConnection(
            rows=[[1, "A"], [2, "B"]],
            column_names=["id", "name"]
        )

        cols, rows = extract_new_rows("address", "2020-01-01T00:00:00+00:00", db)

        assert cols == ["id", "name"]
        assert rows == [[1, "A"], [2, "B"]]
        assert "WHERE last_updated >" in db.last_query

    def test_extract_new_rows_department_has_no_last_updated_filter(self):
        db = FakeDBConnection(
            rows=[[1], [2]],
            column_names=["department_id"]
        )

        cols, rows = extract_new_rows("department", "2020-01-01T00:00:00+00:00", db)

        assert cols == ["department_id"]
        assert rows == [[1], [2]]
        assert "WHERE last_updated" not in db.last_query


# -----------------------------
# convert_new_rows_to_df_and_upload_to_s3_as_csv tests
# -----------------------------

@mock_aws
class TestConvertNewRowsToDfAndUploadToS3:
    def test_uploads_csv_to_s3_and_schema_matches(self, s3_client):
        s3_client.create_bucket(
            Bucket="testbucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )

        column_names = ["age", "height"]
        new_rows = [[18, 192.0], [33, 177.4]]
        batch_id = "2020-01-01T00-00-00+00-00"

        convert_new_rows_to_df_and_upload_to_s3_as_csv(
            ingestion_bucket="testbucket",
            table="person",
            column_names=column_names,
            new_rows=new_rows,
            batch_id=batch_id
        )

        df_read = wr.s3.read_csv(f"s3://testbucket/person/{batch_id}.csv")
        assert list(df_read.columns) == ["age", "height"]
        assert len(df_read) == 2

    def test_raises_error_if_bucket_missing(self, s3_client):
        column_names = ["age", "height"]
        new_rows = [[18, 192.0], [33, 177.4]]
        batch_id = "2020-01-01T00-00-00+00-00"

        with pytest.raises(Exception):
            convert_new_rows_to_df_and_upload_to_s3_as_csv(
                ingestion_bucket="bucket-does-not-exist",
                table="person",
                column_names=column_names,
                new_rows=new_rows,
                batch_id=batch_id
            )