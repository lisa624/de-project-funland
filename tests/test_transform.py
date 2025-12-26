import os
import boto3
import pytest
import awswrangler as wr
import pandas as pd
import numpy as np

from datetime import datetime, date, time
from moto import mock_aws
from pandas.testing import assert_frame_equal


from src.lambda_handler.transform import (
    dim_design,
    dim_currency,
    dim_staff,
    dim_counterparty,
    dim_location,
    fact_sales_order,
    dim_date,
    check_file_exists
)


# ---------------------------------------------------------
# Global fixtures
# ---------------------------------------------------------

@pytest.fixture(autouse=True)
def aws_credentials():
    """Fake AWS creds for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ["AWS_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client():
    """S3 client inside moto (region must match bucket LocationConstraint)."""
    return boto3.client("s3", region_name="eu-west-2")


def _create_bucket(s3_client, name: str):
    """Helper to create a region-specific bucket for moto."""
    s3_client.create_bucket(
        Bucket=name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )


# ---------------------------------------------------------
# Tests
# ---------------------------------------------------------

@mock_aws
class TestDimCurrency:
    def test_dim_currency_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-33-elisa-q"
        processed = "processed-bucket-funland-e-l-3"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "2000-01-01 00:00:00.000000"

        df_in = pd.DataFrame(
            [[0, "GBP", datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]],
            columns=["currency_id", "currency_code", "created_at", "last_updated"]
        )
        wr.s3.to_csv(df=df_in, path=f"s3://{ingestion}/currency/{last_checked}.csv", index=False)

        dim_currency(last_checked=last_checked, ingestion_bucket=ingestion, processed_bucket=processed)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_currency/{last_checked}.parquet")

        df_expected = pd.DataFrame([[0, "GBP", "GBP_Name"]], columns=["currency_id", "currency_code", "currency_name"])

        assert_frame_equal(df_out.reset_index(drop=True), df_expected, check_dtype=False)


@mock_aws
class TestDimDesign:
    def test_dim_design_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        processed = "processed-bucket-124-33"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "1995-01-01 00:00:00.000000"

        df_in = pd.DataFrame(
            [[0, datetime(2022, 11, 3, 14, 20, 49, 962000), datetime(2022, 11, 3, 14, 20, 49, 962000),
              "Wooden", "/usr", "wooden-20220717-npgz.json"]],
            columns=["design_id", "created_at", "last_updated", "design_name", "file_location", "file_name"]
        )
        wr.s3.to_csv(df=df_in, path=f"s3://{ingestion}/design/{last_checked}.csv", index=False)

        dim_design(last_checked=last_checked, ingestion_bucket=ingestion, processed_bucket=processed)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_design/{last_checked}.parquet")
        df_expected = pd.DataFrame(
            [[0, "Wooden", "/usr", "wooden-20220717-npgz.json"]],
            columns=["design_id", "design_name", "file_location", "file_name"]
        )

        assert_frame_equal(df_out.reset_index(drop=True), df_expected, check_dtype=False)


@mock_aws
class TestDimStaff:
    def test_dim_staff_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        processed = "processed-bucket-124-33"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "1995-01-01 00:00:00.000000"

        staff_df = pd.DataFrame(
            [[1, "Jeremie", "Franey", 2, "jeremie.franey@terrifictotes.com",
              datetime(2022, 11, 3, 14, 20, 51, 563000), datetime(2022, 11, 3, 14, 20, 51, 563000)]],
            columns=["staff_id", "first_name", "last_name", "department_id", "email_address", "created_at", "last_updated"]
        )
        wr.s3.to_csv(df=staff_df, path=f"s3://{ingestion}/staff/{last_checked}.csv", index=False)

        dept_df = pd.DataFrame(
            [[2, "Purchasing", "Manchester", "Naomi Lapaglia",
              datetime(2022, 11, 3, 14, 20, 49, 962000), datetime(2022, 11, 3, 14, 20, 49, 962000)]],
            columns=["department_id", "department_name", "location", "manager", "created_at", "last_updated"]
        )
        wr.s3.to_csv(df=dept_df, path=f"s3://{ingestion}/department/{last_checked}.csv", index=False)

        dim_staff(last_checked=last_checked, ingestion_bucket=ingestion, processed_bucket=processed)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_staff/{last_checked}.parquet")

        # New transform.py keeps order: staff fields then dept fields, after drops:
        # staff_id, first_name, last_name, email_address, department_name, location
        df_expected = pd.DataFrame(
            [[1, "Jeremie", "Franey", "jeremie.franey@terrifictotes.com", "Purchasing", "Manchester"]],
            columns=["staff_id", "first_name", "last_name", "email_address", "department_name", "location"]
        )

        assert_frame_equal(df_out.reset_index(drop=True), df_expected, check_dtype=False)


@mock_aws
class TestDimLocation:
    def test_dim_location_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-555-22"
        processed = "processed-bucket-555-22"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "1995-01-01 00:00:00.000000"

        df_in = pd.DataFrame(
            [[1, "6826 Herzog Via", None, "New Patienceburgh", "Turkey",
              datetime(2022, 11, 3, 14, 20, 49, 962000), "Avon",
              datetime(2022, 11, 3, 14, 20, 49, 962000), "1803 637401", "28441"]],
            columns=["address_id", "address_line_1", "address_line_2", "city", "country",
                     "created_at", "district", "last_updated", "phone", "postal_code"]
        )
        wr.s3.to_csv(df=df_in, path=f"s3://{ingestion}/address/{last_checked}.csv", index=False)

        dim_location(last_checked=last_checked, ingestion_bucket=ingestion, processed_bucket=processed)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_location/{last_checked}.parquet")

        df_expected = pd.DataFrame(
            [[1, "6826 Herzog Via", None, "Avon", "New Patienceburgh", "28441", "Turkey", "1803 637401"]],
            columns=["location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]
        )

        # Normalize NaNs/None for comparison
        df_out = df_out.replace({np.nan: None})
        df_expected = df_expected.replace({np.nan: None})

        # ✅ IMPORTANT: parquet can load numeric-looking postal codes as ints (28441) instead of strings ("28441")
        # Force both to string so the values compare equal.
        df_out["postal_code"] = df_out["postal_code"].astype(str)
        df_expected["postal_code"] = df_expected["postal_code"].astype(str)

        assert_frame_equal(df_out.reset_index(drop=True), df_expected, check_dtype=False)


@mock_aws
class TestDimCounterparty:
    def test_dim_counterparty_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        processed = "processed-bucket-124-33"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "1995-01-01 00:00:00.000000"

        counterparty_df = pd.DataFrame(
            [["Micheal Toy", 1, "Fahey and Sons",
              datetime(2022, 11, 3, 14, 20, 51, 563000),
              "Mrs. Lucy Runolfsdottir",
              datetime(2022, 11, 3, 14, 20, 51, 563000),
              15]],
            columns=["commercial_contact", "counterparty_id", "counterparty_legal_name",
                     "created_at", "delivery_contact", "last_updated", "legal_address_id"]
        )
        wr.s3.to_csv(df=counterparty_df, path=f"s3://{ingestion}/counterparty/{last_checked}.csv", index=False)

        address_df = pd.DataFrame(
            [[15, "605 Haskell Trafficway", "Axel Freeway", "East Bobbie", "Heard Island and McDonald Islands",
              datetime(2022, 11, 3, 14, 20, 49, 962000), None,
              datetime(2022, 11, 3, 14, 20, 49, 962000), "9687 937447", "88253-4257"]],
            columns=["address_id", "address_line_1", "address_line_2", "city", "country",
                     "created_at", "district", "last_updated", "phone", "postal_code"]
        )
        wr.s3.to_csv(df=address_df, path=f"s3://{ingestion}/address/{last_checked}.csv", index=False)

        dim_counterparty(last_checked, ingestion, processed, s3_client)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_counterparty/{last_checked}.parquet").replace({np.nan: None})

        # New transform.py keeps address_id (does NOT drop it)
        df_expected = pd.DataFrame(
            [[1, "Fahey and Sons", 15,
              "605 Haskell Trafficway", "Axel Freeway", None,
              "East Bobbie", "88253-4257", "Heard Island and McDonald Islands", "9687 937447"]],
            columns=[
                "counterparty_id",
                "counterparty_legal_name",
                "address_id",
                "counterparty_legal_address_line_1",
                "counterparty_legal_address_line_2",
                "counterparty_legal_district",
                "counterparty_legal_city",
                "counterparty_legal_postal_code",
                "counterparty_legal_country",
                "counterparty_legal_phone_number",
            ],
        ).replace({np.nan: None})

        # Compare as sets + values
        assert set(df_out.columns) == set(df_expected.columns)
        df_out = df_out[df_expected.columns]  # align order
        assert_frame_equal(df_out.reset_index(drop=True), df_expected, check_dtype=False)


@mock_aws
class TestDimDate:
    def test_dim_date_writes_expected_parquet(self, s3_client):
        processed = "processed-bucket-333-33"
        _create_bucket(s3_client, processed)

        start = "2022-11-10"
        end = "2022-11-15"

        dim_date(processed_bucket=processed, start=start, end=end)

        df_out = wr.s3.read_parquet(f"s3://{processed}/dim_date/dim_date.parquet")

        assert df_out.shape == (6, 8)
        assert list(df_out.columns) == ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"]

        # Spot check first & last date
        assert pd.Timestamp("2022-11-10") == df_out.iloc[0]["date_id"]
        assert pd.Timestamp("2022-11-15") == df_out.iloc[-1]["date_id"]


@mock_aws
class TestFactSalesOrder:
    def test_fact_sales_order_writes_expected_parquet(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        processed = "processed-bucket-124-33"
        _create_bucket(s3_client, ingestion)
        _create_bucket(s3_client, processed)

        last_checked = "1995-01-01 00:00:00.000000"

        sales_df = pd.DataFrame(
            [[2, datetime(2022, 11, 3, 14, 20, 52, 186000),
              datetime(2022, 11, 3, 14, 20, 52, 186000), 3,
              19, 8, 42972, 3.94, 2, "2022-11-07", "2022-11-08", 8]],
            columns=[
                "sales_order_id", "created_at", "last_updated", "design_id",
                "staff_id", "counterparty_id", "units_sold", "unit_price",
                "currency_id", "agreed_delivery_date", "agreed_payment_date",
                "agreed_delivery_location_id"
            ],
        )
        wr.s3.to_csv(df=sales_df, path=f"s3://{ingestion}/sales_order/{last_checked}.csv", index=False)

        fact_sales_order(last_checked, ingestion, processed)

        df_out = wr.s3.read_parquet(f"s3://{processed}/fact_sales_order/{last_checked}.parquet")

        # Check a few critical values (avoid dtype issues from parquet)
        row = df_out.iloc[0]

        assert int(row["sales_record_id"]) == 2
        assert int(row["sales_order_id"]) == 2
        assert int(row["sales_staff_id"]) == 19
        assert int(row["counterparty_id"]) == 8
        assert int(row["design_id"]) == 3
        assert int(row["currency_id"]) == 2
        assert float(row["unit_price"]) == 3.94
        assert int(row["units_sold"]) == 42972

        # dates
        assert row["created_date"] == date(2022, 11, 3)
        assert row["last_updated_date"] == date(2022, 11, 3)
        assert row["agreed_delivery_date"] == date(2022, 11, 7)
        assert row["agreed_payment_date"] == date(2022, 11, 8)


@mock_aws
class TestCheckFileExists:
    def test_returns_false_when_key_missing(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        _create_bucket(s3_client, ingestion)

        assert check_file_exists(bucket=ingestion, key="does/not/exist.csv") is False

    def test_returns_true_when_key_exists(self, s3_client):
        ingestion = "ingestion-bucket-124-33"
        _create_bucket(s3_client, ingestion)

        df = pd.DataFrame([[1]], columns=["x"])
        wr.s3.to_csv(df=df, path=f"s3://{ingestion}/design/abc.csv", index=False)

        assert check_file_exists(bucket=ingestion, key="design/abc.csv") is True
