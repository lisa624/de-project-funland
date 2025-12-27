import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# -------------------------------------------------------------------
# Lambda entrypoint
# -------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Transform data from ingestion S3 to processed S3 (star schema).
    """

    last_checked = event.get("timestamp_to_transform")
    if not last_checked:
        raise ValueError("Missing timestamp_to_transform in event")

    ingestion_bucket = os.environ.get("S3_INGESTION_BUCKET")
    processed_bucket = os.environ.get("S3_PROCESSED_BUCKET")

    if not ingestion_bucket or not processed_bucket:
        raise ValueError("Missing required S3 bucket environment variables")

    s3_client = boto3.client("s3")

    fact_sales_order(last_checked, ingestion_bucket, processed_bucket)
    dim_currency(last_checked, ingestion_bucket, processed_bucket)
    dim_location(last_checked, ingestion_bucket, processed_bucket)
    dim_design(last_checked, ingestion_bucket, processed_bucket)
    dim_staff(last_checked, ingestion_bucket, processed_bucket)
    dim_counterparty(last_checked, ingestion_bucket, processed_bucket, s3_client)

    # Generate dim_date every run (cheap & safe)
    dim_date(
        processed_bucket=processed_bucket,
        start="2020-01-01",
        end="2030-12-31"
    )

    return {
        "message": "transform_success",
        "timestamp": last_checked
    }

# -------------------------------------------------------------------
# Helper utilities
# -------------------------------------------------------------------

def check_file_exists(bucket: str, key: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

# -------------------------------------------------------------------
# Dimension tables
# -------------------------------------------------------------------

def dim_currency(last_checked, ingestion_bucket, processed_bucket):
    key = f"currency/{last_checked}.csv"
    if not check_file_exists(ingestion_bucket, key):
        logger.info("No currency file found")
        return

    df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{key}")
    df = df.drop(columns=["Unnamed: 0", "created_at", "last_updated"], errors="ignore")
    df["currency_name"] = df["currency_code"] + "_Name"

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_currency/{last_checked}.parquet"
    )


def dim_location(last_checked, ingestion_bucket, processed_bucket):
    key = f"address/{last_checked}.csv"
    if not check_file_exists(ingestion_bucket, key):
        logger.info("No address file found")
        return

    df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{key}")

    df = df.rename(columns={"address_id": "location_id"})
    df = df.drop(columns=["Unnamed: 0", "created_at", "last_updated"], errors="ignore")

    # ✅ force postal_code to string (handles 28441 and 99305-7380)
    if "postal_code" in df.columns:
        df["postal_code"] = df["postal_code"].astype("string")

    final_columns = [
        "location_id", "address_line_1", "address_line_2", "district",
        "city", "postal_code", "country", "phone"
    ]
    df = df[final_columns]

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_location/{last_checked}.parquet"
    )




def dim_staff(last_checked, ingestion_bucket, processed_bucket):
    staff_key = f"staff/{last_checked}.csv"
    dept_key = f"department/{last_checked}.csv"

    if not check_file_exists(ingestion_bucket, staff_key):
        logger.info("No staff file found")
        return

    staff_df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{staff_key}")

    if check_file_exists(ingestion_bucket, dept_key):
        dept_df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{dept_key}")
        df = staff_df.merge(dept_df, on="department_id", how="left")
    else:
        df = staff_df

    df = df.drop(
        columns=[
            "Unnamed: 0_x", "Unnamed: 0_y",
            "department_id", "manager",
            "created_at_x", "created_at_y",
            "last_updated_x", "last_updated_y"
        ],
        errors="ignore"
    )

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_staff/{last_checked}.parquet"
    )


def dim_counterparty(last_checked, ingestion_bucket, processed_bucket, s3_client):
    cp_key = f"counterparty/{last_checked}.csv"
    if not check_file_exists(ingestion_bucket, cp_key):
        logger.info("No counterparty file found")
        return

    response = s3_client.list_objects_v2(
        Bucket=ingestion_bucket,
        Prefix="address/"
    )

    address_key = max(
         response["Contents"],
        key=lambda x: x["LastModified"]
    )["Key"]

    cp_df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{cp_key}")
    addr_df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{address_key}")

    cp_df = cp_df.rename(columns={"legal_address_id": "address_id"})
    df = cp_df.merge(addr_df, on="address_id", how="left")

    df = df.drop(
        columns=[
            "Unnamed: 0_x", "Unnamed: 0_y",
            "created_at_x", "created_at_y",
            "last_updated_x", "last_updated_y",
            "commercial_contact", "delivery_contact"
        ],
        errors="ignore"
    )

    df = df.rename(columns={
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "city": "counterparty_legal_city",
        "country": "counterparty_legal_country",
        "district": "counterparty_legal_district",
        "phone": "counterparty_legal_phone_number",
        "postal_code": "counterparty_legal_postal_code",
    })

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_counterparty/{last_checked}.parquet"
    )

def dim_design(last_checked, ingestion_bucket, processed_bucket):
    key = f"design/{last_checked}.csv"
    if not check_file_exists(ingestion_bucket, key):
        logger.info("No design file found")
        return

    df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{key}")
    df = df.drop(columns=["Unnamed: 0", "created_at", "last_updated"], errors="ignore")

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_design/{last_checked}.parquet"
    )


# -------------------------------------------------------------------
# Date dimension
# -------------------------------------------------------------------

def dim_date(processed_bucket, start, end):
    df = pd.DataFrame({"date_id": pd.date_range(start, end)})

    df["year"] = df.date_id.dt.year
    df["month"] = df.date_id.dt.month
    df["day"] = df.date_id.dt.day
    df["day_of_week"] = df.date_id.dt.dayofweek
    df["day_name"] = df.date_id.dt.day_name()
    df["month_name"] = df.date_id.dt.month_name()
    df["quarter"] = df.date_id.dt.quarter

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/dim_date/dim_date.parquet"
    )

# -------------------------------------------------------------------
# Fact table
# -------------------------------------------------------------------

def fact_sales_order(last_checked, ingestion_bucket, processed_bucket):
    key = f"sales_order/{last_checked}.csv"
    if not check_file_exists(ingestion_bucket, key):
        logger.info("No sales_order file found")
        return

    df = wr.s3.read_csv(f"s3://{ingestion_bucket}/{key}")
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    df["sales_record_id"] = df["sales_order_id"]

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["created_date"] = df["created_at"].dt.date
    df["created_time"] = df["created_at"].dt.time

    df["last_updated"] = pd.to_datetime(df["last_updated"])
    df["last_updated_date"] = df["last_updated"].dt.date
    df["last_updated_time"] = df["last_updated"].dt.time

    df["sales_staff_id"] = df["staff_id"]

    df["agreed_delivery_date"] = pd.to_datetime(df["agreed_delivery_date"]).dt.date
    df["agreed_payment_date"] = pd.to_datetime(df["agreed_payment_date"]).dt.date

    final_columns = [
        "sales_record_id", "sales_order_id",
        "created_date", "created_time",
        "last_updated_date", "last_updated_time",
        "sales_staff_id", "counterparty_id",
        "units_sold", "unit_price",
        "currency_id", "design_id",
        "agreed_payment_date", "agreed_delivery_date",
        "agreed_delivery_location_id"
    ]

    df = df[final_columns]

    wr.s3.to_parquet(
        df,
        f"s3://{processed_bucket}/fact_sales_order/{last_checked}.parquet"
    )


