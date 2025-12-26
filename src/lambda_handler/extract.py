import os
import logging
from pg8000.native import Connection, identifier, literal, DatabaseError, InterfaceError
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import json
import pandas as pd
import awswrangler as wr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


##################################################################################
# Lambda Handler
##################################################################################
    

def lambda_handler(event, context):
    """ summary
    
    - obtain last_checked via ssm and store in a variable 
    (in a specified timezone).
    Store this in a variable so that it can be 
    passed onto the next lambda handler(Transform) in order to 
    transform the newly updated rows.
        tests:
        - check that last_checked is a datetime in the past and not in the future in the given time zone?
        - check, change, check to see of obtaining is dynamic?
        
    
    - obtain db_creds via secret manager and store in a variable
        tests:
        - check the password is a string and the port is an int etc. 
    
    - create db connection using db creds
        tests:
        - check that successful connection is made.
        - check for a unsuccessful connection.
    
    - query for the new data using connection and last_checked
        test:
        - test that query list is same length as number of tables.
        - test that the rows that are chosen have a last_updated 
        date after our last_checked date.
        (checking each last_updated cloumn in a for loop)
        
    -format the data into a pandas df and upload to s3 bucket.
        test:
        - test that the file exists
        - test that the content is valid
    
    - update last checked variable ## figure out where to do this step so we have access to the latest files for transform step.
        test:
        - test to see if the function updates the 
        variable in the parameter store as expected
    
   
    
    - obtain bucket name
        test:
        - that the bucket name starts with funland-ingestion-bucket-...... where the rest is numbers.
    

    

    Args:
        no inputs as this is the first lambda
        
        
    Returns:
        dictionary
        Optional:
            - maybe the old timestamp?
            - maybe a success message?
            {"timestamp":"2020....", "message":"extract successful"}
    """
    ssm_client=boto3.client("ssm")
    sm_client=boto3.client("secretsmanager")
    logger.info("created s3 clients")
    
    last_checked = get_last_checked(ssm_client)["last_checked"]
    logger.info(f"obtained last checked: {last_checked}")
    
    #Fetch DB creds
    db_credentials = get_db_credentials(sm_client)
    logger.info("Database credentials fetched successfully")
    
    
    db_conn = create_db_connection(db_credentials)
    logger.info(f"created db connection")
    
    tables_to_import = ["transaction", "sales_order", 
                        "payment","counterparty", 
                        "currency", "department", 
                        "design", "staff",
                        "address", "purchase_order",
                        "payment_type"]
    
    
    ingestion_bucket = get_bucket_name()
    logger.info(f"obtained ingestion bucket name: {ingestion_bucket}")
    # Use a safe batch_id for file naming (no spaces/colons)
    batch_id = _utc_now_iso().replace(":", "-")
    
    try:
        uploaded_tables = []

        for table in tables_to_import:
            column_names, new_rows = extract_new_rows(table, last_checked, db_conn)
            if new_rows:
                convert_new_rows_to_df_and_upload_to_s3_as_csv(
                    ingestion_bucket=ingestion_bucket,
                    table=table,
                    column_names=column_names,
                    new_rows=new_rows,
                    batch_id=batch_id
                )
                uploaded_tables.append(table)
            else:
                logger.info(f"No new rows for table={table}")

        # Only update last_checked if the run succeeded
        new_last_checked = _utc_now_iso()
        update_last_checked(ssm_client, new_last_checked)
        logger.info(f"Updated last_checked to: {new_last_checked}")

        return {
            "message": "success",
            "timestamp_to_transform": last_checked,
            "batch_id": batch_id,
            "uploaded_tables": uploaded_tables
        }

    finally:
        try:
            db_conn.close()
            logger.info("Database connection closed")
        except Exception:
            # Don't fail the function just because close failed
            logger.warning("Failed to close DB connection cleanly")

##################################################################################
# Useful functions for the Lambda Handler
##################################################################################

DEFAULT_LAST_CHECKED = "1970-01-01T00:00:00+00:00"  # used on first ever run

def _utc_now_iso() -> str:
    """Return current time in UTC as string."""
    return datetime.now(timezone.utc).isoformat()

def get_last_checked(ssm_client): # test and code complete
    """
    Summary:
    Access the aws parameter store, and obtain the last_checked parameter.
    Store the parameter and its value in a dictionary and return it.
    If it does not exist (first run), return a default old timestamp.
    Returns:
        dictionary of paramter name and value
        {"last_checked" : "2020...."}
    """
    
    try:
        response = ssm_client.get_parameter(
        Name='last_checked',
        WithDecryption=True
        )
        result = {"last_checked": response['Parameter']['Value']}
        return result

    except ssm_client.exceptions.ParameterNotFound as par_not_found_error:
        logger.info("SSM parameter 'last_checked' not found. Using default start time.")
        return {"last_checked": DEFAULT_LAST_CHECKED}

    except ClientError as error:
        logger.error(f"get_last_checked: There has been an AWS ClientError: {str(error)}")
        raise error
    
    
   
def get_db_credentials(sm_client): # test and code complete
    """_summary_
    This function should return a dictionary of all 
    the db credentials obtained from secret manager
    
    Returns:
    dictionary of credentials
    {"user":"totesys", "password":".......}

    """
    secret_name = os.environ.get("DB_SECRET_NAME")
    if not secret_name:
        raise ValueError("Missing environment variable DB_SECRET_NAME")


    try:
        response = sm_client.get_secret_value(SecretId = secret_name)
        return json.loads(response["SecretString"])
    

    except sm_client.exceptions.ResourceNotFoundException as par_not_found_error:
        logger.error(f"get_last_checked: The parameter was not found: {str(par_not_found_error)}")
        raise par_not_found_error
    except ClientError as error:
        logger.error(f"get_last_checked: There has been an error: {str(error)}")
        raise error


def create_db_connection(db_credentials): #test and code complete
    """ Summary:
    Connect to the totesys database using credentials fetched from 
    AWS Parameter Store (a separate function employed for this purpose).
    Uses Connection module from pg8000.native library 
    (from pg8000.native import Connection)

    Return Connection
    """
    try:
        return Connection(
            user = db_credentials["user"],
            password = db_credentials["password"],
            database = db_credentials["database"],
            host = db_credentials["host"],
            port = db_credentials["port"]
        )
        
    except KeyError as e:
        raise KeyError(f"Missing key in db_credentials secret JSON: {e}") from e
    except InterfaceError as interface_error:
        logger.error(f"create_db_connection: cannot connect to database: {interface_error}")
        raise interface_error
    except Exception as error:
        logger.error(f"create_db_connection: there has been an error: {error}")
        raise error
    

def extract_new_rows(table_name, last_checked, db_connection): 
    """ 
    Summary :
        Use connection object to query for rows in a given table where 
        the last_updated is after our last_checked variable.
        
        returns a tuple of column names and new rows.
    
    Args:
    
        table_name (str):
        name of the table to query for
        
        last_checked (datetime object): 
        should be the datetime object store in parameter store
        
        db_connection (object):
        a connection object to the totesys database
    
    
    Returns:
        - extract new data from updated tables using SQL query.
        - the data will be a list of lists. 
        
        returns a tuple of (column_names, new_rows):
    """
    
    last_checked_dt_obj = datetime.fromisoformat(last_checked)

    if table_name in ["department"]:
        query = f"""
    SELECT * FROM {identifier(table_name)}
    """
    else:
        query = f"""
    SELECT * FROM {identifier(table_name)} WHERE last_updated > {literal(last_checked_dt_obj)}
    """
           
    try:
        new_rows = db_connection.run(query)
        column_names = [column['name'] for column in db_connection.columns]
        return column_names, new_rows
    except DatabaseError as db_error:
        logger.error(f"There has been a database error: {str(db_error)}")
    except Exception as error:
        logger.error(f"There has been an error: {str(error)}")

    


def convert_new_rows_to_df_and_upload_to_s3_as_csv(ingestion_bucket, table, column_names, new_rows,batch_id: str):
    """
    Summary:
    This function will take the column names and new row data, 
    and create a pandas dataframe from this.
    From here, this dataframe is uploaded directly to given s3 bucket as
    a csv file.
    

    Args:
        ingestion_bucket (str): name of the ingestion bucket
        table (str): name of the table with the new data
        column_names (list): list of column names
        new_rows (list): nested list of new row values
        
        
    returns:
        sucess message through a log
    """
    
    #convert new rows to a dataframe
    df = pd.DataFrame(new_rows,columns=column_names)
    logger.info(f"dataframe for {table} has been created")

    s3_path = f"s3://{ingestion_bucket}/{table}/{batch_id}.csv"
    #convert dataframe to a csv file
    try:
        wr.s3.to_csv(df=df, path=s3_path, index=False)
        logger.info(f"{table} has been saved to {s3_path}")
    except Exception as error:
        logger.error(f"convert_new_rows_to_df_and_upload_to_s3_as_csv: There has been a dataframe error: {str(error)}")
        raise error

    
def update_last_checked(ssm_client, new_value: str) -> str:
    """
    Summary:
    Initialise ssm_client using boto3.client("ssm")
    Use AWS parameter store to access/update the 'last_checked' parameter
    Use .put_parameter method to update (using Overwrite=TRUE) 
    the last_checked time each time extract_lambda_handler is run.
            
    """

    try:
        ssm_client.put_parameter(
            Name = "last_checked",
            Value = new_value,
            Description="Timestamp of last successful extract Lambda execution",
            Type="String",
            Overwrite=True
        )
        return new_value
    except ClientError as error:
        logger.error(f"update_last_checked: AWS ClientError: {str(error)}")
        raise




    
def get_bucket_name(): #completed with tests
    """
    Summary : this function should obtain the ingestion bucket name from the
    environment variables and return it.
    
    """

    bucket_name = os.environ.get("S3_INGESTION_BUCKET")
    if not bucket_name:
        raise ValueError("Missing environment variable S3_INGESTION_BUCKET")
    return bucket_name

    