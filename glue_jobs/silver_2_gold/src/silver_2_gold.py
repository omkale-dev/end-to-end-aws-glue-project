import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
import importlib
import boto3
from datetime import datetime
import helper_functions
importlib.reload(helper_functions)


delta_bucket = "pf-gold"
database_name = "pf_gold_db"
crawler_name = "pf-gold-crawler"

def glue_init():
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .getOrCreate()
    sc = spark.sparkContext
    glueContext = GlueContext(sc, region='us-east-1')
    # spark = glueContext.spark_session
    job = Job(glueContext)
    return spark, glueContext, job


def custom_quarter(month):
    if month in [3, 4, 5]:
        return 1
    elif month in [6, 7, 8]:
        return 2
    elif month in [9, 10, 11]:
        return 3
    elif month in [12, 1, 2]:
        return 4
    else:
        return None


quarter_udf = udf(custom_quarter, IntegerType())


def get_date_dim(start_date, end_date):
    # Convert date strings to datetime objects
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    # Validate input dates
    if start_date_obj >= end_date_obj:
        raise ValueError("End date must be after start date.")

    # Calculate the difference in days
    delta = end_date_obj - start_date_obj
    total_days = delta.days
    date_range = spark.range(0, total_days + 1).selectExpr(
        f"date_add(to_date('{start_date}'), CAST(id AS INT)) as date")
    # Extract date attributes
    date_dimension = date_range \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date"))) \
        .withColumn("day_of_week", dayofweek(col("date"))) \
        .withColumn("week_of_year", weekofyear(col("date")))\
        .withColumn("quarter", quarter_udf(col("month")))
    return date_dimension


def load(df):
    path = f"s3a://{delta_bucket}/dim_date/"
    additional_options = {
        "path": path
    }
    # to hard overwrite, simply adding overwrite will fail if some table metadata is not found
    helper_functions.empty_s3_path(delta_bucket, "dim_date")
    # df.write \
    #     .format("delta") \
    #     .options(**additional_options) \
    #     .mode("overwrite") \
    #     .saveAsTable(f"`{database_name}`.dim_date")
    # Note:As of 05/24 theres a known issue with writing directly to table using saveAsTable https://github.com/datahub-project/datahub/pull/10299
    # So we will have to use crawler.
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path)
    df.show()
    # Start the crawler
    client = boto3.client('glue')
    print(f"Starting the crawler: {crawler_name}")
    response = client.start_crawler(Name=crawler_name)
    print(response)
    print(f"dim_date was loaded successfully")


def send_event_to_eventbridge(run_id, status, process_update):
    # Create an EventBridge client
    client = boto3.client('events')
    if status == 'failed':
        message = f'An error was occured while processing dim date. Error:{process_update}'

    else:
        message = f'Processing dim date was successfull:{process_update}'
    detail = {
        'run_id': run_id,
        'producer': 'silver-2-gold',
        'status': status,
        'message': message,

    }
    # Define the event
    event = {
        "Source": "pf-resources",
        "DetailType": "pf-resource-event",
        "Detail": json.dumps(detail),
    }
    # Send the event
    response = client.put_events(Entries=[event])


def process_dim_date(spark, start_date, end_date):
    df = get_date_dim(start_date, end_date)
    load(df)


# Main
if __name__ == "__main__":
    error = False
    error_msg = " "
    spark, glueContext, job = glue_init()

    start_date = "2022-01-01"
    end_date = "2025-12-31"

    try:
        process_dim_date(spark, start_date, end_date)
    except Exception as E:
        exception_details = E.args[0] if E.args else "Unknown error"
        print(E)
        error_msg += f"There was issue while processing dim date \nERROR: {exception_details}\n"
        print(error_msg)
        error = True
    if error:
        send_event_to_eventbridge(0, 'failed', error_msg)
    else:
        send_event_to_eventbridge(0, 'success', 'ok')





