import os
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
from boto3 import client, session, resource
from botocore.exceptions import ClientError
from datetime import datetime
import json
import csv


def get_source_df(glueContext, source_table_path):
    dyf = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [source_table_path]},
        format="parquet",
        format_options={'withHeader': True}
    )
    df = dyf.toDF()
    return df


def get_csv_mapping(mapping_path):
    csv_data = open(mapping_path)
    reader = csv.DictReader(csv_data)
    table_mapping = [row for row in reader]
    return table_mapping

# added for testing
def get_null_df(spark):
    # Define the columns
    columns = [
        "chl_id",
        "date",
        "fat",
        "qa_check",
        "quantity",
        "route_id",
        "snf"
    ]

    # Create a Row with null values
    null_row_data = [None, None, None, None, None, None, None]
    null_row = Row(*null_row_data)

    # Define the schema
    schema = StructType([
        StructField("chl_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("fat", FloatType(), True),
        StructField("qa_check", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("route_id", StringType(), True),
        StructField("snf", FloatType(), True)
    ])

    # Create a DataFrame with the null_row and schema
    null_row_df = spark.createDataFrame([null_row], schema)
    return null_row_df


def generate_ruleset_string(rule_list):
    ruleset_string = "Rules = ["
    for index, rule in enumerate(rule_list):
        # to make sure the its aligned with dq rule standards
        rule = rule.replace("'", '"')
        if index == len(rule_list) - 1:
            # If it's the last item, don't add a comma
            ruleset_string += f"{rule}"
        else:
            ruleset_string += f"{rule},"
    ruleset_string += "]"
    return ruleset_string


def empty_s3_path(bucket_name, prefix):
    s3 = resource('s3')
    bucket = s3.Bucket(bucket_name)

    try:
        bucket.objects.filter(Prefix=prefix).delete()
        print("Successfully emptied data in S3 path:", prefix)
    except Exception as e:
        print("Error occurred while emptying S3 path:", str(e))


def save_failed_dq_records(df, dq_failed_bucket, table_name, run_id):
    df.write.json("s3://{}/{}/{}".format(dq_failed_bucket,
                  table_name, run_id), mode="overwrite")


def test():
    print("Hello from helper function...")
