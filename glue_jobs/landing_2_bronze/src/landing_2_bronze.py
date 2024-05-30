import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import boto3
import json
import importlib
import helper_functions
importlib.reload(helper_functions)

# Globals
LOCAL = False
global run_id
run_id = 1
bronze_bucket = "pf-bronze"
landing_bucket = "pf-landing"

def glue_init():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    if not LOCAL:
        global run_id
        args = getResolvedOptions(sys.argv, ["run_id"])
        run_id = args["run_id"]
    return spark, glueContext, job
    
def send_event_to_eventbridge(run_id,status,process_update):
    # Create an EventBridge client
    client = boto3.client('events')
    if status=='failed':
        message= f'An error was occured while transfering files from landing to bronze. Error:{process_update}'
   
    else:
        message= f'The data transfer from landing to bronze was successfull:{process_update}'
    detail = {
        'run_id': str(run_id),
        'producer':'landing-2-bronze',
        'status':status,
        'message':message,
      
    }
    # Define the event
    event = {
        "Source": "pf-resources",
        "DetailType": "pf-resource-event",
        "Detail": json.dumps(detail),
    }
    # Send the event
    response = client.put_events(Entries=[event])

def get_files():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=landing_bucket)
    json_files = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.json'):
            json_files.append(key)
    return json_files

def extract_table_name(file_path):
    parts = file_path.split('/')
    file_name_with_extension = parts[-1]
    name_parts = file_name_with_extension.split('.')
    table_name = name_parts[0]
    return table_name
    
def convert_to_parquet(spark,files):
    for file in files:
        table_name = extract_table_name(file)
        df = spark.read.json(f"s3://{landing_bucket}/{file}")
        bronze_file_path = f"s3a://{bronze_bucket}/{run_id}/{table_name}"
        df.write.mode('overwrite').parquet(bronze_file_path)
        print("Files were converted and loaded successfully.")


if __name__=="__main__":
    helper_functions.test()
    error = False
    spark, glueContext, job = glue_init()
    try:
        convert_to_parquet(spark,get_files())
    except Exception as e:
        send_event_to_eventbridge(run_id,'failed',e)
        error = True
    if not error:
        send_event_to_eventbridge(run_id,'success','ok')
    spark.stop()





