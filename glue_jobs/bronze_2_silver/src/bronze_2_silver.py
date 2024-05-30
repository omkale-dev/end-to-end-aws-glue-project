import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, to_date, round, substring, length, trim, current_timestamp, date_format
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
import importlib
import boto3
from pyspark.sql.functions import col, coalesce, lit
import helper_functions
importlib.reload(helper_functions)

LOCAL = False
global run_id, mapping_path, load_type
run_id = 1080
load_type = 'full_load'
job_name = "bronze_2_silver"
bronze_bucket = "pf-bronze"
mapping_path = f"./jupyter_workspace/{job_name}/resources/mapping.csv"
delta_bucket = "pf-silver"
database_name = "pf_silver_db"
dq_failed_bucket = "pf-dq-failed"
crawler_name = "pf-silver-crawler"

select_cast = {
    "date": [DateType(),'DATE'],
    "string": [StringType(),'STRING'],
    "integer": [LongType(),'LONG'], #for now 
    "float": [FloatType(),'FLOAT']
}

def glue_init():
    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .getOrCreate()
    sc = spark.sparkContext
    glueContext = GlueContext(sc,region='us-east-1')
    # spark = glueContext.spark_session
    job = Job(glueContext)
    if not LOCAL:
        global run_id, mapping_path, load_type
        args = getResolvedOptions(sys.argv, ["run_id", "load_type"])
        run_id = args["run_id"]
        load_type = args["load_type"]
        print(run_id, load_type)
        mapping_path = "./mapping.csv"

    return spark, glueContext, job
    
def extract_data(glueContext, source_table_path):
    source_df = helper_functions.get_source_df(glueContext, source_table_path)
    return source_df

def clean_data(glueContext, source_df, mapping, table_name):
    select_list = []
    for row in mapping:
        if row['source_table'] == table_name and row['is_generated'] == 'N':
            # 1. dropping duplicates
            source_df = source_df.dropDuplicates()
            # 2. filling null values with default values
            source_df = source_df.fillna({row['source_column']:row['na_default']})
            # 3. ensuring the correct datatypes
            if row['cast_to'] == 'date':
                select_list.append(to_date(col(row['source_column']),"yyyy-MM-dd").alias(row['source_column']))
            # 4. ensuring data standards
            elif row['cast_to'] == 'float':
                select_list.append(round(col(row['source_column']).cast(select_cast[row['cast_to']][0]),2).alias(row['source_column']))
            else:
                select_list.append(col(row['source_column']).cast(select_cast[row['cast_to']][0]).alias(row['source_column']))  
            
    # 5. selecting only required data      
    cleaned_df = source_df.select(select_list)
    return cleaned_df

def transform_data(glueContext, cleaned_df, mapping, table_name):
    select_list = []
    for row in mapping:
        if row['source_table'] == table_name:
            # 1. predefined transformations
            if row['transformation'] == "PHONE":
                select_list.append(substring(row['target_column'],-10,10).alias(row['target_column']))
            # 2. expression based transformations
            elif row['transformation'] == 'EXPR':
                select_list.append(expr(f"CAST({row['expr']} AS {select_cast[row['cast_to']][1]}) AS {row['target_column']}")) # eval expr, then cast, then alias
            # 3. the custom transformation would have gone here...
            elif row['transformation'] == "CUSTOM":
                pass
            else:
                # 4. mapping source with target column
                select_list.append(col(row['source_column']).alias(row['target_column']))
    # 5. add last update timestamp
    select_list.append(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("last_updated"))
    # 6. add etl run_id
    select_list.append(lit(run_id).alias('etl_run_id'))
    transformed_df = cleaned_df.select(select_list)
    return transformed_df

def check_dq(glueContext, transformed_df, mapping, table_name):
    from awsgluedq.transforms import EvaluateDataQuality
    rule_list = []
    for row in mapping:
        if row['source_table'] == table_name:
            if row['dq_rule'] != "NA":
                rule_list.append(row['dq_rule'])
    if len(rule_list)>0: # if there are no dq rules then skip the process
        EvaluateDataQuality_ruleset = helper_functions.generate_ruleset_string(rule_list) # the format: EvaluateDataQuality_ruleset="""Rules = [ColumnValues "gender" in ["f","m"],ColumnValues "cattle_capacity" >= 2]"""
        print(EvaluateDataQuality_ruleset)
        
        dyf = DynamicFrame.fromDF(transformed_df,glueContext,"dyf")
        
        # applying dq check wrt ruleset
        EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
            frame=dyf,
            ruleset=EvaluateDataQuality_ruleset,
            publishing_options={
                "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": False,
            },
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
        )
        
        # result of dq
        ruleOutcomes = SelectFromCollection.apply(
        dfc=EvaluateDataQualityMultiframe,
        key="ruleOutcomes",
        transformation_ctx="ruleOutcomes",
        )
        ruleOutcomes.toDF().show(truncate=False)
        
        # to get df out of dyf
        rowLevelOutcomes = SelectFromCollection.apply(
        dfc=EvaluateDataQualityMultiframe,
        key="rowLevelOutcomes",
        transformation_ctx="rowLevelOutcomes",
        )
        
        # the passed and failed records
        rowLevelOutcomes_df = rowLevelOutcomes.toDF() # Convert Glue DynamicFrame to SparkSQL DataFrame
        rowLevelOutcomes_df_passed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Passed") # Filter only the Passed records.
        rowLevelOutcomes_df_failed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Failed") # Review the Failed records
       
        # dropping the dq related columns 
        columns_to_drop = ['DataQualityRulesPass', 'DataQualityRulesFail', 'DataQualityRulesSkip', 'DataQualityEvaluationResult'] # we dont need these dq results
        # Selecting all columns except the specified ones
        selected_columns = [col for col in rowLevelOutcomes_df_passed.columns if col not in columns_to_drop]
        # Creating the DataFrame with selected columns
        rowLevelOutcomes_df_passed = rowLevelOutcomes_df_passed.select(selected_columns)
        
        # rowLevelOutcomes_df_passed.show(5)
        # rowLevelOutcomes_df_failed.show(5)
        helper_functions.save_failed_dq_records(rowLevelOutcomes_df_failed, dq_failed_bucket, table_name,run_id) # saving the data to s3
        return rowLevelOutcomes_df_passed
    else:
        return transformed_df
    

# used to create update and insert set
def delta_set(table_name,mapping):
    whenNotMatchedInsert_set = {}
    whenMatchedUpdate_set = {}
    for row in mapping:
        if row['source_table'] == table_name:
            prev_col_name = f"prev_df.{row['target_column']}"
            append_col_name = f"append_df.{row['target_column']}"
            if row['target_column'] != 'dwid':  # we dont want to update the dwid
                whenMatchedUpdate_set[prev_col_name] = col(append_col_name)
            whenNotMatchedInsert_set[prev_col_name] = col(append_col_name)
    # adding the etl_run_id and last_updated 
    whenMatchedUpdate_set["prev_df.etl_run_id"] = col("append_df.etl_run_id")
    whenMatchedUpdate_set["prev_df.last_updated"] = col("append_df.last_updated")
    whenNotMatchedInsert_set["prev_df.last_updated"] = col("append_df.last_updated")
    whenNotMatchedInsert_set["prev_df.etl_run_id"] = col("append_df.etl_run_id")
    return whenNotMatchedInsert_set, whenMatchedUpdate_set

def load(table_name,transformed_df,whenNotMatchedInsert_set, whenMatchedUpdate_set):
    global run_crawler
    path = f"s3a://{delta_bucket}/{table_name}/"
    if load_type == 'full_load':
        additional_options = {
            "path": path
        }
        helper_functions.empty_s3_path(delta_bucket,table_name) # to hard overwrite, simply adding overwrite will fail if some table metadata is not found
        # transformed_df.write \
        #     .format("delta") \
        #     .options(**additional_options) \
        #     .mode("overwrite") \
        #     .saveAsTable(f"`{database_name}`.{table_name}")
        # Note:As of 05/24 theres a known issue with writing directly to table using saveAsTable https://github.com/datahub-project/datahub/pull/10299
        # So we will have to use crawler.
        transformed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(path)

        # Start the crawler
        run_crawler = True
        print(f"full_load for {table_name} was successfull")
    else:
        delta_df = DeltaTable.forPath(spark,  f"s3://{delta_bucket}" + f"/{table_name}/")
        delta_df.alias("prev_df").merge(
        source=transformed_df.alias("append_df"), \
        # matching on dwid
        condition=expr("prev_df.dwid = append_df.dwid"))\
        .whenMatchedUpdate(
        set=whenMatchedUpdate_set
        )\
        .whenNotMatchedInsert(
        values=whenNotMatchedInsert_set
        ).execute()
        print(f"SCD1 load for {table_name} was successfull")
        
def send_event_to_eventbridge(run_id,status,process_update):
    # Create an EventBridge client
    client = boto3.client('events')
    if status=='failed':
        message=f'An error was occured while transfering files from bronze to silver. Error:{process_update}'
   
    else:
        message= f'The data transfer from bronze to silver was successfull:{process_update}'
    detail = {
        'run_id': str(run_id),
        'producer':'bronze-2-silver',
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
    
def process_tables(glueContext,mapping ,table_name):
    # for each table path in mapping
    source_table_path = f"s3://{bronze_bucket}/{run_id}/{table_name}"
    
    source_df = extract_data(glueContext, source_table_path)
    # source_df = source_df.union(helper_functions.get_null_df(spark)) # for testing
    # print(f"{table_name}:source")
    # source_df.show()
    # source_df.printSchema()
    
    cleaned_df = clean_data(glueContext, source_df, mapping, table_name)
    # cleaned_df.printSchema()
    # print(f"{table_name}:cleaned")
    # cleaned_df.show()
    
    transformed_df = transform_data(glueContext, cleaned_df, mapping, table_name)
    # print(f"{table_name}:transformed")
    # transformed_df.show()
    
    whenNotMatchedInsert_set, whenMatchedUpdate_set = delta_set(table_name,mapping)
    
    if not LOCAL:  # as the dq will not work locally
        dq_passed_df = check_dq(glueContext, transformed_df, mapping, table_name)
    else:
        dq_passed_df = transformed_df
    
    load(table_name,dq_passed_df,whenNotMatchedInsert_set, whenMatchedUpdate_set)
    
# Main
if __name__ == "__main__":
    error = False
    error_msg =" "
    spark, glueContext, job = glue_init()
    
    mapping = helper_functions.get_csv_mapping(mapping_path)
    tables = set()
    for row in mapping:
        tables.add(row['source_table'])
    for index,table in enumerate(tables):
        # if table == 'aggregated_procurement': # for test used: aggregated_procurement
        # if table == 'collection_center_procurement':
            try:
                process_tables(glueContext,mapping,table)
                if index == len(tables)-1:
                    client = boto3.client('glue')
                    print(f"Starting the crawler: {crawler_name}")
                    response = client.start_crawler(Name=crawler_name)
                    print(response)
        
            except Exception as E:
                print(E)
                exception_details = E.args[0] if E.args else "Unknown error"
                error_msg += f"There was issue while processing table:{table}\nERROR: {exception_details}\n"
                print(error_msg)
                error = True
    if error:
        send_event_to_eventbridge(run_id,'partial',error_msg)
    else:
        send_event_to_eventbridge(run_id,'success','ok')
    
