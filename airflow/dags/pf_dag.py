from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.models.param import Param
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import json
import boto3
import time

# AWS credentials and region
AWS_CONN_ID = 'aws_conn'  # Connection id to your AWS credentials stored in Airflow
AWS_REGION_NAME = 'us-east-1'
# Replace with your actual ARN
topic_arn = 'arn:aws:sns:us-east-1:<your account id>:pf-comm-channel'

# Define the default arguments for the DAG
default_args = {
    'owner': 'purefreshltd',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 24),

    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False  # Disable backfilling
}

# Define the DAG
dag = DAG(
    'pure_fresh_dag',
    default_args=default_args,
    schedule_interval='@once',
    params={
        "run_id": Param(5, type="integer", minimum=1),
        "input_date": Param(
            default="2023-01-01",
            type="string",
            format="date",
            description="The date for which you want to query data"
        ),
        "start_date": Param(
            default="2023-01-01",
            type="string",
            format="date",
            description="IMP:(Keep it one month from input_date), data from this date till input_date will be queried. Why?(cause api gateway cant return large json data)"
        ),
        "load_type": Param(enum=["full_load", "incremental_load"], default="incremental_load"),
        "backfill": Param(enum=["0", "1"], default="0", description="0 if you dont want bulk data from start_date till input_date.")
    }
)


def process_input_params(**context):
    input_date_obj = datetime.strptime(
        context['dag_run'].conf.get("input_date"), '%Y-%m-%d')
    input_date_formatted = input_date_obj.strftime('%Y-%m-%d')
    start_date_obj = datetime.strptime(
        context['dag_run'].conf.get("start_date"), '%Y-%m-%d')
    start_date_formatted = start_date_obj.strftime('%Y-%m-%d')
    run_id = str(context['dag_run'].conf.get("run_id"))
    date_input = str(input_date_formatted)
    start_date = str(start_date_formatted)
    load_type = str(context['dag_run'].conf.get("load_type"))
    backfill = str(context['dag_run'].conf.get("backfill"))

    # push to xcom
    context['task_instance'].xcom_push(key='run_id', value=run_id)
    context['task_instance'].xcom_push(key='date_input', value=date_input)
    context['task_instance'].xcom_push(key='start_date', value=start_date)
    context['task_instance'].xcom_push(key='load_type', value=load_type)
    context['task_instance'].xcom_push(key='backfill', value=backfill)


def send_notification(body):
    aws_hook = BaseHook.get_connection(AWS_CONN_ID)
    # Retrieve AWS credentials from the connection
    aws_access_key_id = aws_hook.login
    aws_secret_access_key = aws_hook.password
    # Default to us-east-1 if region_name is not specified
    aws_region_name = aws_hook.extra_dejson.get('region_name', 'us-east-1')

    sns = boto3.client('sns',
                       region_name=aws_region_name,
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key)

    message = json.dumps(body)
    response = sns.publish(TopicArn=topic_arn, Message=message)
    print(response)


def check_pipeline_logs(producer, **context):
    aws_hook = BaseHook.get_connection(AWS_CONN_ID)
    # Retrieve AWS credentials from the connection
    aws_access_key_id = aws_hook.login
    aws_secret_access_key = aws_hook.password
    # Default to us-east-1 if region_name is not specified
    aws_region_name = aws_hook.extra_dejson.get('region_name', 'us-east-1')
    time.sleep(10)  # wait for while, the new data to appear in dynamodb

    # Initialize DynamoDB client with credentials
    dynamodb = boto3.resource('dynamodb',
                              region_name=aws_region_name,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    table_name = 'pf-pipeline-logs'
    table = dynamodb.Table(table_name)
    key = {
        'task_run_id': str(context['task_instance'].xcom_pull(task_ids='process_input_params', key='run_id'))+"_"+producer
    }
    response = table.get_item(Key=key)
    item = response.get('Item')
    if item:
        print("Run Id found")
        print(item)
        if producer == "ingest-lambda":
            send_notification(item)
            if item['body']['status'] == "success":
                return True
            else:
                # stop the upstream tasks, report failure
                return False

        elif producer == "landing-2-bronze":
            send_notification(item)
            if item['body']['status'] == "success":
                return True
            else:
                # stop the upstream tasks, report failure
                return False
        elif producer == "bronze-2-silver":
            send_notification(item)
            if item['body']['status'] == "success":
                return True
            else:
                # continue upstream tasks, report table process failure
                # status will be 'partial', the complete failure is not anticipated
                return False
    else:
        print("Run Id not found.")


chk_ingest_lambda_status = ShortCircuitOperator(
    task_id='chk_ingest_lambda_status',
    python_callable=check_pipeline_logs,
    ignore_downstream_trigger_rules=True,
    provide_context=True,
    op_kwargs={'producer': 'ingest-lambda'}
)

chk_l2b_status = ShortCircuitOperator(
    task_id='chk_l2b_status',
    python_callable=check_pipeline_logs,
    provide_context=True,
    op_kwargs={'producer': 'landing-2-bronze'}
)

chk_b2s_lambda_status = ShortCircuitOperator(
    task_id='chk_b2s_lambda_status',
    python_callable=check_pipeline_logs,
    provide_context=True,
    op_kwargs={'producer': 'bronze-2-silver'}
)

process_input_params = PythonOperator(
    task_id="process_input_params",
    python_callable=process_input_params,
    provide_context=True,
    dag=dag
)

# JSON payload to pass to the Lambda function


def get_ingest_payload(context):
    return {
        'run_id': context['task_instance'].xcom_pull(task_ids='process_input_params', key='run_id'),
        'date_input': context['task_instance'].xcom_pull(task_ids='process_input_params', key='date_input'),
        # as theres no backfill, only the current day will be processed
        'start_date': context['task_instance'].xcom_pull(task_ids='process_input_params', key='date_input') if context['task_instance'].xcom_pull(task_ids='process_input_params', key='backfill') == '0' else context['task_instance'].xcom_pull(task_ids='process_input_params', key='start_date'),
        'backfill': context['task_instance'].xcom_pull(task_ids='process_input_params', key='backfill')
    }


class CustomLambdaInvokeFunctionOperator(LambdaInvokeFunctionOperator):
    def __init__(self, payload_function, *args, **kwargs):
        self.payload_function = payload_function
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # Generate the payload using the external function
        payload = self.payload_function(context)
        # Convert the payload to a JSON string if required by Lambda
        self.payload = json.dumps(payload)
        # Call the original execute method to invoke the Lambda function
        return super().execute(context)


trigger_ingest_lambda = CustomLambdaInvokeFunctionOperator(
    task_id='trigger_ingest_lambda',
    aws_conn_id=AWS_CONN_ID,
    region_name=AWS_REGION_NAME,
    function_name='pf_ingest',  # ingest lambda
    payload_function=get_ingest_payload,
    dag=dag
)


def get_args_l2b(context):
    return {"--run_id": context['task_instance'].xcom_pull(task_ids='process_input_params', key='run_id')}


class CustomGlueJobOperator(GlueJobOperator):
    def __init__(self, args_function, *args, **kwargs):
        self.get_args_function = args_function
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.script_args = self.get_args_function(context)
        return super().execute(context)


landing_2_bronze_job = CustomGlueJobOperator(
    task_id="landing_2_bronze_job",
    job_name="test_l2b",
    aws_conn_id=AWS_CONN_ID,
    region_name=AWS_REGION_NAME,
    args_function=get_args_l2b,
    dag=dag
)


def get_args_b2s(context):
    return {
        "--run_id": context['task_instance'].xcom_pull(task_ids='process_input_params', key='run_id'),
        "--load_type": context['task_instance'].xcom_pull(task_ids='process_input_params', key='load_type'),
        "--datalake-formats": "delta"
    }


bronze_2_silver_job = CustomGlueJobOperator(
    task_id="bronze_2_silver_job",
    job_name="test_b2s",
    aws_conn_id=AWS_CONN_ID,
    region_name=AWS_REGION_NAME,
    args_function=get_args_b2s,
    dag=dag
)

process_input_params >> trigger_ingest_lambda >> chk_ingest_lambda_status >> landing_2_bronze_job >> chk_l2b_status >> bronze_2_silver_job >> chk_b2s_lambda_status
