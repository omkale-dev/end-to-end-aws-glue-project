import requests
import json
import boto3
from botocore.exceptions import ClientError

# Globals
global run_id
run_id=0
dim_url = "https://<your api gateway id>.execute-api.us-east-1.amazonaws.com/dev/get-dims" # change the urls to your apis
fact_url = "https://<your api gateway id>.execute-api.us-east-1.amazonaws.com/dev/get-facts" # change the urls to your apis
bucket_name = 'pf-landing'

def upload_to_s3(prefix,file_name,data):
    s3 = boto3.client('s3')
    file_name = f'{prefix}/{file_name}.json'

    # Upload the JSON data to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=data
    )

def get_data_from_source(url):
    secret = get_secret(run_id)
    secret = json.loads(secret)
    pf_api_key = secret['pf-api-key']
    payload = {}
    headers = {
      'x-api-key': pf_api_key
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.content)['data']
    return data

def upload_dims_data(dims_data):
    farmers_data = dims_data['farmers_data']
    collection_center_data = dims_data['collection_center_data']
    chilling_center_data = dims_data['chilling_center_data']
    logistics_contractor_data = dims_data['logistics_contractor_data']
    routes_data = dims_data['routes_data']
    distributor_data = dims_data['distributor_data']
    products_data = dims_data['products_data']
    recipe_data = dims_data['recipe_data']

    dims_dict = {
        'farmers_data': farmers_data,
        'collection_center_data': collection_center_data,
        'chilling_center_data': chilling_center_data,
        'logistics_contractor_data': logistics_contractor_data,
        'routes_data': routes_data,
        'distributor_data': distributor_data,
        'products_data': products_data,
        'recipe_data': recipe_data
    }

    # Call upload_to_s3 for each dataframe
    for file_name, data in dims_dict.items():
        upload_to_s3('dims',file_name,data)

def upload_facts_data(facts_data):
    collection_center_procurement = facts_data['collection_center_procurement']
    chilling_center_procurement = facts_data['chilling_center_procurement']
    aggregated_procurement = facts_data['aggregated_procurement']
    daily_logistics = facts_data['daily_logistics']
    sales_data = facts_data['sales_data']
    production_data = facts_data['production_data']

    facts_dict = {
        'collection_center_procurement': collection_center_procurement,
        'chilling_center_procurement':chilling_center_procurement,
        'aggregated_procurement':aggregated_procurement,
        'daily_logistics':daily_logistics,
        'sales_data':sales_data,
        'production_data':production_data
    }

    # Call upload_to_s3 for each dataframe
    for file_name, data in facts_dict.items():
        upload_to_s3('facts',file_name,data)

def send_event_to_eventbridge(run_id,status,process_update):
    # Create an EventBridge client
    client = boto3.client('events')
    if status=='failed':
        message= f'An error was occured while ingesting files. Error:{process_update}'
   
    else:
        message= f'The ingestion was successfull:{process_update}'
    detail = {
        'run_id': run_id,
        'producer':'ingest-lambda',
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

def get_secret(run_id):
    secret_name = "pf-creds"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        send_event_to_eventbridge(run_id,e)
        raise e


    return get_secret_value_response['SecretString']

def lambda_handler(event, context):
    date_input = event.get('date_input', None)
    start_date = event.get('start_date',None)
    backfill = event.get('backfill',None)
    run_id = event.get('run_id',None)
    error = False
    try:
      if run_id:
        dims_data = get_data_from_source(dim_url)
        upload_dims_data(dims_data)
        if date_input and start_date and backfill:
            facts_data = get_data_from_source(f"{fact_url}?date-input={date_input}&start-date={start_date}&backfill={backfill}")
            upload_facts_data(facts_data)
        else: 
            raise ValueError("Date input was not provided in the event.")
      else:
          raise ValueError("Run Id was not provided.")
    except Exception as e:
        send_event_to_eventbridge(run_id,'failed',e)
        error = True
    if not error:
        send_event_to_eventbridge(run_id,'success','ok')
      
# event ={
#     'run_id':1234,
#     'date_input':'2024-01-02'
# }
# lambda_handler(event,'c')


