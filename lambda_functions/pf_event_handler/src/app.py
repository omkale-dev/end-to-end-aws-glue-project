import json
import boto3

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    event_data = event['detail']
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('pf-pipeline-logs')
    run_id=int(event_data['run_id'])
    producer=event_data['producer']
    task_run_id= str(run_id)+"_"+producer # as in some places the run id is sent through json string, its type will mismatch with dynamodb pk
    data = {
        "task_run_id":task_run_id,
        "body":event_data
    }
    response = table.put_item(Item=data)
    return {
        'statusCode': 200,
        'body': json.dumps('Event processed successfully')
    }