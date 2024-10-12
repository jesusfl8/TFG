import json
import boto3
import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'spotydb-expot'
    
    # List objects in the S3 bucket
    objects = s3.list_objects_v2(Bucket=bucket_name)
    
    # Sort the objects by 'LastModified' timestamp in descending order
    sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)
    
    if sorted_objects:
        # Get the name of the most recent JSON file
        file_name = sorted_objects[0]['Key']
        
        # Get the JSON data from the most recent file
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = json.loads(obj['Body'].read())
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(data),
            'file_name': file_name
        }
    else:
        return {
            'statusCode': 404,
            'body': 'No JSON files found in the S3 bucket'
        }