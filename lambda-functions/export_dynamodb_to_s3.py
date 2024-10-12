import json
import boto3
import datetime

def lambda_handler(event, context):
    # Nombre de la tabla de DynamoDB
    dynamodb_table_name = "spotify_data"
    
    # Nombre del bucket de S3
    s3_bucket_name = "spotydb_expot"
    
    # Nombre del archivo de exportación (puedes incluir la fecha en el nombre)
    export_file_name = f"export-{datetime.datetime.now().isoformat()}.json"
    
    # Configuración de los clientes de DynamoDB y S3
    dynamodb = boto3.client('dynamodb')
    s3 = boto3.client('s3')
    
    # Escanea la tabla DynamoDB para obtener todos los elementos
    response = dynamodb.scan(TableName=dynamodb_table_name)
    
    # Convierte los datos de DynamoDB a formato JSON
    data = json.dumps(response['Items'], default=str)
    
    # Sube los datos exportados al bucket S3
    s3.put_object(Bucket=s3_bucket_name, Key=export_file_name, Body=data)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Exportacion exitosa')
    }
