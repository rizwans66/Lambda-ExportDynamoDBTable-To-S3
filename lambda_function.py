import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime, date

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    
    # Retrieve environment variables
    table_name = os.environ.get('TABLE_NAME')
    s3_bucket = os.environ.get('S3_BUCKET')
    current_date = datetime.now().strftime('%Y-%m-%d')
    s3_prefix = f"{current_date}/"

    # Log the environment variables
    print("TABLE_NAME:", table_name)
    print("S3_BUCKET:", s3_bucket)

    # Validate environment variables
    if not table_name or not s3_bucket:
        return {
            'statusCode': 400,
            'body': json.dumps('TABLE_NAME and S3_BUCKET environment variables are required')
        }

    try:
        response = dynamodb.export_table_to_point_in_time(
            TableArn=f'arn:aws:dynamodb:{context.invoked_function_arn.split(":")[3]}:{context.invoked_function_arn.split(":")[4]}:table/{table_name}',
            ExportFormat='DYNAMODB_JSON',
            S3Bucket=s3_bucket,
            S3Prefix=s3_prefix
        )

        return {
            'statusCode': 200,
            'body': json.dumps(response, cls=CustomJSONEncoder)
        }
    except ClientError as e:
        print("Error exporting table:", e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error exporting table: {e}")
        }
