import boto3
from config.dynamodb_params import dynamodb_table_name



# Set your AWS credentials
aws_access_key_id = 'YOUR_ACCESS_KEY'
aws_secret_access_key = 'YOUR_SECRET_KEY'
region_name = 'YOUR_REGION'

# Set up DynamoDB client
dynamodb_client = boto3.client('dynamodb',
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key,
                               region_name=region_name)

# Function to get the latest checkpoint from DynamoDB
def get_latest_checkpoint(dynamodb_checkpoint_key):
    response = dynamodb_client.get_item(
        TableName=dynamodb_table_name,
        Key={'checkpoint_key': {'S': dynamodb_checkpoint_key}}
    )
    if 'Item' in response:
        return int(response['Item']['checkpoint_offset']['N'])
    else:
        return 0

# Function to save the latest checkpoint to DynamoDB
def save_checkpoint(dynamodb_checkpoint_key, offset):
    dynamodb_client.put_item(
        TableName=dynamodb_table_name,
        Item={
            'checkpoint_key': {'S': dynamodb_checkpoint_key},
            'checkpoint_offset': {'N': str(offset)}
        }
    )
