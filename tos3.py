import boto3
import json
import datetime
from tzlocal import get_localzone
from decimal import Decimal

# Custom JSON encoder for Decimal and datetime serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Convert datetime to string
        return super(DecimalEncoder, self).default(obj)

# Initialize the S3 client
s3_client = boto3.client('s3')

# Your S3 bucket name
bucket_name = 'quarkstail-datalake-s3-bucket'

# Define your record (the entry you provided)
def tzlocal():
    local_timezone = get_localzone()
    return local_timezone

record = {
    'eventID': 'a93b42f3a4dc744bf312201a52252af1',
    'eventName': 'INSERT',
    'eventVersion': '1.1',
    'eventSource': 'aws:dynamodb',
    'awsRegion': 'ap-south-1',
    'dynamodb': {
        'ApproximateCreationDateTime': datetime.datetime(2024, 11, 29, 21, 20, 25, tzinfo=tzlocal()),
        'Keys': {'userId': {'S': 'user123'}},
        'NewImage': {
            'age': {'N': '30'},
            'email': {'S': 'john.doe@example.com'},
            'name': {'S': 'John Doe'},
            'userId': {'S': 'user123'}
        },
        'SequenceNumber': '100000000021053503235',
        'SizeBytes': 68,
        'StreamViewType': 'NEW_AND_OLD_IMAGES'
    }
}

# Convert the record to JSON
record_json = json.dumps(record, indent=2, cls=DecimalEncoder)

# Define the S3 object key (filename) where the JSON will be stored
s3_key = 'record.json'  # Change the folder name as required

# Upload the JSON string to S3 as an object
try:
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=record_json,
        ContentType='application/json'
    )
    print(f"Successfully uploaded to {bucket_name}/{s3_key}")
except Exception as e:
    print(f"Error uploading to S3: {e}")
