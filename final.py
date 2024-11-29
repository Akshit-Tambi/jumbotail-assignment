import boto3
import json
import time
from decimal import Decimal
import datetime
from tzlocal import get_localzone
import threading

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            return Decimal(str(obj))  # Convert float to Decimal
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

def tzlocal():
    local_timezone = get_localzone()
    return local_timezone


def upload_to_s3(record, bucket_name, prefix):
    """Upload record to specified S3 bucket"""
    try:
        s3_client = boto3.client('s3')
        record_json = json.dumps(record, indent=2, cls=DecimalEncoder)
        s3_key = f"{prefix}/{record['eventID']}.json"
        
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=record_json,
            ContentType='application/json'
        )
        print(f"Successfully uploaded to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def process_stream(dynamodb_client, table, bucket_name, prefix):
    """Process DynamoDB stream records for a specific table"""
    print(f"Processing stream for {table.name}")
    
    # Get the stream ARN
    stream_arn = table.latest_stream_arn
    
    # Describe the stream to get shard information
    stream_description = dynamodb_client.describe_stream(
        StreamArn=stream_arn
    )
    
    # Get the first shard
    shard_id = stream_description['StreamDescription']['Shards'][0]['ShardId']
    
    # Get shard iterator
    shard_iterator = dynamodb_client.get_shard_iterator(
        StreamArn=stream_arn,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
    )
    
    iterator = shard_iterator['ShardIterator']
    print(f"\nWatching stream for {table.name} records...")
    
    while True:  # Continuous processing
        try:
            response = dynamodb_client.get_records(ShardIterator=iterator, Limit=10)
            print(response)
            
            if response['Records']:
                for record in response['Records']:
                    print(f"\n{table.name} Stream Record:")
                    print(f'\n{record}\n\n')
                    upload_to_s3(record, bucket_name, prefix)
            
            # Update iterator
            iterator = response['NextShardIterator']
            time.sleep(15)
        
        except Exception as e:
            print(f"Error processing stream for {table.name}: {e}")
            time.sleep(30)  # Wait before retrying

def main():
    # Initialize AWS clients
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodbstreams')
    
    # Define tables and their corresponding S3 bucket details
    tables_config = [
        {
            'table_name': 'Customer',
            'bucket_name': 'quarkstail-datalake-s3-bucket',
            'prefix': 'customers'
        },
        {
            'table_name': 'Order',
            'bucket_name': 'quarkstail-datalake-s3-bucket',
            'prefix': 'orders'
        }
    ]
    
    # Create and start threads for each table's stream processing
    threads = []
    for config in tables_config:
        table = dynamodb.Table(config['table_name'])
        thread = threading.Thread(
            target=process_stream, 
            args=(
                dynamodb_client, 
                table, 
                config['bucket_name'], 
                config['prefix']
            )
        )
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete (which they won't unless an error occurs)
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main()