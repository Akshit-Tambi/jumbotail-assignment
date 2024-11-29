import boto3
import json
import time
from decimal import Decimal
import datetime
from tzlocal import get_localzone

# Custom JSON encoder for Decimal and datetime serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Convert datetime to string
        return super(DecimalEncoder, self).default(obj)


def tzlocal():
    local_timezone = get_localzone()
    return local_timezone
'''
def create_table(dynamodb, table_name):
    
    # Create the table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'CustomerID',  # Partition key
                'KeyType': 'HASH'  # Partition key (HASH)
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'CustomerID',  # Partition key attribute
                'AttributeType': 'S'  # String data type
            }
        ],
        BillingMode='PAY_PER_REQUEST',  # On-demand billing mode
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_AND_OLD_IMAGES'  # Stream includes both new and old images
        }
    )

    # Wait until the table is created
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print the table status
    print(f"Table '{table_name}' created successfully!")
    print(f"Table status: {table.table_status}")
    return table

def get_table_properties(dynamodb,table_name):
    """Get dynamodb table properties"""
    # Get the table details
    table = dynamodb.Table(table_name)
    return table

def insert_sample_item(table,sample_no):
    """Insert a sample item into the table"""
    customer_data = [
                    {
                        "CustomerID": "CUST67890",
                        "Name": "Jane Doe",
                        "Email": "jane.doe@example.com",
                        "Phone": "+1234567890",
                        "Address": {
                            "Street": "123 Main St",
                            "City": "Springfield",
                            "State": "IL",
                            "ZIP": "62704"
                        },
                        "IsPrimeMember": True
                    },
                    {
                        "CustomerID": "CUST67891",
                        "Name": "John Smith",
                        "Email": "john.smith@example.com",
                        "Phone": "+1987654321",
                        "Address": {
                            "Street": "456 Oak Ave",
                            "City": "Greenwood",
                            "State": "IN",
                            "ZIP": "46142"
                        },
                        "IsPrimeMember": False
                    },
                    {
                        "CustomerID": "CUST67892",
                        "Name": "Alice Johnson",
                        "Email": "alice.johnson@example.com",
                        "Phone": "+1122334455",
                        "Address": {
                            "Street": "789 Pine Rd",
                            "City": "Dallas",
                            "State": "TX",
                            "ZIP": "75201"
                        },
                        "IsPrimeMember": True
                    },
                    {
                        "CustomerID": "CUST67893",
                        "Name": "Bob Brown",
                        "Email": "bob.brown@example.com",
                        "Phone": "+1222333444",
                        "Address": {
                            "Street": "101 Maple Dr",
                            "City": "Phoenix",
                            "State": "AZ",
                            "ZIP": "85001"
                        },
                        "IsPrimeMember": False
                    },
                    {
                        "CustomerID": "CUST67894",
                        "Name": "Eve White",
                        "Email": "eve.white@example.com",
                        "Phone": "+1333444555",
                        "Address": {
                            "Street": "202 Birch Ln",
                            "City": "Austin",
                            "State": "TX",
                            "ZIP": "73301"
                        },
                        "IsPrimeMember": True
                    }
                ]
    item = customer_data[sample_no]
    
    table.put_item(Item=item)
    print("Sample item inserted:")
    print(json.dumps(item, indent=2, cls=DecimalEncoder))
    
'''
    
# Initialize the S3 client
s3_client = boto3.client('s3')

# Your S3 bucket name
bucket_name = 'quarkstail-datalake-s3-bucket'  
def upload_to_s3(record):
        # Upload the JSON string to S3 as an object
    try:
        record_json = json.dumps(record, indent=2, cls=DecimalEncoder)
        s3_key = f"customers/{record['eventID']}.json"
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=record_json,
            ContentType='application/json'
        )
        print(f"Successfully uploaded to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")





def process_stream(dynamodb_client, table):
    """Process DynamoDB stream records"""
    print(table)
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
    
    # Print stream records
    iterator = shard_iterator['ShardIterator']
    print("\nWatching stream for records...")
    
    for i in range(5):  # Check for 5 iterations
        response = dynamodb_client.get_records(ShardIterator=iterator, Limit=10)
        print(response)
        if response['Records']:
            for record in response['Records']:
                print("\nStream Record:")
                print(f'\n{record}\n\n')
                upload_to_s3(record)
        
        # Update iterator
        iterator = response['NextShardIterator']
        time.sleep(15)

def main():
    
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodbstreams')
    # Create table with streams
    table_name = 'Customertable'
    table = dynamodb.Table(table_name)
    #table = get_table_properties(dynamodb, table_name)
    # Process stream records
    process_stream(dynamodb_client, table)
    
    

if __name__ == '__main__':
    main()