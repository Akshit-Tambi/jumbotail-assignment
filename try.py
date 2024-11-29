import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Set up the DynamoDB and Stream clients
dynamodb = boto3.client('dynamodb', region_name='ap-south-1')
streams_client = boto3.client('dynamodbstreams', region_name='ap-south-1')

def process_stream_records(stream_arn):
    try:
        # Get the stream details
        stream_response = streams_client.describe_stream(StreamArn=stream_arn)
        print(stream_response)
        shard_id = stream_response['StreamDescription']['Shards'][0]['ShardId']

        # Get the shard iterator
        shard_iterator_response = streams_client.get_shard_iterator(
            StreamArn=stream_arn,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'  # Can also use 'LATEST'
        )
        print(f'\n\n{shard_iterator_response}')
        shard_iterator = shard_iterator_response['ShardIterator']

        # Read and process records from the stream
        while shard_iterator:
            stream_response = streams_client.get_records(ShardIterator=shard_iterator)
            for record in stream_response['Records']:
                print("Stream record:")
                print(record)

                # Optional: If you want to format the records, you can parse them
                if 'dynamodb' in record:
                    keys = record['dynamodb'].get('Keys', {})
                    new_image = record['dynamodb'].get('NewImage', {})
                    print("Keys:", keys)
                    print("New Image:", new_image)

            # Get the next shard iterator
            shard_iterator = stream_response.get('NextShardIterator')
            if not stream_response['Records']:
                print("No new records. Waiting...")
                break

    except NoCredentialsError:
        print("AWS credentials not found.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials configuration.")
    except Exception as e:
        print(f"Error processing stream records: {str(e)}")

# Replace with your table's Stream ARN
your_stream_arn = "arn:aws:dynamodb:ap-south-1:140023404813:table/Customer/stream/2024-11-28T20:32:37.346"

process_stream_records(your_stream_arn)
