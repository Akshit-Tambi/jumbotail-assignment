import boto3
import json
import time
from decimal import Decimal
import datetime
from tzlocal import get_localzone

# Custom JSON encoder for Decimal and datetime serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            return Decimal(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Convert datetime to string
        return super(DecimalEncoder, self).default(obj)


def convert_floats_to_decimals(d):
    if isinstance(d, dict):  # If it's a dictionary, process each key-value pair
        return {k: convert_floats_to_decimals(v) for k, v in d.items()}
    elif isinstance(d, list):  # If it's a list, process each element
        return [convert_floats_to_decimals(i) for i in d]
    elif isinstance(d, float):  # If it's a float, convert it to Decimal
        return Decimal(str(d))
    else:
        return d
    
    

def tzlocal():
    local_timezone = get_localzone()
    return local_timezone


def create_table(dynamodb):
    try:
        table1 = dynamodb.create_table(
                TableName='Customer',
                KeySchema=[
                    {'AttributeName': 'CustomerId', 'KeyType': 'HASH'}  
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'CustomerId', 'AttributeType': 'S'} 
                ],
                ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
                },
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
                )
        table1.meta.client.get_waiter('table_exists').wait(TableName='Customer')
    except Exception as e:
        table1 = dynamodb.Table('Customer')

    try:
        table2 = dynamodb.create_table(
                TableName='Order',
                KeySchema=[
                    {'AttributeName': 'OrderId', 'KeyType': 'HASH'},  
                    {'AttributeName': 'CustomerId', 'KeyType': 'RANGE'} 
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'OrderId', 'AttributeType': 'S'}, 
                    {'AttributeName': 'CustomerId', 'AttributeType': 'S'} 
                ],
                ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
                },
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
                )
        table2.meta.client.get_waiter('table_exists').wait(TableName='Order')
    except Exception as e:
        table2 = dynamodb.Table('Order')
        
    print(f"Tables are ready!!")
    return table1,table2




def insert_sample_item(table, data, sample_no):
    """Insert a sample item into the table"""
    item = data[sample_no]
    print(f'\n\n{item}\n\n')
    item = convert_floats_to_decimals(item)
    print(f'\n\n{item}\n\n')
    table.put_item(Item=item)
    print("Sample item inserted:")
    print(item)
    
    
def main():
    
    dynamodb = boto3.resource('dynamodb')
    table1, table2 = create_table(dynamodb)
    time.sleep(7)
    
    
    customer_data = [
                    {
                        "CustomerId": "CUST67895",
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
                        "CustomerId": "CUST67896",
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
                        "CustomerId": "CUST67897",
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
                        "CustomerId": "CUST67898",
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
                        "CustomerId": "CUST67899",
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
    
    
    orders_data = [
                    {
                        "OrderId": "ORD12350",
                        "CustomerId": "CUST67895",
                        "OrderDate": "2024-11-28T14:30:00Z",
                        "OrderTotal": 199.99,
                        "OrderStatus": "Shipped",
                        "Items": [
                            {"ItemID": "ITEM001", "Quantity": 2, "Price": 49.99},
                            {"ItemID": "ITEM002", "Quantity": 1, "Price": 99.99}
                        ]
                    },
                    {
                        "OrderId": "ORD12351",
                        "CustomerId": "CUST67896",
                        "OrderDate": "2024-11-28T15:00:00Z",
                        "OrderTotal": 149.99,
                        "OrderStatus": "Delivered",
                        "Items": [
                            {"ItemID": "ITEM003", "Quantity": 1, "Price": 49.99},
                            {"ItemID": "ITEM004", "Quantity": 2, "Price": 50.00}
                        ]
                    },
                    {
                        "OrderId": "ORD123452",
                        "CustomerId": "CUST67897",
                        "OrderDate": "2024-11-28T16:30:00Z",
                        "OrderTotal": 279.98,
                        "OrderStatus": "Pending",
                        "Items": [
                            {"ItemID": "ITEM005", "Quantity": 4, "Price": 69.99}
                        ]
                    },
                    {
                        "OrderId": "ORD12353",
                        "CustomerId": "CUST67898",
                        "OrderDate": "2024-11-28T17:30:00Z",
                        "OrderTotal": 389.97,
                        "OrderStatus": "Shipped",
                        "Items": [
                            {"ItemID": "ITEM006", "Quantity": 3, "Price": 129.99}
                        ]
                    },
                    {
                        "OrderId": "ORD12354",
                        "CustomerId": "CUST67899",
                        "OrderDate": "2024-11-28T18:00:00Z",
                        "OrderTotal": 499.98,
                        "OrderStatus": "Cancelled",
                        "Items": [
                            {"ItemID": "ITEM007", "Quantity": 2, "Price": 99.99},
                            {"ItemID": "ITEM008", "Quantity": 2, "Price": 100.00}
                        ]
                    }
                ]
    
    i = 0
    while i<=4:
        insert_sample_item(table1,customer_data, i)
        insert_sample_item(table2, orders_data, i)
        
        i+=1

if __name__ == '__main__':
    main()