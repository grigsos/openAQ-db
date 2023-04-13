import boto3
from typing import Any, Dict

from utils import get_parameters, replace_none_num, replace_none_str
from constants import TABLE_NAME

dynamodb_client: boto3.client = boto3.client('dynamodb')

def put_data_to_dynamodb(response: Dict[str, Any]) -> Dict[str, Any]:
    write_requests = []
    for i, result in enumerate(response['results']):
        parameters: Dict[str, Any] = get_parameters(result)
        if len(parameters) == 0:
            key = {'locationID': {'N': str(replace_none_num(result['id']))}}
            write_requests.append({'DeleteRequest': {'Key': key}})
        else:
            latitude: float = replace_none_num(result['coordinates']['latitude']) if result['coordinates'] is not None else 100
            longitude: float = replace_none_num(result['coordinates']['longitude']) if result['coordinates'] is not None else 100
            item = {
                'locationID': {'N': str(replace_none_num(result['id']))},
                'city': {'S': replace_none_str(result['city'])},
                'name': {'S': replace_none_str(result['name'])},
                'parameters': {'S': str(parameters)},
                'latitude': {'N': str(latitude)},
                'longitude': {'N': str(longitude)},
            }
            write_requests.append({'PutRequest': {'Item': item}})
            
        # Batch write items in chunks of 25 or less
        if i % 25 == 24 or i == len(response['results'])-1:
            try:
                dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: write_requests})
            except boto3.exceptions.Boto3Error as e:
                error_message : str = "Error occurred while processing adding data to table: {}".format(str(e))
                return {
                    "statusCode": 500,
                    "body": {
                        "message": error_message
                    }
                }
            write_requests = []

    return {
        "statusCode": 200,
        "body": {
            "message": "Data added successfully"
        }
    }
