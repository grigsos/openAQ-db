import boto3
import json
from typing import Any, Dict,List
from botocore.exceptions import ClientError
from boto3.resources.base import ServiceResource

from datetime import datetime
from utils import  replace_none_num, replace_none_str , is_within_hours_threshold, calculate_weighted_average,convert_datetime_string_to_list,convert_datetime_to_iso
from constants import TABLE_NAME, get_dynamodb_client, PARTITION_KEY, get_dynamoDB_table

dynamodb: boto3.client = get_dynamodb_client()
table: ServiceResource = get_dynamoDB_table()


def put_data_to_dynamodb(responseAPI: Dict[str, Any]) -> Dict[str, Any]:
    
    start_count: int = get_count_table() # number of items before running code
    write_requests: List[Dict[str, Any]] = []
    sensors_added: int = 0
    lastHit=False
    
    for result in responseAPI['results']:
        
        if lastHit==True and sensors_added>=start_count:
            break
        
        parameterFound=False
        for param in result['parameters']:

            response = dynamodb.get_item(
                Key={
                    PARTITION_KEY: {
                        'N': str(param['id'])}
                },
                TableName=TABLE_NAME)
            if 'Item' in response: # present in table
                lTimestampsResponse : List[datetime.datetime]= convert_datetime_string_to_list(response['Item']['list_timestamps']['S'])
                lReadingsResponse: List[float]  = json.loads(response['Item']['list_readings']['S'])

        
                last_timestamp : datetime.datetime = lTimestampsResponse[-1]
                
                
                if str(last_timestamp) != str(datetime.strptime(param['lastUpdated'], '%Y-%m-%dT%H:%M:%S%z')): # need to update
                    lTimestampsResponse.append(last_timestamp)
                    last_reading : float = float(param['lastValue'])
                    lReadingsResponse.append(last_reading)
                up_av,up_lt,up_lr = calculate_weighted_average(lReadingsResponse,lTimestampsResponse)
                if len(up_lr) ==0:
                    key = {PARTITION_KEY: {'N': str(param['id']) }}
                    write_requests.append({'DeleteRequest': {'Key': key}})
                else:
                    up_ltISO = convert_datetime_to_iso(up_lt)
                    
                    item = {
                        PARTITION_KEY: {'N': str(param['id'])},
                        'name_provider' :response['Item']['name_provider'],
                        'latitude' : response['Item']['latitude'],
                        'longitude' : response['Item']['longitude'],
                        'parameter_name' : response['Item']['parameter_name'],
                        'list_timestamps': {'S': str(up_ltISO)},
                        'list_readings': {'S' : str(up_lr)},
                        'average' : {'N' : str(up_av)}
                    }
                    write_requests.append({'PutRequest': {'Item': item}})   
                sensors_added+=1
                parameterFound=True
            else: # not present in table
                if is_within_hours_threshold(datetime.strptime(param['lastUpdated'], '%Y-%m-%dT%H:%M:%S%z')) == True: # Sensor is not in table, but to be added
                    latitude: float = replace_none_num(result['coordinates']['latitude']) if result['coordinates'] is not None else 100
                    longitude: float = replace_none_num(result['coordinates']['longitude']) if result['coordinates'] is not None else 100
                    
                    lTimestamps = [param['lastUpdated']]
                    lReadings = [param['lastValue']]
                    
                    item = {
                        PARTITION_KEY: {'N': str(param['id'])},
                        'name_provider': {'S': replace_none_str(result['name'])},
                        'latitude': {'N': str(latitude)},
                        'longitude': {'N': str(longitude)},
                        'list_timestamps': {'S': str(lTimestamps)},
                        'list_readings': {'S' : str(lReadings)},
                        'parameter_name' : {'S' : str(param['parameter'])},
                        'average' : {'N' : str(param['lastValue'])} # no calculation needed
                    }
                    write_requests.append({'PutRequest': {'Item': item}})
                    sensors_added+=1
                    parameterFound=True
                else:
                    continue #sensor is not in table and also isn't enough in time, go to next param avaliable

            if sensors_added % 25 == 0:
                batch_write_items(write_requests)
                write_requests = [] # empty request list  
        if parameterFound == False:
            lastHit: bool = True
        
    batch_write_items(write_requests)

    return {
        "statusCode": 200,
        "body": {
            "message": "Data added successfully"
        }
    }

    
    
def get_count_table() -> int:
    return dynamodb.scan(TableName=TABLE_NAME, Select='COUNT')['Count'] - 1  # cause mutex    
    
def get_item_from_table(table: ServiceResource, partition_key_name: str, partition_key_value: str) -> Dict[str, Any]:
    try:
        response = table.get_item(
            Key={
                partition_key_name: partition_key_value
            }
        )
    except ClientError as e:
        print(f"Error getting item with partition key '{partition_key_name}': {e.response['Error']['Message']}")
        return {}
    else:
        return response
        
def batch_write_items(write_requests: List[Dict[str, Any]]) -> None:
    try:
        dynamodb.batch_write_item(RequestItems={TABLE_NAME: write_requests})
    except boto3.exceptions.Boto3Error as e:
        error_message: str = "Boto3 error: {}".format(str(e))
        return {
            "statusCode": 500,
            "body": {
                "message": error_message
            }
        }