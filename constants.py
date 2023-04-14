import os
import boto3
from typing import Optional
from boto3.resources.base import ServiceResource
from boto3.resources.factory import ResourceFactory

TABLE_NAME: str = os.environ.get('dynamoDB_TABLE_NAME')
REGION_NAME: str = os.environ.get('REGION_NAME')
COUNTRY_ID: str = os.environ.get('COUNTRY_ID')
HOURS_THRESHOLD: int = int(os.environ.get('HOURS_THRESHOLD', '5'))
MUTEX_KEY: int = int(os.environ.get('MUTEX_KEY'))
PARTITION_KEY: str = os.environ.get('PARTITION_KEY')

if not all([TABLE_NAME, REGION_NAME, COUNTRY_ID,HOURS_THRESHOLD,MUTEX_KEY,PARTITION_KEY]):
    raise ValueError('One or more of the required environment variables is not defined')
    
#Could be hardcoded but feels unnecessary at current time
##
LIMIT: int = 150
PAGE: int = 1
OFFSET: int = 0
RADIUS: int = 1000
ORDER_BY: str = 'lastUpdated'
##

_client: boto3.client = None
dynamoDB_Table = None

def get_dynamodb_client() -> boto3.client:
    global _client
    if not _client:
        _client = boto3.client('dynamodb')
    return _client


def get_dynamoDB_table() -> Optional[ServiceResource]:
    global dynamoDB_Table
    if dynamoDB_Table is None:
        dynamoDB_Table = boto3.resource('dynamodb').Table(TABLE_NAME)
    return dynamoDB_Table
