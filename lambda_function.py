import json
import os
from typing import Any, Dict, Optional

from api import get_api_data
from constants import TABLE_NAME
from dynamodb_func import  put_data_to_dynamodb
from mutex import get_mutex, release_mutex


def lambda_handler(event: Dict[str, Any], context: Any) -> Optional[Dict[str, Any]]:
    response: Dict[str, Any] = get_api_data()
    if response is not None: #check if there is response
        if not get_mutex(): #if already locked
            return {
                "statusCode": 503,
                "body": {
                    "message": "Resource is currently in use"
                }
            }
        try: # execute actual code
            return put_data_to_dynamodb(response) #returns on successful 
        finally:
            release_mutex()
    else:
        return {
            "statusCode": 500,
            "body": {
                "message": "Error occurred while processing data (Response is None)"
            }
        }
