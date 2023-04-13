import json
import os
from typing import Any, Dict, Optional

from api import get_api_data
from constants import TABLE_NAME
from dynamodb import  put_data_to_dynamodb


def lambda_handler(event: Dict[str, Any], context: Any) -> Optional[Dict[str, Any]]:
    response: Dict[str, Any] = get_api_data()
    if response is not None:
        return put_data_to_dynamodb(response)
    else:
        return {
            "statusCode": 500,
            "body": {
                "message": "Error occurred while processing data (Response is None)"
            }
        }
