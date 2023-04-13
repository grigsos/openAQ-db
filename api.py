import requests
from typing import Any, Dict, Optional

from constants import COUNTRY_ID, LIMIT, OFFSET, ORDER_BY, RADIUS


def get_api_data() -> Optional[Dict[str, Any]]:
    url: str = f"https://api.openaq.org/v2/locations?limit={LIMIT}&page=1&offset={OFFSET}&sort=desc&radius={RADIUS}&country_id={COUNTRY_ID}&order_by={ORDER_BY}&dumpRaw=false"
    headers: Dict[str, str] = {"accept": "application/json"}
    try:
        response: Dict[str, Any] = requests.get(url, headers=headers).json()
        if 'results' in response:
            return response
    except requests.exceptions.RequestException as e:
        error_message : str = "Error occurred while processing adding data to table: {}".format(str(e))
        return {
                    "statusCode": 500,
                    "body": {
                        "message": error_message
                    }
                }
