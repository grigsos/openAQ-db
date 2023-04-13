from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from constants import DAYS_THRESHOLD


def replace_none_str(value: Optional[str]) -> str:
    if value is None or (isinstance(value, str) and len(value) == 0):
        return "None"
    return value


def replace_none_num(value: Optional[str]) -> Union[float, int]:
    if value is None or (isinstance(value, str) and len(value) == 0):
        return 100
    return float(value)


def get_parameters(result: Dict[str, Any]) -> List[Dict[str, Union[str, int]]]:
    parameters: List[Dict[str, Union[str, int]]] = []
    for param in result['parameters']:
        if 'lastUpdated' in param:
            last_updated_str: str = replace_none_str(param['lastUpdated'])
            last_updated: datetime = datetime.fromisoformat(last_updated_str[:-6])
            if datetime.now() - last_updated <= timedelta(days=DAYS_THRESHOLD):
                parameters.append({
                    'id': replace_none_str(param['id']),
                    'unit': replace_none_str(param['unit']),
                    'parameter': replace_none_str(param['parameter']),
                    'displayName': replace_none_str(param['displayName']),
                    'parameterId': replace_none_str(param['parameterId']),
                    'lastUpdated': last_updated_str,
                })
    return parameters
