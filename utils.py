from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
import pytz

from constants import HOURS_THRESHOLD


def replace_none_str(value: Optional[str]) -> str:
    if value is None or (isinstance(value, str) and len(value) == 0):
        return "None"
    return value


def replace_none_num(value: Optional[str]) -> Union[float, int]:
    if value is None or (isinstance(value, str) and len(value) == 0):
        return 100
    return float(value)

def calculate_weighted_average(readings: List[float], timestamps: List[datetime]) -> Tuple[float, List[datetime], List[float]]:
    recent_datetimes, recent_readings = get_recent_datetimes(timestamps, readings)
    
    
    if len(recent_readings) == 0:
        return 0, [], []
    elif len(recent_readings) == 1:
        return recent_readings[0], recent_datetimes, recent_readings
    else:
        total_weight = 0
        weighted_sum = 0
        
        print(recent_datetimes)
    
        # Calculate the weighted average over the past x hours
        for i in range(len(recent_datetimes)-1, -1, -1):
            tz = pytz.timezone('UTC')  # create a timezone object with UTC offset
            recent_datetime_with_tz = tz.localize(recent_datetimes[i])  # add timezone information to datetime object
            time_diff = datetime.datetime.now(datetime.timezone.utc) - recent_datetime_with_tz
    
            weight = 1 / time_diff.total_seconds()  # Use inverse time difference as weight
            total_weight += weight
            weighted_sum += weight * recent_readings[i]
    
        try:
            avg = weighted_sum / total_weight
        except ZeroDivisionError:
            avg = 0  
        
        return round(avg, 3), recent_datetimes, recent_readings
            


def get_recent_datetimes(datetime_list: List[datetime], readings_list: List[float]) -> Tuple[List[datetime], List[float]]:
    
    print(datetime_list)
    print(readings_list)
    now = datetime.now()
    recent_datetimes = []
    recent_readings = []
    for dt, reading in reversed(list(zip(datetime_list, readings_list))):
        if is_within_hours_threshold(dt):
            recent_datetimes.insert(0, dt)
            recent_readings.insert(0, reading)
        else:
            break
    return recent_datetimes, recent_readings
    
def is_within_hours_threshold(dt: datetime) -> bool:
    now = datetime.now(dt.tzinfo)
    return now - dt <= timedelta(hours=HOURS_THRESHOLD)
    
from datetime import datetime
from typing import List

def convert_datetime_string_to_list(datetime_string: str) -> List[datetime]:
    try:
        datetime_strings = datetime_string[1:-1].split(",")
        datetime_objects = [datetime.fromisoformat(dt.strip("'")) for dt in datetime_strings]
        return datetime_objects

    except ValueError as e:
        raise ValueError("Invalid input string format. Please provide a valid string of ISO-formatted datetime strings.") from e
        
        
def convert_datetime_to_iso(input_list):
    output_list = []
    for dt_obj in input_list:
        dt_str = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
        utc_offset = dt_obj.strftime('%z')
        output_str = f"{dt_str}{utc_offset[:-2]}:{utc_offset[-2:]}"
        output_list.append(output_str)
    return output_list
