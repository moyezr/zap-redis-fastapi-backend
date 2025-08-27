# utils.py
from datetime import datetime
import dateparser

def get_parsed_timestamp(time_str: str) -> int:
    """Parse natural language time string to epoch timestamp"""
    parsed = dateparser.parse(time_str)
    if not parsed:
        raise ValueError(f"Could not parse time string: {time_str}")
    return int(parsed.timestamp())

def get_current_timestamp() -> int:
    """Get current time as epoch timestamp"""
    return int(datetime.now().timestamp())

def get_end_of_day_timestamp() -> int:
    """Get timestamp for end of current day"""
    now = datetime.now()
    end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59)
    return int(end_of_day.timestamp())