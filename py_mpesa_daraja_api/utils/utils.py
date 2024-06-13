import logging
import base64
import time
from datetime import datetime
from typing import Optional, Union

logger = logging.getLogger(__name__)


def encode_string(txt: str) -> Optional[bytes]:
    """
    Base64 Encode a given string.

    :param txt: The string to encode.
    :return: The base64 encoded string as bytes, or None if encoding fails.
    """
    try:
        return base64.b64encode(txt.encode())
    except Exception as e:
        logger.error(f"Error encoding string: {e}")
        return None


def current_timestamp(ms: bool = True) -> Union[int, float]:
    """
    Get the current timestamp.

    :param ms: If True, return the timestamp in milliseconds. Otherwise, return it in seconds.
    :return: The current timestamp as an integer (milliseconds) or float (seconds).
    """
    current_time = time.time()
    return int(current_time * 1000) if ms else current_time


def convert_datetime_to_int(dt_time: datetime) -> int:
    """
    Convert a datetime object to an integer formatted as YYYYMMDDHHMMSS.

    :param dt_time: The datetime object to convert.
    :return: The datetime as an integer formatted as YYYYMMDDHHMMSS.
    """
    return int(dt_time.strftime("%Y%m%d%H%M%S"))
