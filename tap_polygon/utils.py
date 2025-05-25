import logging
import typing as t
from datetime import datetime, timezone


def safe_parse_datetime(dt_str: t.Any) -> datetime | None:
    if isinstance(dt_str, datetime):
        return dt_str.replace(tzinfo=timezone.utc) if dt_str.tzinfo is None else dt_str
    if isinstance(dt_str, (int, float)):
        try:
            seconds = _timestamp_to_epoch(dt_str)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logging.warning(f"Could not parse numeric timestamp: {dt_str}, error: {e}")
            return None
    if isinstance(dt_str, str):
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            logging.warning(f"Could not parse datetime string: {dt_str}")
            return None
    logging.warning(f"Invalid datetime type: {type(dt_str)}, value: {dt_str}")
    return None


def _timestamp_to_epoch(ts: int | float | str | None) -> float | None:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        if ts > 1e15:  # nanoseconds
            return ts / 1e9
        if ts > 1e13:  # microseconds
            return ts / 1e6
        if ts > 1e10:  # milliseconds
            return ts / 1e3
        return float(ts)
    return None
