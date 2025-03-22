from datetime import datetime


def parse_date(date: str):
    dt = datetime.strptime(date, "%Y-%m-%d")
    return {"year": dt.year, "month": dt.month, "day": dt.day}


def parse_time(time: str) -> dict:
    # Parse the time string into a datetime object
    dt = datetime.strptime(time, "%H:%M:%S")
    return {
        "hours": dt.hour,
        "minutes": dt.minute,
        "seconds": dt.second,
        "nanos": 0,  # Assuming nanoseconds are zero
    }
