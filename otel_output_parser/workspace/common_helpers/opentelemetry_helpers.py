import dateutil.parser as dp  # type: ignore


def iso8601_to_epoch_s(iso8601_datetime: str) -> float:
    # This may not correctly handle timezones correctly:
    # https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
    return dp.parse(iso8601_datetime).timestamp()


def iso8601_to_epoch_ms(iso8601_datetime: str) -> int:
    return int(iso8601_to_epoch_s(iso8601_datetime) * 1000)
