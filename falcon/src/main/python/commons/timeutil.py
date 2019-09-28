import datetime
import time


def now():
    return int(time.mktime(datetime.datetime.now().timetuple())) * 1000


def get_last_hour_times():
    now = datetime.datetime.now()

    end = int(time.mktime(datetime.datetime(
        year=now.year, month=now.month, day=now.day, hour=now.hour, minute=0, second=0).timetuple())) * 1000

    begin = end - 3600000

    return (begin, end)


def get_last_hour_strtimes():
    times = get_last_hour_times()

    return (datetime.datetime.fromtimestamp(times[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(times[1] / 1000).strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == '__main__':
    print now()

    times = get_last_hour_times()

    print times

    times = get_last_hour_strtimes()

    print times
