import datetime
import time
from hadoop import yarn
from commons import watchalert
import logging
import traceback
import sys


logging.basicConfig(level=logging.INFO)


def get_times():
    now = datetime.datetime.now()

    end = int(time.mktime(datetime.datetime(
        year=now.year, month=now.month, day=now.day, hour=now.hour, minute=0, second=0).timetuple())) * 1000

    begin = end - 3600000

    return (begin, end)

if __name__ == '__main__':
    times = get_times()

    rm1 = 'd056072.eos.dip.sina.com.cn:8088'
    rm2 = 'd056081.eos.dip.sina.com.cn:8088'

    timeout = 5

    yarn_client = yarn.YarnClient(rm1, rm2, timeout)

    parameters = {}

    parameters['states'] = 'FAILED'
    parameters['finishedTimeBegin'] = str(times[0])
    parameters['finishedTimeEnd'] = str(times[1])

    try:
        apps = yarn_client.get_applications(parameters=parameters)
    except Exception:
        logging.error("get failed apps from yarn error: %s" %
                      traceback.format_exc())

        sys.exit(0)

    if apps:
        appnames = []

        for app in apps:
            appnames.append(app['name'])

        logging.warn("faild apps: %s" % str(appnames))

        watchalert.sendAlertToGroups(
            "Hadoop-Yarn(Batch)", "Application Falied", str(appnames), str(appnames), "DIP_ALL", True, True, False)
