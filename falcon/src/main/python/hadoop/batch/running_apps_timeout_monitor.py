from commons import timeutil
from hadoop import yarn
from commons import watchalert
import logging
import traceback
import sys


logging.basicConfig(level=logging.INFO)

__THRESHOLD__ = 3 * 3600 * 1000

if __name__ == '__main__':
    now = timeutil.now()

    rm1 = 'd056072.eos.dip.sina.com.cn:8088'
    rm2 = 'd056081.eos.dip.sina.com.cn:8088'

    timeout = 5

    yarn_client = yarn.YarnClient(rm1, rm2, timeout)

    parameters = {}

    parameters['states'] = 'RUNNING'

    try:
        apps = yarn_client.get_applications(parameters=parameters)
    except Exception:
        logging.error("get failed apps from yarn error: %s" %
                      traceback.format_exc())

        sys.exit(0)

    timeout_apps = []

    for app in apps:
        if app['queue'] != 'root.hive':
            continue

        started_time = app['startedTime']
        if now - started_time >= __THRESHOLD__:
            timeout_apps.append(app['name'])

    if timeout_apps:
        logging.info("timeout apps: %s" % str(timeout_apps))

        watchalert.sendAlertToGroups(
            "Hadoop-Yarn(Batch)", "Application Timeout", str(timeout_apps), str(timeout_apps), "DIP_ALL", True, True, False)
