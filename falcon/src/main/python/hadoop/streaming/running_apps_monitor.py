import datetime
import time
from hadoop import yarn
from commons import watchalert
import logging
import traceback
import sys


logging.basicConfig(level=logging.INFO)

__APPS__ = ['UserRelationStreamingV2']

if __name__ == '__main__':
    rm1 = 'bx004042.dip.sina.com.cn:8088'
    rm2 = 'bx004043.dip.sina.com.cn:8088'

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

    appnames = []

    if apps:
        for app in apps:
            appnames.append(app['name'])

    not_running_apps = []

    for APP in __APPS__:
        if APP not in appnames:
            not_running_apps.append(APP)

    if not_running_apps:
        watchalert.sendAlertToGroups(
            "Hadoop-Yarn(Streaming)", "Application Not Running", str(not_running_apps), str(not_running_apps), "DIP_ALL", True, True, False)
