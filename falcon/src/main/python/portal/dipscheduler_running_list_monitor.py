from commons import http
from commons import watchalert
from datetime import datetime
import logging

__TIMEOUT__ = 3600 * 3

logging.basicConfig(level=logging.INFO)

def __monitor(domain):
    now = datetime.now()

    response = http.get(domain=domain, url='/rest/monitor/runningList')
    if response['code'] == '200' and response['data']['selectjob']:
        running_apps = response['data']['selectjob']

        for running_app in running_apps:
            running_app_name = running_app['jobName']
            running_app_execute_time = datetime.strptime(running_app['executeTime'], '%b %d, %Y %I:%M:%S %p')

            running_app_time = (now - running_app_execute_time).total_seconds()

            if running_app_time > __TIMEOUT__:
                print "Application {} timeout".format(running_app_name)

                watchalert.sendAlertToGroups(
                        "Portal-DIPScheduler", "RunningList Serious", "Application {} timeout".format(running_app_name), "", "DIP_ALL", True, True, False)

if __name__ == '__main__':
    __monitor('10.13.56.252:8084')
    __monitor('10.13.56.253:8084')