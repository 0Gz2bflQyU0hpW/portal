from commons import http
from commons import watchalert
import logging

logging.basicConfig(level=logging.INFO)

__THRESHOLD__ = 30


def __monitor(domain):
    response = http.get(domain=domain, url='/rest/monitor/waitingQueue')
    if response['code'] == '200' and response['data']['selectjob']:
        waiting_apps = len(response['data']['selectjob'])
        if waiting_apps >= __THRESHOLD__:
            logging.warn('%s watting queue serious: %d' %
                         (domain, waiting_apps))

            watchalert.sendAlertToGroups("Portal-DIPScheduler", "WaittingQueue Serious", '%s: %d' % (
                domain, waiting_apps), '%s: %d' % (domain, waiting_apps), "DIP_ALL", True, True, False)

if __name__ == '__main__':
    __monitor('10.13.56.252:8084')
    __monitor('10.13.56.253:8084')
