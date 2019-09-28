import urllib
import urllib2
import json
import logging

__ACTIVE__ = 'ACTIVE'
__STANDBY__ = 'STANDBY'


class YarnClient(object):

    def __init__(self, rm1, rm2, timeout):
        self.__rm1 = rm1
        self.__rm2 = rm2

        self.__timeout = timeout

    def get_rm1(self):
        return self.__rm1

    def get_rm2(self):
        return self.__rm2

    def __request(self, rm, url, parameters):
        if parameters:
            url = url + '?' + urllib.urlencode(parameters)

        url = 'http://%s%s' % (rm, url)

        logging.info('request: %s' % url)

        request = urllib2.Request(url)

        request.add_header('Accept', 'application/json')

        response = urllib2.urlopen(url=request, timeout=self.__timeout)

        return json.loads(response.read())

    def is_active(self, rm):
        response = self.__request(rm, '/ws/v1/cluster/info', None)

        if response['clusterInfo']['haState'] == __ACTIVE__:
            return True

        return False

    def is_standby(self, rm):
        return not self.is_active(rm)

    def get_active_rm(self):
        if self.is_active(self.__rm1):
            return self.__rm1
        else:
            return self.__rm2

    def get_applications(self, parameters=None):
        response = self.__request(
            self.get_active_rm(), '/ws/v1/cluster/apps', parameters)

        if response and response.has_key('apps') and response['apps'] and response['apps'].has_key('app'):
            return response['apps']['app']
        else:
            return None

    def output(self):
        print self.__rm1, self.rm2

if __name__ == '__main__':
    rm1 = 'd056072.eos.dip.sina.com.cn:8088'
    rm2 = 'd056081.eos.dip.sina.com.cn:8088'

    timeout = 5

    yarn_client = YarnClient(rm1, rm2, timeout)

    parameters = {}

    parameters['states'] = 'RUNNING'

    apps = yarn_client.get_applications(parameters=parameters)

    if apps:
        for app in apps:
            if app['name'] == 'select_job_weibomobileaction_1464_onlinetime_20171010-day_rawlog':
                print app
        
        print len(apps)
