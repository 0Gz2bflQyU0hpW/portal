import urllib
import urllib2
import json
import logging
import traceback


def get(domain, url, parameters=None, protocol='http', timeout=3):
    if parameters:
        url = url + '?' + urllib.urlencode(parameters)

    url = '%s://%s%s' % (protocol, domain, url)

    logging.info('request: %s' % url)

    request = urllib2.Request(url)

    request.add_header('Accept', 'application/json')
    request.add_header('Accept-Charset', 'utf-8')

    response = urllib2.urlopen(url=request, timeout=timeout)

    try:
        data = json.loads(response.read())
    except ValueError:
        data = {}
        logging.warn(traceback.format_exc())

    return data


def post(domain, url, parameters, protocol='http', timeout=3):
    url = '%s://%s%s' % (protocol, domain, url)

    logging.info('request: %s' % url)

    request = urllib2.Request(url, urllib.urlencode(parameters))

    request.add_header('Accept', 'application/json')
    request.add_header('Accept-Charset', 'utf-8')

    response = urllib2.urlopen(url=request, timeout=timeout)

    try:
        data = json.loads(response.read())
    except ValueError:
        data = {}
        logging.warn(traceback.format_exc())

    return data


if __name__ == '__main__':
    domain = "api.dip.weibo.com:9083"

    url = "/summon/write"

    parameters = {"business": "business_test",
                  "timestamp": 1509587670222,
                  "parameters": "key1|str_value|string;key2|2|long;key3|0.123|float"}

    response = post(domain, url, parameters)

    print response
