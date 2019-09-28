import http

__SUMMON_DOMAIN = 'api.dip.weibo.com:9083'

__SUMMON_API = '/summon/writes'

__BUSINESS = 'business'
__TIMESTAMP = 'timestamp'
__DIMENSIONS = 'dimensions'
__METRICS = 'metrics'

__RECORDS = 'records'


def store(business, timestamp, metrics):
    parameters = ''

    for name in metrics:
        value = metrics[name]
        value_type = type(value)

        if value_type == str:
            parameters += (__SUMMON_FORMAT % (name, value, __STRING))
        elif value_type == int:
            parameters += (__SUMMON_FORMAT % (name, str(value), __LONG))
        elif value_type == float:
            parameters += (__SUMMON_FORMAT % (name, str(value), __FLOAT))
        else:
            continue

    return http.post(__SUMMON_DOMAIN, __SUMMON_API, {__BUSINESS: business, __TIMESTAMP: timestamp, __PARAMETERS: parameters})

def store(records):
    return http.post(__SUMMON_DOMAIN, __SUMMON_API, {__RECORDS: records})

if __name__ == '__main__':
    records = []

    record = {}

    record[__BUSINESS] = 'dip_business_test'
    record[__TIMESTAMP] = 1516610699478
    record[__DIMENSIONS] = {'d1': 'v1', 'd2': 'v2'}
    record[__METRICS] = {'m1': 1, 'm2': 2}

    records.append(record)

    print store(records)