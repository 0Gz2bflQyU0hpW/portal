import http

__SUMMON_DOMAIN = 'api.dip.weibo.com:9083'

__SUMMON_API = '/summon/write'

__SUMMON_FORMAT = '%s|%s|%s;'

__STRING = 'string'
__LONG = 'long'
__FLOAT = 'float'

__BUSINESS = 'business'
__TIMESTAMP = 'timestamp'
__PARAMETERS = 'parameters'


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

if __name__ == '__main__':
    business = 'dip_business_test'

    timestamp = '1509587670222'

    metrics = {'key1': 'str_value', 'key2': 123, 'key3': 1.23}

    print store(business, timestamp, metrics)
