from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from datetime import timedelta
import logging

es_domain = "es.intra.dip.weibo.com"
es_port = 9200

es_user = "admin"
es_passwd = "esadmin"

index_prefix = "dip-godeyes-"
index_type_collect = "collect"
index_type_predict = "predict"

ts_format = "%Y%m%d%H%M%S"
uts_format = "%Y-%m-%dT%H:%M:%S"

client = Elasticsearch(hosts=[es_domain], http_auth=(
    es_user, es_passwd), port=es_port)


def hasException(begin_time, end_time):
    search = {
        "index": "dip-godeyes-*",
        "doc_type": "collect",
        "body": {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "format": "yyyyMMddHHmmss",
                                    "gt": datetime.strftime(begin_time, ts_format),
                                    "lte": datetime.strftime(end_time, ts_format),
                                    "time_zone": "+08:00"
                                }
                            }
                        },
                        {
                            "term": {
                                "exception": True
                            }
                        }
                    ]
                }
            }
        }
    }

    count = client.count(index=search["index"], doc_type=search[
        "doc_type"], body=search["body"])["count"]

    return True if count > 0 else False


def load_data(begin_time, end_time):
    datas = []

    search = {
        "index": "dip-godeyes-*",
        "doc_type": "collect",
        "body": {
            "query": {
                "range": {
                    "timestamp": {
                        "format": "yyyyMMddHHmmss",
                        "gt": datetime.strftime(begin_time, ts_format),
                        "lte": datetime.strftime(end_time, ts_format),
                        "time_zone": "+08:00"
                    }
                }
            }
        }
    }

    responses = helpers.scan(client, index=search["index"], doc_type=search[
        "doc_type"], query=search["body"])

    for response in responses:
        data = response["_source"]

        rpcService = data["rpcService"]
        timestamp = data["timestamp"]
        value = data["c_value"]

        timestamp = datetime.strftime(datetime.strptime(
            timestamp, uts_format) + timedelta(hours=8), ts_format)

        datas.append(
            {"rpcService": rpcService, "timestamp": timestamp, "value": value})

    return datas


def load_history_datas(begin_time=datetime.now(), days=7):
    datas = []

    times = []

    for offset in range(0, days + 1):
        times.append(begin_time - timedelta(days=offset))

    for index in range(0, days):
        begin = times[index + 1]
        end = times[index]

        if not hasException(begin, end):
            load_day_datas = load_data(begin, end)

            datas = datas + load_day_datas

            logging.info("({0}, {1}] load data points: {2}".format(datetime.strftime(
                begin, ts_format), datetime.strftime(end, ts_format), str(len(load_day_datas))))
        else:
            logging.info(
                "({0}, {1}] has exception data points, skip".format(datetime.strftime(
                    begin, ts_format), datetime.strftime(end, ts_format)))

    return datas

if __name__ == "__main__":
    datas = load_history_datas(days=7)

    for data in datas:
        print data

    print datas[len(datas) - 1]
