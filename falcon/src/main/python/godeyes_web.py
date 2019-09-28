import web
import logging
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)

urls = (
    "/watch", "Watch"
)

es_domain = "es.intra.dip.weibo.com"
es_port = 9200

es_user = "admin"
es_passwd = "esadmin"

index_prefix = "dip-godeyes-"
index_type_collect = "collect"
index_type_predict = "predict"

ts_format = "%Y%m%d%H%M%S"

threshold = 0.05

client = Elasticsearch(hosts=[es_domain], http_auth=(
    es_user, es_passwd), port=es_port)


class Watch:

    def utc(self, timestamp):
        return datetime.strptime(timestamp, ts_format) - timedelta(hours=8)

    def get_predict_value(self, rpcService, timestamp):
        search = {
            "index": index_prefix + "*",
            "doc_type": index_type_predict,
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"rpcService": rpcService}},
                            {"term": {"timestamp": self.utc(timestamp)}}
                        ]
                    }
                }
            }
        }

        responses = client.search(index=search["index"], doc_type=search[
            "doc_type"], body=search["body"])["hits"]["hits"]

        return responses[0]["_source"]["value"] if responses else None

    def get_boundary_value(self, value):
        min = value - (value * threshold)
        max = value + (value * threshold)

        return (min, max)

    def save_to_es(self, rpcService, timestamp, collect_value, predict_value, exception):
        doc = {
            "index": index_prefix + timestamp[0:6],
            "type": index_type_collect,
            "id": rpcService + "_" + timestamp,
            "data": {
                "rpcService": rpcService,
                "timestamp": self.utc(timestamp),
                "c_value": collect_value,
                "p_value": predict_value,
                "exception": exception
            }
        }

        client.index(index=doc["index"], doc_type=doc[
                     "type"], id=doc["id"], body=doc["data"])

    def GET(self):
        params = web.input()

        rpcService = params.rpcService
        timestamp = params.timestamp
        value = int(params.value)

        predict_value = self.get_predict_value(rpcService, timestamp)

        exception = False

        if predict_value:
            boundary = self.get_boundary_value(predict_value)

            if value < boundary[0] or boundary[1] < value:
                exception = True
        else:
            predict_value = 0

        result = {"rpcService": rpcService, "timestamp": timestamp, "collect value": value,
                  "predict value": predict_value, "exception": exception}

        self.save_to_es(rpcService, timestamp, value, predict_value, exception)

        logging.info(result)

        return result


if __name__ == "__main__":
    app = web.application(urls, globals())

    app.run()
