import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s',
    filename='/var/log/data-platform/godeyes_predict.log',
    filemode='a')

from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
from pandas import DataFrame
from statsmodels.tsa.arima_model import ARIMA
import godeyes_load

client = Elasticsearch(hosts=["es.intra.dip.weibo.com"], http_auth=(
    "admin", "esadmin"), port=9200)


def writetoEs(nowtime, result, service_id):
    actions = []

    for index in range(len(result)):
        predict_time = nowtime + timedelta(seconds=60) * (index + 1)

        record = {
            '_op_type': 'index',
            # index
            '_index': "dip-godeyes-" + predict_time.strftime('%Y%m'),
            '_type': "predict",  # type
            '_id': service_id + '_' + predict_time.strftime('%Y%m%d%H%M%S'),
            '_source': {
                "timestamp": predict_time - timedelta(hours=8),
                "rpcService": service_id,
                "value": int(result[index])
            }
        }

        logging.info(record)

        actions.append(record)

    helpers.bulk(client, actions)


def preprocess(sourcedata):
    df = pd.DataFrame(sourcedata, columns=['timestamp', 'rpcService', 'value'])

    df['value'] = df['value'].astype('float64')

    df.index = df['timestamp']

    df = df.sort_values(by='timestamp', ascending=True)

    return df


def train_simple(nowtime, df, step=2):
    df_gy = df.groupby('rpcService')

    idvar = []
    idcons = []

    idmean = df_gy.mean()

    for index, row in df_gy.std().iterrows():
        if row['value'] != 0:
            idvar.append(index)
        else:
            idcons.append(index)

            res = idmean[idmean.index == index].values[0][0]
            res = int(res)
            resarr = [res] * step

            tmp = df[df.rpcService == index].sort_values(
                by='timestamp', ascending=True)

            lasttime = tmp.ix[len(tmp) - 1, 'timestamp']

            writetoEs(nowtime, resarr, index)

    logging.info("no changing count{0}:{1}".format(len(idcons), idcons))

    errid = []

    for service_id in idvar:
        tmp = df[df.rpcService == service_id].sort_values(
            by='timestamp', ascending=True)

        lasttime = tmp.ix[len(tmp) - 1, 'timestamp']

        trainset = tmp['value']

        model = ARIMA(trainset, order=(2, 1, 0))

        try:
            model_fit = model.fit(disp=0)
            output = model_fit.forecast(step)  # parameter
        except:
            errid.append(service_id)

        writetoEs(nowtime, output[0], service_id)

        logging.info("the changing rpcService:{0}, trainset size:{1}, lasttime of trainset:{2}".format(
            service_id, len(trainset), lasttime))

        logging.info("predict length:{0}".format(len(output[0])))

if __name__ == "__main__":
    nowtime = datetime.strptime(datetime.now().strftime('%Y%m%d%H%M00'),'%Y%m%d%H%M%S')

    logging.info("nowtime:{0}".format(nowtime))

    sourcedata = godeyes_load.load_history_datas(days=7)

    train_simple(nowtime, preprocess(sourcedata), step=30)
