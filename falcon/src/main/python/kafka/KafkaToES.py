#!/usr/bin/python
# -*- coding: UTF-8 -*-

import commands
import logging
import re
from kafka import KafkaProducer
import time
import json

'''
author: jianhong1
date: 2017-09-26
'''

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/falcon/kafka-to-es.log',
                    filemode='a')


BROKER_LIST = 'first.kafka.dip.weibo.com:9092','second.kafka.dip.weibo.com:9092','third.kafka.dip.weibo.com:9092','fourth.kafka.dip.weibo.com:9092','fifth.kafka.dip.weibo.com:9092'
TOPIC = 'dip-kafka2es-common';
API_VERSION = (0, 8, 2)
producer = KafkaProducer(bootstrap_servers=BROKER_LIST, api_version = API_VERSION)


class KafkaClient(object):

    def __init__(self, zookeeper, consumer_group, topic, last_offset_sum, last_logsize_sum):
        self.zookeeper = zookeeper
        self.consumer_group = consumer_group
        self.topic = topic
        self.last_offset_sum = last_offset_sum
        self.last_logsize_sum = last_logsize_sum


    def consumer_offset_checker(self):
        command = '/usr/lib/kafka_2.10-0.8.2.1/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper %s --group %s' %  (self.zookeeper, self.consumer_group)
        output = commands.getoutput(command).split('\n')
        if len(output) == 1:
            logging.warn('there is not ConsumerGroup: %s \t %s' % (self.consumer_group, output))
            return
        timestamp = int(time.time()*1000)
        lag_sum = 0
        offset_sum = 0;
        logsize_sum = 0;
        for line in output[1:]:
            if line.find(self.topic) == -1:
                continue
            value = re.split('\s+', line)
            lag_sum = lag_sum + int(value[5])
            logsize_sum = logsize_sum + int(value[4])
            offset_sum = offset_sum + int(value[3])
        msg = "{'business': 'kafka-produce-consume-checker', 'timestamp': %s, 'group': \'%s\', 'topic': \'%s\', 'offset': %s, 'logsize': %s, 'lag': %s ,'last_offset': %s, 'last_logsize': %s}"  \
              % (timestamp, self.consumer_group, self.topic, offset_sum, logsize_sum, lag_sum, self.last_offset_sum, self.last_logsize_sum)
        producer.send(TOPIC, json.dumps(eval(msg)).encode('utf-8'))
#        logging.info(eval(msg))

        return offset_sum, logsize_sum


if __name__ == "__main__":
    zookeeper_ali = 'first.zookeeper.aliyun.dip.weibo.com:2181,second.zookeeper.aliyun.dip.weibo.com:2181,third.zookeeper.aliyun.dip.weibo.com:2181/kafka/k1'
    zookeeper_k1001 = 'first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001'
    consumer_group = 'openapi_feed_kafka_receiver'
    offset_sum1 = 0
    logsize_sum1 = 0
    offset_sum2 = 0
    logsize_sum2 = 0
    offset_sum3 = 0
    logsize_sum3 = 0
    offset_sum4 = 0
    logsize_sum4 = 0
    offset_sum5 = 0
    logsize_sum5 = 0
    offset_sum6 = 0
    logsize_sum6 = 0
    while(True):
        offset_sum1, logsize_sum1 = KafkaClient(zookeeper_ali,consumer_group,"weibo_status_attitudes", offset_sum1, logsize_sum1).consumer_offset_checker()
        offset_sum2, logsize_sum2 = KafkaClient(zookeeper_ali,consumer_group,"weibo_status_comment", offset_sum2, logsize_sum2).consumer_offset_checker()
        offset_sum3, logsize_sum3 = KafkaClient(zookeeper_k1001,consumer_group,"weibo_status_read", offset_sum3, logsize_sum3).consumer_offset_checker()
        offset_sum4, logsize_sum4 = KafkaClient(zookeeper_k1001,consumer_group,"weibo_status_update", offset_sum4, logsize_sum4).consumer_offset_checker()

        offset_sum5, logsize_sum5 = KafkaClient(zookeeper_k1001,'dip-kafka2es-common-new','dip-kafka2es-common', offset_sum5, logsize_sum5).consumer_offset_checker()
        offset_sum6, logsize_sum6 = KafkaClient(zookeeper_k1001,'dip-kafka2es-trace','dip-kafka2es-trace', offset_sum6, logsize_sum6).consumer_offset_checker()

        time.sleep(300)

    producer.close()
