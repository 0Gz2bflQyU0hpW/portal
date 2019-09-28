#!/usr/bin/python
# -*- coding: UTF-8 -*-

import commands
import logging
import re
import traceback
from commons import watchalert
from mysql import mysql_client


'''
author: jianhong1
date: 2017-10-25
'''


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/falcon/kafka_client_auto.log',
                    filemode='a')

'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class KafkaClient(object):

    def __init__(self, zookeeper, consumer_group, topic, threshold, contact_person):
        self.zookeeper = zookeeper
        self.consumer_group = consumer_group
        self.topic = topic
        self.threshold = threshold
        self.contact_person = contact_person

    def consumer_offset_checker(self):
        command = '/usr/lib/kafka_2.10-0.8.2.1/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --zookeeper %s --group %s' %  (self.zookeeper, self.consumer_group)
        output = commands.getoutput(command).split('\n')
        if len(output) == 1:
            logging.warn('there is not ConsumerGroup: %s \t %s' % (self.consumer_group, output))
            return
        lag_sum = 0
        for line in output[1:]:
            if line.find(self.topic) == -1:
                continue
            value = re.split('\s+', line)
            if value[1] != self.topic:
                continue
            lag_sum = lag_sum + int(value[5])
        #        msg = "{'group': %s, 'topic': %s, 'pid': %s, 'offset': %s, 'logsize': %s, 'lag': %s }" % (value[0], value[1], value[2], value[3], value[4], value[5])
        #        print msg
        logging.info("consumer_group: %s, topic: %s, lag: %s, threshold: %s" % (self.consumer_group, self.topic, lag_sum, self.threshold))
        if lag_sum > self.threshold:
            subject = 'ConsumerGroup: %s, Topic: %s, delayed messages: %s, threshold: %d' % (self.consumer_group, self.topic, lag_sum, self.threshold)
            content = subject
            watchalert.sendAlertToUsers("Kafka", "KafkaClientAuto ConsumerOffset", subject, content, self.contact_person, True, True, False)
            logging.warn(content)


def get_threshold(qps, topic_name):
    if topic_name == 'dip-kafka2es-trace':
        return 50000000
    return int(qps) * 50


if __name__ == "__main__":
    zookeeper_ali = 'first.zookeeper.aliyun.dip.weibo.com:2181,second.zookeeper.aliyun.dip.weibo.com:2181,third.zookeeper.aliyun.dip.weibo.com:2181/kafka/k1'
    zookeeper_k1001 = 'first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001'

    consumer_list = []
    try:
        consumer_list = mysql_client.get_consumers_info()
    except:
        subject = 'mysqlclient unable fetch data'
        content = 'mysqlclient unable fetch data \n %s' % traceback.format_exc()
        logging.error(content)
        watchalert.sendAlertToGroups("Kafka", "KafkaClientAuto ConsumerOffset", subject, content, "DIP_ALL", True, True, False)
    for element in consumer_list:
        topic_name = element['topic_name']
        consumer_group = element['consumer_group']
        contact_person = element['contact_person']
        qps = element['qps']

        if topic_name == 'app_weibomobilekafka1234_weibomobileaction26':
            continue
        threshold = get_threshold(qps, topic_name)
        KafkaClient(zookeeper_k1001, consumer_group, topic_name, threshold, contact_person).consumer_offset_checker()