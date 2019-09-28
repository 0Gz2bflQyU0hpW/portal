#!/usr/bin/python
# -*- coding: UTF-8 -*-

import commands
import logging
import re
from commons import watchalert

'''
author: jianhong1
date: 2017-09-26
'''


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/falcon/kafka-client.log',
                    filemode='a')


'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class KafkaClient(object):

    def __init__(self, zookeeper, consumer_group, topic, threshold):
        self.zookeeper = zookeeper
        self.consumer_group = consumer_group
        self.topic = topic
        self.threshold = threshold

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
            lag_sum = lag_sum + int(value[5])
    #        msg = "{'group': %s, 'topic': %s, 'pid': %s, 'offset': %s, 'logsize': %s, 'lag': %s }" % (value[0], value[1], value[2], value[3], value[4], value[5])
    #        print msg
        logging.info("consumer_group: %s, topic: %s, lag: %s" % (self.consumer_group, self.topic, lag_sum))
        if lag_sum > self.threshold:
            subject = 'ConsumerGroup: %s, Topic: %s, delayed messages: %s, threshold: %d' % (value[0], value[1], lag_sum, self.threshold)
            content = subject
            if self.consumer_group == 'openapi_feed_kafka_receiver':
                watchalert.sendAlertToUsers("Kafka", "ConsumerOffset", subject, content, "yurun,jianhong1,tianlin5,jiajun3,leijian", True, True, False)
            elif self.consumer_group == 'dip-kafka2es-common-new' or self.consumer_group == 'dip-kafka2es-trace':
                watchalert.sendAlertToUsers("Kafka", "ConsumerOffset", subject, content, "yurun,jianhong1,liunan1", True, True, False)
            logging.warn(content)


if __name__ == "__main__":
    zookeeper_ali = 'first.zookeeper.aliyun.dip.weibo.com:2181,second.zookeeper.aliyun.dip.weibo.com:2181,third.zookeeper.aliyun.dip.weibo.com:2181/kafka/k1'
    zookeeper_k1001 = 'first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001'
    consumer_group = 'openapi_feed_kafka_receiver'

    KafkaClient(zookeeper_ali,consumer_group,"weibo_status_attitudes", 40000).consumer_offset_checker()
    KafkaClient(zookeeper_ali,consumer_group,"weibo_status_comment", 10000).consumer_offset_checker()
    KafkaClient(zookeeper_k1001,consumer_group,"weibo_status_read", 1000000).consumer_offset_checker()
    KafkaClient(zookeeper_k1001,consumer_group,"weibo_status_update", 30000).consumer_offset_checker()

    KafkaClient(zookeeper_k1001,'dip-kafka2es-common-new','dip-kafka2es-common', 20000).consumer_offset_checker()
    KafkaClient(zookeeper_k1001,'dip-kafka2es-trace','dip-kafka2es-trace', 10000000).consumer_offset_checker()


