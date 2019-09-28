#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s', filename='/var/log/falcon/kafka-exist-monitor.log', filemode='a')
import salt.client
import traceback
from commons import watchalert

'''
author: jianhong1
date: 2017-09-29
'''

kafka_aliyun_group = ['first.kafka.aliyun.dip.weibo.com', 'second.kafka.aliyun.dip.weibo.com', 'third.kafka.aliyun.dip.weibo.com']


'''发送报警给用户组,并打印日志'''
def report_to_group(subservice, subject, content):
    watchalert.sendAlertToGroups("Kafka", subservice, subject, content, "DIP_ALL", True, True, False)
    logging.error(content)


def main():
    salt_client = salt.client.LocalClient()
    subservice = "IsExist"

    for kafka_node in kafka_aliyun_group:
        '''节点无响应抛出异常'''
        try:
            kafka_status = salt_client.cmd(kafka_node, 'cmd.run', ['netstat -tnlp | grep 9092'])
            kafka_status_value = kafka_status[kafka_node]
        except:
            subject = "KafkaNode no response, KafkaNode: %s" % kafka_node
            content = "KafkaNode no response, KafkaNode: %s\n %s" % (kafka_node, traceback.format_exc())
            report_to_group(subservice, subject, content)
            continue

        '''若为空说明kafka服务不存在'''
        if kafka_status_value == '':
            subject = 'kafka rpcService is not exist, KafkaNode: %s' % kafka_node
            content = subject
            report_to_group(subservice, subject, content)


if __name__ == "__main__":
    main()