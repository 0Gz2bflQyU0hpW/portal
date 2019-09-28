#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
import commands
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s', filename='/var/log/falcon/kafka-survival-monitor.log', filemode='a')
from commons import watchalert

'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

'''
author: jianhong1
date: 2017-12-12
'''

def report_to_group(subject, content):
    watchalert.sendAlertToGroups("Kafka", "KafkaSurvivalMonitor", subject, content, "DIP_ALL", True, True, False)
    logging.error(content)


def report_to_user(subject, content):
    watchalert.sendAlertToUsers("Kafka", "KafkaSurvivalMonitor", subject, content, "jianhong1", True, True, False)
    logging.error(content)


def main():
    output = commands.getoutput('netstat -tnlp | grep 9092')
    hostname = commands.getoutput('hostname')

    if output == '':
        subject = 'Kafka rpcService is not survival, now restart. Hostname: %s' % hostname
        content = subject
        report_to_user(subject, content)
        os.system('/usr/local/kafka_2.10-0.8.2.1/bin/kafka-server-start.sh /usr/local/kafka_2.10-0.8.2.1/config/server.properties > /dev/null 2>&1 &')
    else:
        logging.info(output)


if __name__ == "__main__":
    main()