#!/usr/bin/python
# -*- coding:utf-8 -*-

"""
author: jianhong1
date: 2018-08-29
"""

from kazoo.client import KazooClient
import logging
import os
import watchalert

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s',
                    filename='/var/log/falcon/controller-change-monitor.log',
                    filemode='a')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

#ZK_SERVER = '10.13.4.44:2181'
#KAFKA_PATH = '/kafka_intra/'
ZK_SERVER = '10.13.80.21:2181,10.13.80.22:2181,10.13.80.23:2181'
KAFKA_PATH = '/kafka/k1001/'


def main():
    """连接zk"""
    zk = KazooClient(hosts=ZK_SERVER, read_only=True)
    zk.start()

    current_epoch = zk.get(KAFKA_PATH + "controller_epoch")[0]

    """若不存在本地文件，则先创建"""
    filename = "controller_epoch"
    if not os.path.exists(filename):
        f = open(filename, 'w')
        try:
            f.write(current_epoch)
        finally:
            f.close()

    """打开本地文件读取相关信息"""
    f = open(filename, 'r')
    try:
        last_epoch = f.read().strip()
    finally:
        f.close()

    """查看controller epoch是否改变，如果改变则发送报警"""
    if last_epoch == current_epoch:
        logging.info("last epoch:%s, current epoch: %s", last_epoch, current_epoch)
    else:
        controller_info = zk.get(KAFKA_PATH + "controller")[0]
        msg = "epoch changed, last epoch:%s, current epoch:%s, current controller:%s" \
              % (last_epoch, current_epoch, controller_info)
        logging.warn(msg)
#        watchalert.sendAlertToGroups("kafka", "controller", msg, msg, "DIP_ALL", True, True, False)
        watchalert.sendAlertToUsers("kafka", "controller", msg, msg, "jianhong1", True, True, False)
        f = open(filename, 'w')
        try:
            f.write(current_epoch)
        finally:
            f.close()


if __name__ == "__main__":
    main()
