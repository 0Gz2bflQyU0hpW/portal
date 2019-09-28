#!/usr/bin/env python
# -*- coding:utf-8 -*-

from src.main.python.commons import metric
import time
from src.main.python.commons import watchalert
from zk_info import ZooKeeperInfo
import socket
import logging
import sys


ZOOKEEPER_SERVER = "first.zookeeper.dip.weibo.com:2181," \
                   "second.zookeeper.dip.weibo.com:2181," \
                   "third.zookeeper.dip.weibo.com:2181"

BUSINESS="dip-kafka-zookeeper-monitor"
timestamp = int(time.time() / 60) * 60


def get_cluster_stats(servers):
    """ Get stats for all the servers in the cluster """
    stats = {}
    for host, port in servers:
        try:
            zk = ZooKeeperInfo(host, port)
            stats["%s:%s" % (host, port)] = zk.get_stats()
        except socket.error, e:
            watchalert.sendAlertToGroups("DIP", "Zookeeper",u"%s error: %s" % (host, "not running"), "", "DIP_ALL")
            logging.info('unable to connect to server ''"%s" on port "%s"' % (host, port))
    return stats

def main():
    servers = [s.split(':') for s in ZOOKEEPER_SERVER.split(',')]
    cluster_stats = get_cluster_stats(servers)
    for server, stats in cluster_stats.items():
        stats.pop("zk_version")
        stats['zk_server'] = server
        print stats
        print metric.store(BUSINESS,int(round(time.time() * 1000)),stats)

if __name__ == '__main__':
    sys.exit(main())

