#!/usr/bin/python
# -*- coding: UTF-8 -*-

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s', filename='/var/log/falcon/storm-supervisor-monitor.log', filemode='a')
import salt.client
import traceback
from commons import watchalert

'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

'''
author: jianhong1
date: 2017-12-04
'''

storm_group = ['d077136043.dip.weibo.com', 'd077136044.dip.weibo.com', 'd077136045.dip.weibo.com', 'd077136046.dip.weibo.com']


'''发送报警给用户组,并打印日志'''
def report_to_user(subject, content):
    watchalert.sendAlertToUsers("Storm1.1.1", "SupervisorWhetherSurvivor", subject, content, "jianhong1,yurun", True, True, False)
    logging.error(content)

'''发送报警给用户组'''
def report_to_group(subject, content):
    watchalert.sendAlertToGroups("Storm1.1.1", "SupervisorWhetherSurvivor", subject, content, "DIP_ALL", True, True, False)
    logging.error(content)

def main():
    salt_client = salt.client.LocalClient()

    for storm_node in storm_group:
        '''节点无响应抛出异常'''
        try:
            supervisor_status = salt_client.cmd(storm_node, 'cmd.run', ['ps aux | grep org.apache.storm.daemon.supervisor.Supervisor | grep -v grep'])
        except:
            subject = "Salt-minion: %s no response" % storm_node
            content = "Salt-minion: %s no response\n %s" % (storm_node, traceback.format_exc())
            report_to_group(subject, content)
            continue

        '''若supervisor服务不存在则重启'''
        if supervisor_status[storm_node] == '':
            subject = 'Supervisor rpcService is not survivor, now restart. Storm node: %s' % storm_node
            content = subject
            report_to_group(subject, content)
            salt_client.cmd(storm_node, 'cmd.run', ['/usr/local/apache-storm-1.1.1/bin/storm supervisor > /dev/null 2>&1 &'])


if __name__ == "__main__":
    main()