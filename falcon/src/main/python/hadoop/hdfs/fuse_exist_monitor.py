#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import logging
import watchalert

def watching():
    command="ps -ef | grep 'fuse_dfs' | grep -v 'grep' | awk '{print $2}' "
    pid=os.popen(command).readline()
    if(len(pid)<=0):
        msg="fuse_dfs closed "
        #watchalert.sendAlertToUsers("Hadoop", "HDFS", msg, msg, "zhiqiang32", True, True, True)
        watchalert.sendAlertToGroups("Hadoop", "HDFS", msg, msg, "DIP_ALL", True, True, True)
    else:
        logging.info("fuse_dfs pid is "+pid)

def configlog():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s',
                        filename='fuse-change-monitor.log',
                        filemode='a')

if __name__=="__main__":
    configlog()
    watching()