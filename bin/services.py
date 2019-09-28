#!/usr/bin/env python
# -*- coding: utf-8 -*-

import data_platform
import argparse
import os
import commands

java_home = data_platform.JAVA_HOME

data_platform_home = data_platform.DATA_PLATFORM_HOME

data_platform_version = data_platform.VERSION

running = "running"

stoped = "stoped"


def start():
    if status() == running:
        print "services is runing: %s" % getPID()

        return

    command = "sudo -u hdfs %s/bin/java -Xmx1024M -cp %s/services-server/target/data-platform-services-server-%s.jar:%s/services-server/target/data-platform-services-server-%s-lib/* com.weibo.dip.data.platform.services.server.ServicesApplication >> /dev/null &" % (
        java_home, data_platform_home, data_platform_version, data_platform_home, data_platform_version)

    print command

    os.system(command)


def getPID():
    return commands.getoutput(
        "ps aux | grep -v root | grep 'com.weibo.dip.data.platform.services.server.ServicesApplication' | grep -v grep | awk '{print $2}'")


def status():
    pid = getPID()

    if pid:
        return running
    else:
        return stoped


def stop():
    pid = getPID()

    if not pid:
        print "services stoped"

        return

    command = "sudo -u hdfs kill %s" % getPID()

    print command

    os.system(command)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="data-platform services")

    group = parser.add_mutually_exclusive_group()

    group.add_argument("-start", "--start",
                       action="store_true", required=False, help="services start")

    group.add_argument("-status", "--status",
                       action="store_true", required=False, help="services status")

    group.add_argument("-stop", "--stop", action="store_true",
                       required=False, help="services stop")

    args = parser.parse_args()

    if args.start:
        start()
    elif args.status:
        print status()
    elif args.stop:
        stop()
    else:
        parser.print_help()