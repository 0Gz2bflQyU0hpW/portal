#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import commands
import redis
import datetime
import time
import logging
import traceback
from commons import watchalert

'''
author: jianhong1
date: 2017-09-05
'''

REDIS_SERVER = 'rc7840.eos.grid.sina.com.cn'
REDIS_PORT = 7840
HOSTNAME = commands.getoutput('echo $HOSTNAME')
RESTART_HOUR = 6
CENTOS5 = '5'
CENTOS6 = '6'
CENTOS7 = '7'
SLEEPTIME = 10


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/falcon/salt-minion-restart.log',
                    filemode='a')

'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


'''发送报警给用户组'''
def report_to_group(msg, detail_msg):
    watchalert.sendAlertToGroups( "salt", "salt minion", msg, detail_msg, "DIP_ALL", True, True, False)


''' 以字符串形式返回进程id，多个id/n为间隔，如果没有没有符合的返回空字符串'''
def get_salt_pid():
    if get_os_version().startswith(CENTOS5):
        return commands.getoutput("sudo ps aux | grep /usr/bin/salt-minion | grep -v grep | awk '{print $2}'")
    elif get_os_version().startswith(CENTOS6) or get_os_version().startswith(CENTOS7):
        return commands.getoutput("sudo ps aux | grep /usr/local/saltstack/python2.7/bin/salt-minion | grep -v grep | awk '{print $2}'")


'''获取系统版本号'''
def get_os_version():
    fd = commands.getoutput('lsb_release -a')
    version = ''
    for line in fd.split('\n'):
        if line.startswith('Release:'):
            version = line.strip().split('\t')[1]
            break
    return version


'''判断salt-minion是否运行'''
def is_salt_running():
    if get_os_version().startswith(CENTOS5):
        output = commands.getoutput('/etc/init.d/salt-minion status')
        if output.find('running') == -1:
            return False
    elif get_os_version().startswith(CENTOS6) or get_os_version().startswith(CENTOS7):
        output = commands.getoutput('/usr/local/saltstack/python2.7/bin/python /usr/local/saltstack/salt_operate.py status')
        if output.find('is running') == -1:
            return False
    return True


'''正常流程启动salt-minion'''
def start_salt_normal():
    if get_os_version().startswith(CENTOS5):
        os.system('/etc/init.d/salt-minion start')
    elif get_os_version().startswith(CENTOS6) or get_os_version().startswith(CENTOS7):
        os.system('/usr/local/saltstack/python2.7/bin/python /usr/local/saltstack/salt_operate.py start')
    logging.info('starting salt-minion')
    time.sleep(SLEEPTIME)


'''通过'kill -9'命令停止salt-minion'''
def kill_salt():
    if get_salt_pid():
        ''' salt-minion会启动多个进程，默认启动2个，一个等待接受命令，一个等待执行命令'''
        for item in get_salt_pid().split('\n'):
            commands.getoutput("sudo kill -9  %s" % (item))
    time.sleep(1)


'''通过'kill'命令停止salt-minion'''
def kill_salt_centos5():
    if get_salt_pid():
        ''' salt-minion会启动多个进程，默认启动2个，一个等待接受命令，一个等待执行命令'''
        for item in get_salt_pid().split('\n'):
            commands.getoutput("sudo kill %s" % (item))


'''判断salt-minion是否已经停止'''
def is_salt_stopped():
    if get_os_version().startswith(CENTOS5):
        output = commands.getoutput('/etc/init.d/salt-minion status')
        if output.find('stopped') == -1:
            return False
    elif get_os_version().startswith(CENTOS6) or get_os_version().startswith(CENTOS7):
        output = commands.getoutput('/usr/local/saltstack/python2.7/bin/python /usr/local/saltstack/salt_operate.py status')
        if output.find('is not running') == -1:
            return False
    return True


'''正常流程停止salt-minion'''
def stop_salt_normal():
    if get_os_version().startswith(CENTOS5):
        # os.system('/etc/init.d/salt-minion stop')  #该命令不能把所有的salt停止
        kill_salt_centos5()
    elif get_os_version().startswith(CENTOS6) or get_os_version().startswith(CENTOS7):
        os.system('/usr/local/saltstack/python2.7/bin/python /usr/local/saltstack/salt_operate.py stop')
    logging.info('stopping salt-minion')
    time.sleep(SLEEPTIME)


'''启动salt-minion'''
def start_salt():
    start_salt_normal()
    if is_salt_running():
        logging.info('salt-minion has been restarted')
    else:
        logging.error('salt-minion has not been restarted!')
        report_to_group("salt-minion has not been restarted: %s" % HOSTNAME , "salt-minion has not been restarted: %s" % HOSTNAME)


'''停止salt-minion'''
def stop_salt():
    stop_salt_normal()
    if is_salt_stopped():
        logging.info('stopped salt-minion')
    else:
        kill_salt()
        if is_salt_stopped():
            logging.info("stopped salt-minion through 'kill -9'")
        else:
            logging.error('salt-minion could not been killed!')
            report_to_group("salt-minion could not been killed: %s" % HOSTNAME, "salt-minion could not been killed: %s" % HOSTNAME)


'''判断当前时间（hour）是否达到重启条件'''
def is_restart_hour():
    hour = datetime.datetime.now().strftime('%H')
    if int(hour) == RESTART_HOUR:
        return True
    return False


'''通过redis的标志位来判断是否需要立即重启'''
def is_restart_rightnow():
    restart_flag = ''
    try:
        redis_client = redis.Redis(host=REDIS_SERVER, port=REDIS_PORT, db=0)
        restart_flag = redis_client.get('salt_minion_restart')
    except:
        logging.warn('can not connect redis_server: %s', traceback.print_exc())
        report_to_group("%s can not connect redis_server" % HOSTNAME, "can not connect redis_server: %s" % traceback.print_exc())
    if not restart_flag is None and restart_flag.lower() == 'true':
        return True
    return False


def main():
    '''根据是否立即重启和是否达到重启时间来判断是否需要重启salt-minion'''
    if is_restart_rightnow() or is_restart_hour():
        stop_salt()
        start_salt()


if __name__ == "__main__":
    main()