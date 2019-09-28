#!/usr/bin/python
# -r- coding: UTF-8 -*-

import salt.client
import traceback
import logging
import datetime
from commons import watchalert

'''
author: jianhong1
date: 2017-09-08
'''

scribe_group = [
'tc142056.scribe.dip.sina.com.cn',
'tc142058.scribe.dip.sina.com.cn',
's052025.tc.ss.dip.sina.com.cn',
's052026.tc.ss.dip.sina.com.cn',
's052027.tc.ss.dip.sina.com.cn',
'bx001121.scribe.dip.sina.com.cn',
'bx001122.scribe.dip.sina.com.cn',
'bx002133.scribe.dip.sina.com.cn',
'bx002134.scribe.dip.sina.com.cn',
'yf140066.scribe.dip.sina.com.cn',
'yf140083.scribe.dip.sina.com.cn',
#'yf140084.scribe.dip.sina.com.cn',  #salt时好时坏
'yf234085.scribe.dip.sina.com.cn',
'yf234092.scribe.dip.sina.com.cn',
'admin.aer.dip.sina.com.cn',
'ja108108.load.dip.sina.com.cn',
'ja108068.load.dip.sina.com.cn',
'ja078021.load.dip.sina.com.cn',
'ja078083.load.dip.sina.com.cn',
'yf235027.scribe.dip.sina.com.cn',
'yf235235.scribe.dip.sina.com.cn']


THRESHOLD_VALUE = 128


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/falcon/scribe-cache-monitor.log',
                    filemode='a')


'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


'''发送报警给用户组,并打印日志'''
def report_to_group(subservice, subject, content):
    watchalert.sendAlertToGroups("databus", subservice, subject, content, "DIP_ALL", True, True, False)
    logging.info(content)


def main():
    local_client = salt.client.LocalClient()
    for scribe_node in scribe_group:
        now = datetime.datetime.now()
        std_now = now.strftime('%Y-%m-%d %H:%M:%S')
        subservice = "scribe"

        '''节点无响应抛出异常'''
        try:
            scribe_cache = local_client.cmd(scribe_node, 'cmd.run', ['du -sh /data1/scribe_cache'])
            scribe_cache_size = scribe_cache[scribe_node].split("\t")[0]
        except:
            subject = "scribe节点没有响应, scribe node: %s %s" % (scribe_node, std_now)
            content = "scribe节点没有响应, scribe node: %s %s \n %s" % (scribe_node, std_now, traceback.format_exc())
            report_to_group(subservice, subject, content)
            continue

        subject = "scribe 堆积: %s, threshold: %sM, scribe node: %s" % (scribe_cache_size, THRESHOLD_VALUE, scribe_node)
        content = "scribe 堆积。 the size of '/data1/scribe_cache': %s, threshold: %sM。 scribe node: %s, %s" % (scribe_cache_size, THRESHOLD_VALUE, scribe_node, std_now)

        '''当scribe_cache大小超过128M时报警'''
        if scribe_cache_size.find("G") != -1 or scribe_cache_size.find("T") != -1:
            report_to_group(subservice, subject, content)
            continue
        elif scribe_cache_size.find("M") != -1:
            number = scribe_cache_size.replace("M", "")
            if float(number) > THRESHOLD_VALUE:
                report_to_group(subservice, subject, content)
                continue

        '''正常工作打印日志'''
        logging.info("scribe node: %s, the size of '/data1/scribe_cache': %s, %s, work normal" % (scribe_node, scribe_cache_size, std_now))


if __name__ == "__main__":
    main()