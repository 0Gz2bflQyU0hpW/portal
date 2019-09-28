#!/usr/bin/env python
# encoding=utf-8

import json
import ConfigParser
import logging
import traceback
import watchalert
from dateutil import *
from getdatautil import *
from tokafkautil import *
from parsedatautil import *

date = subday(1,0) 
job_date = subday(0,1)
projects = ['platform','video-total','video-client-b']
cf = ConfigParser.ConfigParser()
cf.read("/data0/workspace/datacubic/src/main/resources/dm.conf")

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
                    filename='/var/log/data-platform/fulllink_dm.log',
                    filemode='a')


'''定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象'''
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def main():

    pd = Kafka_Producer()

    pd.getConnect()
    
    for i in range(len(projects)):

        try:
	        datas = selectJobData(cf.get(projects[i],'job'),cf.get(projects[i],'job_select'),job_date)
       	    lines = datas.split('\n')
  	    
            for j in range(len(lines)):
                get_statistics_bygroup = parseRow(eval(cf.get(projects[i],'rowdefines')),lines[j],cf.get(projects[i],'business'),date)
                pd.sendjsondata(get_statistics_bygroup)
   
        except:
            subject = projects[i] + '(' + str(i+1) + '/' + str(len(projects)) + ')'
	        content = traceback.format_exc()
	        logging.info(traceback.format_exc())
        
            watchalert.sendAlertToUsers(
        	'fulllink',
		    'dm',
		    subject,
		    content,
		    "qianqian25",
		    True,
		    True,
		    False)
    
    pd.disConnect()
    
if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()

