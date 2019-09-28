#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

'''
author: jianhong1
date: 2017-10-25
'''


def get_consumers_info():
    # 打开数据库连接
    db = MySQLdb.connect("10.13.56.31","aladdin","aladdin*admin","dip_data_analyze" )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    sql = "SELECT * FROM dataplatform_consumer"
    list = []
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            topic_name = row[1]
            consumer_group = row[3]
            contact_person = row[4]
            cursor.execute("select qps from dataplatform_topic where topic_name = '%s'" % topic_name)
            qps = cursor.fetchall()[0][0]
            dict = {}
            dict['topic_name'] = topic_name
            dict['consumer_group'] = consumer_group
            dict['contact_person'] = contact_person
            dict['qps'] = qps
            list.append(dict)
    except:
        print 'mysqlclient unable fetch data'
        raise
    finally:
        # 关闭数据库连接
        db.close()
    return list

if __name__ == "__main__":
    list = get_consumers_info()
    print "the length of list: %s" % len(list)
    for element in list:
        print element