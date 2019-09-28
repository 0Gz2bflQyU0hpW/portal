#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import datetime
import os

driver_cores = "1"
driver_memory = "2g"

executor_nums = "24"
executor_cores = "1"
executor_mems = "2g"

class_name = "com.weibo.dip.data.platform.datacubic.business.SummonAgg"

jar_path = "/data0/liyang28/summonagg/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar"

today = datetime.datetime.now()

yesterday = (today - datetime.timedelta(days=1)).strftime("%Y%m%d")

day = yesterday

log_dir = "/data0/liyang28/summonagg/logs"

command = """\
/usr/local/spark-2.2.0-bin-hadoop2.8.2/bin/spark-submit \
    --name {app_name} \
    --master yarn \
    --deploy-mode cluster \
    --driver-cores {driver_cores} \
    --driver-memory {driver_memory} \
    --num-executors {executor_nums} \
    --executor-cores {executor_cores} \
    --executor-memory {executor_mems} \
    --queue hive \
    --class {class_name} \
    {jar_path} {business_agg} > {log_dir}/{app_name}-{day}.log 2>&1 &
"""

def connectMysql():
  # 打开数据库连接
  db = MySQLdb.connect(host="d136092.innet.dip.weibo.com", port=3307, db="portal", user="root", passwd="mysqladmin",
                       charset='utf8')

  # 使用cursor()方法获取操作游标
  cursor = db.cursor()

  sql = "select business, agg_type from summon_agg_config"
  all_business = []
  try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    for row in results:
      business = row[0]
      aggtype = row[1]
      all_agg_business = business + ',' + aggtype
      all_business.append(all_agg_business)
  except:
    print "Error: unable to fecth data"
  # 关闭数据库连接
  db.close()
  print(all_business)
  return all_business

if __name__ == "__main__":
  all_business_agg = connectMysql()

  for business_agg in all_business_agg:
    app_name = business_agg
    command = command.format(app_name=app_name, driver_cores=driver_cores, driver_memory=driver_memory,
                             executor_nums=executor_nums, executor_cores=executor_cores,
                             executor_mems=executor_mems, class_name=class_name, jar_path=jar_path, day=day,
                             log_dir=log_dir,business_agg=business_agg)
    os.system(command)






