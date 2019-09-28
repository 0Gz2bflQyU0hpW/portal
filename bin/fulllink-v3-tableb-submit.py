import sys
import datetime
import os

app_name = "PerformanceMetricAggregationV3TableB"

driver_cores = "1"
driver_memory = "2g"

executor_nums = "30"
executor_cores = "1"
executor_mems = "2g"

class_name = "com.weibo.dip.data.platform.datacubic.fulllink.PerformanceMetricAggregationV3TableB"

jar_path = "/data0/workspace/portal-fulllink-v3/datacubic/target/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar"

today = datetime.datetime.now()

yesterday = (today - datetime.timedelta(days=1)).strftime("%Y%m%d")

day = yesterday

log_dir = "/var/log/data-platform"

command = """\
/usr/local/spark-2.0.1-bin-2.5.0-cdh5.3.2/bin/spark-submit \
    --name {app_name} \
    --master yarn \
    --deploy-mode client \
    --driver-cores {driver_cores} \
    --driver-memory {driver_memory} \
    --driver-java-options "-Duser.dir=/tmp/metastore/{app_name}" \
    --num-executors {executor_nums} \
    --executor-cores {executor_cores} \
    --executor-memory {executor_mems} \
    --queue hive \
    --class {class_name} \
    {jar_path} {day} > {log_dir}/{app_name}-{day}.log 2>&1 &
"""


if __name__ == "__main__":
    if(len(sys.argv) == 2):
        day = sys.argv[1]

    command = command.format(app_name=app_name, driver_cores=driver_cores, driver_memory=driver_memory,
                             executor_nums=executor_nums, executor_cores=executor_cores, executor_mems=executor_mems, class_name=class_name, jar_path=jar_path, day=day, log_dir=log_dir)

    os.system(command)
