import os

app_name = "CDNRAW"

driver_cores = "1"
driver_memory = "2g"

executor_nums = "150"
executor_cores = "1"
executor_mems = "2g"

class_name = "com.weibo.dip.data.platform.datacubic.business.CDNRAW"

jar_path = "/data0/workspace/cdn/datacubic/target/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar"

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
    {jar_path} > {log_dir}/{app_name}.log 2>&1 &
"""


if __name__ == "__main__":

    command = command.format(app_name=app_name, driver_cores=driver_cores, driver_memory=driver_memory,
                             executor_nums=executor_nums, executor_cores=executor_cores, executor_mems=executor_mems, class_name=class_name, jar_path=jar_path, log_dir=log_dir)

    os.system(command)
