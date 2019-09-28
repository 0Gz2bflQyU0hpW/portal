/usr/local/spark-2.0.1-bin-2.5.0-cdh5.3.2/bin/spark-submit \
    --name AliyunOSS \
    --master yarn \
    --deploy-mode client \
    --driver-cores 1 \
    --driver-memory 1g \
    --num-executors 10 \
    --executor-cores 1 \
    --executor-memory 2g \
    --queue hive \
    --class com.weibo.dip.data.platform.datacubic.batch.aliyunoss.AliyunOSS \
    /data0/workspace/portal-aliyunoss-v1/datacubic/target/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar >> /var/log/data-platform/aliyunoss-v1.log 2>&1 &
