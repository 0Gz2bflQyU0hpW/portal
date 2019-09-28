/usr/local/spark-2.0.1-client-bin-2.5.0-cdh5.3.2/bin/spark-submit \
    --name PlayLogStatistic \
    --master yarn \
    --deploy-mode client \
    --driver-cores 1 \
    --driver-memory 1g \
    --num-executors 100 \
    --executor-cores 1 \
    --executor-memory 2g \
    --queue hive \
    --class com.weibo.dip.data.platform.datacubic.youku.PlayLogStatistic \
    /data0/xiaoyu/portal/datacubic/target/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar 
