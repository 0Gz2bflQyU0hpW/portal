#!/bin/sh
nohup /usr/local/spark-2.2.0-bin-hadoop2.8.2/bin/spark-submit \
    --name SummonAgg \
    --master yarn \
    --deploy-mode cluster \
    --driver-cores 1 \
    --driver-memory 2g \
    --num-executors 4 \
    --executor-cores 1 \
    --executor-memory 2g \
    --queue hive \
    --conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2 \
    --conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=registry.api.weibo.com/dip/nyx-hadoop:2.8.2 \
    --class com.weibo.dip.data.platform.datacubic.business.SummonAgg \
     /data0/liyang28/summonagg/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar "dip-cdn-client-video-aggregation,daily">  /data0/liyang28/summonagg/agg.log 2>&1 &