1 com.weibo.dip.data.platform.falcon.kafka.metric功能说明
采集kafka的 topic/broker/partition metric信息

2 部署（10.13.4.44）

2.1 编译(root用户)
cd /data0/jianhong1/git-repo/portal
git pull origin develop
mvn clean package -pl falcon -am

2.2 备份上个版本
mv /data0/jianhong1/portal-kafka-metric-collect /data0/jianhong1/portal-kafka-metric-collect.TIMESTAMP
cp ../portal /data0/jianhong1/portal-kafka-metric-collect -r

2.3 启动程序(portal目录下)
java -cp falcon/kafka-metric-collect-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.metric.KafkaMetricCollectionMain
tailf /var/log/falcon/kafka-metric-collect.log

3 部署cron
cat /etc/cron.d/kafka-metric-collect
*/1 * * * * root cd /data0/jianhong1/portal-kafka-metric-collect && java -cp falcon/kafka-metric-collect-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.metric.KafkaMetricCollectionMain > /dev/null 2>&1


=============================
1 com.weibo.dip.data.platform.falcon.kafka.replica功能说明
UnderReplicatedPartitions监控报警

2 部署（10.13.4.44）

2.1 编译(root用户)
cd /data0/jianhong1/git-repo/portal
git pull origin develop
mvn clean package -pl falcon -am

2.2 备份上个版本
mv /data0/jianhong1/portal-kafka-replica /data0/jianhong1/portal-kafka-replica.TIMESTAMP
cp ../portal /data0/jianhong1/portal-kafka-replica -r

2.3 启动程序
cd /data0/jianhong1/portal-kafka-replica && java -cp falcon/kafka-monitor-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.replica.UnderReplicatedMonitorMain > /dev/null 2>&1

3 部署cron
cat /etc/cron.d/kafka-monitor
*/10 * * * * root cd /data0/jianhong1/portal-kafka-replica && java -cp falcon/kafka-monitor-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.replica.UnderReplicatedMonitorMain > /dev/null 2>&1

---------------------------
#### 1 功能说明 

    PreferredReplicaImbalanceCount监控报警

#### 2 编译启动（10.13.4.44）

- 2.1 编译(root用户)

    cd /data0/jianhong1/git-repo/portal

    git pull origin develop

    mvn clean package -pl falcon -am

    cp ../portal /data0/jianhong1/portal-kafka-replica -r

- 2.2 启动程序

     cd /data0/jianhong1/portal-kafka-replica && java -cp falcon/kafka-monitor-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.replica.PreferredReplicaImbalanceMonitorMain > /dev/null 2>&1

#### 3 部署cron
    cat /etc/cron.d/kafka-monitor

    */10 * * * * root cd /data0/jianhong1/portal-kafka-replica && java -cp falcon/kafka-monitor-conf/:falcon/target/falcon-2.0.0-lib/*:falcon/target/falcon-2.0.0.jar com.weibo.dip.data.platform.falcon.kafka.replica.PreferredReplicaImbalanceMonitorMain > /dev/null 2>&1
