1 功能说明
把kafka_0.10.2.1的topic数据落地HDFS

2 部署方式（10.13.4.44）

2.1 编译(root用户)
sudo -s
cd /data0/jianhong1/git-repo/portal
git pull origin develop
mvn clean package -pl databus-kafka -am

2.2 把portal拷贝到新目录
mv /data0/jianhong1/kafka2hdfs/portal /data0/jianhong1/kafka2hdfs/portal.bak
cp /data0/jianhong1/git-repo/portal /data0/jianhong1/kafka2hdfs/ -r

2.3 启动程序
cd /data0/jianhong1/kafka2hdfs/portal
su hdfs
第一个参数为启动线程个数（默认为3），第二个参数为checkFile路径（默认为databus-kafka/stopped-task/modifyme）
落地到生产集群nyx的HDFS运行实例
sudo -u hdfs java -cp /data0/jianhong1/kafka2hdfs/portal/databus-kafka/hadoop-conf-nyx:/data0/jianhong1/kafka2hdfs/portal/databus-kafka/target/databus-kafka-2.0.0-SNAPSHOT.jar:/data0/jianhong1/kafka2hdfs/portal/databus-kafka/target/lib/* com.weibo.dip.databus.kafka.KafkaToHDFS > /dev/null 2>&1 &   #使用默认参数
落地到测试集群的HDFS运行实例
sudo -u hdfs java -cp /data0/jianhong1/kafka2hdfs/portal/databus-kafka/test-cluster-hadoop-conf:/data0/jianhong1/kafka2hdfs/portal/databus-kafka/target/databus-kafka-2.0.0-SNAPSHOT.jar:/data0/jianhong1/kafka2hdfs/portal/databus-kafka/target/lib/* com.weibo.dip.databus.kafka.KafkaToHDFS > /dev/null 2>&1 &    #使用默认参数

2.3 停止程序
修改databus-kafka/stopped-task/modifyme文件的内容，即可停止