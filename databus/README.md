Pipeline相互之前不影响，包括：启动、停止、重启、新增、删除；

1.单台部署databus
(1)在10.13.4.44编译打包(root用户)
cd /data0/jianhong1/git-repo/portal
git pull origin develop
mvn clean package -pl databus -am

(2)停止程序
echo "" > /data0/workspace/portal/databus/pipelines/stop.file
或
ps aux | grep DatabusDriver | grep -v grep | awk '{print $2}' | xargs kill

(3)把编译好的databus拷贝到服务器
mv /data0/workspace/portal /data0/workspace/portal.v2    #备份最近的portal，注意修改版本号
rsync 10.13.4.44::move/data0/jianhong1/git-repo/portal /data0/workspace -arv

(4)拷贝pipeline配置文件
cp /data0/workspace/portal.v2/databus/pipelines /data0/workspace/portal/databus/ -r

(5)启动程序
rm -rf /data0/workspace/portal/databus/pipelines/stop.file
数据同步到hadoop nyx集群
sudo -u hdfs /data0/datalinker/jdk1.8.0_05/bin/java -Xms1G -Xmx4G -cp /data0/workspace/portal/databus/hadoop-conf-nyx/:/data0/workspace/portal/databus/conf/:/data0/workspace/portal/databus/target/databus-2.0.0.jar:/data0/workspace/portal/databus/target/databus-2.0.0-lib/* com.weibo.dip.databus.DatabusDriver > /dev/null 2>&1 &
数据同步到hadoop eos集群
sudo -u hdfs /data0/datalinker/jdk1.8.0_05/bin/java -Xms1G -Xmx4G -cp /data0/workspace/portal/databus/hadoop-conf-eos/:/data0/workspace/portal/databus/conf/:/data0/workspace/portal/databus/target/databus-2.0.0-SNAPSHOT.jar:/data0/workspace/portal/databus/target/databus-2.0.0-SNAPSHOT-lib/* com.weibo.dip.databus.DatabusDriver > /dev/null 2>&1 &
注意：不同的hadoop集群要选择不同的hadoop-conf配置,如:hadoop-conf-eos 和hadoop-conf-nyx
tail -f /var/log/data-platform/databus.log  #查看日志
sar -n DEV 1 1

注意：
需要先创建日志目录
mkdir /var/log/data-platform
chmod 777 /var/log/data-platform
vim /etc/cron.d/databus-log-clean
32 14 */1 * * root find /var/log/data-platform/ -type f -mtime +6 | xargs rm -f

-------------------------------------

2.多台部署databus(salt master服务器: master.salt.intra.dip.weibo.com)
(1)把服务器已经部署的databus拷贝到10.13.4.44
rsync /data0/workspace/portal 10.13.4.44::move//data0/jianhong1/rsync-dir -arv

(2)停止程序
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "echo '' > /data0/workspace/portal/databus/pipelines/stop.file"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ps aux | grep databus"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "tail -n 20 /var/log/data-platform/databus.log"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "rm -f /data0/workspace/portal/databus/pipelines/stop.file"   #确定程序停止后在删除文件

(3)把编译好的databus下发到各个服务器
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ls /data0/workspace/"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "mv /data0/workspace/portal /data0/workspace/portal.v2"   #备份最近的portal，注意修改版本号
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "rsync 10.13.4.44::move/data0/jianhong1/rsync-dir/portal /data0/workspace -arv"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ls -l /data0/workspace/portal/databus/pipelines"  #查看日期确认是否最新

(4)启动程序
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "sudo -u hdfs /data0/datalinker/jdk1.8.0_05/bin/java -Xms1G -Xmx4G -cp /data0/workspace/portal/databus/hadoop-conf-nyx/:/data0/workspace/portal/databus/conf/:/data0/workspace/portal/databus/target/databus-2.0.0.jar:/data0/workspace/portal/databus/target/databus-2.0.0-lib/* com.weibo.dip.databus.DatabusDriver > /dev/null 2>&1 &"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ps aux | grep databus"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "sar -n DEV 1 1"

-------------------------------------

3.databus添加配置文件
(1)登录salt Master服务器(master.salt.intra.dip.weibo.com)
在/srv/salt/databus编辑pipeline配置文件 kafka-to-hdfs-test.properties
pipeline配置文件例子:
vim pipelines/kafka-to-hdfs-test.properties
pipeline.name=kafka-to-hdfs-test

pipeline.source=com.weibo.dip.databus.source.KafkaSourceV2
pipeline.converter=com.weibo.dip.databus.converter.TopicNameConverter
pipeline.store=com.weibo.dip.databus.store.DefaultStore
pipeline.sink=com.weibo.dip.databus.sink.DIPHDFSSinkV2

#source
zookeeper.connect=first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001
group.id=dip
topic.and.threads=test:2,yurun_1:1

topic.mappings=test:test1246,yurun_1:test2112

#sink
compression=gz

(2)拷贝pipeline配置文件到各个minion节点(salt:/ 映射目录 /srv/salt)
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cp.get_file salt://databus/kafka-to-hdfs-test.properties /data0/workspace/portal/databus/pipelines/kafka-to-hdfs-test.properties

(3)确认下是否拷贝
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cmd.run "ls /data0/workspace/portal/databus/pipelines"


-------------------------------------

3.databus删除配置文件
(1)备份pipeline
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cmd.run "cp /data0/workspace/portal/databus/pipelines /data0/workspace/databus-pipelines.TIMESTAMP -r"

(2)删除配置文件
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cmd.run "rm /data0/workspace/portal/databus/pipelines/kafka-to-hdfs-test.properties"

(3)确认下是否删除
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cmd.run "ls /data0/workspace/portal/databus/pipelines"

-------------------------------------

salt-key:
d216093.eos.dip.sina.com.cn
d216094.eos.dip.sina.com.cn
d216095.eos.dip.sina.com.cn
d216096.eos.dip.sina.com.cn
d216097.eos.dip.sina.com.cn
d216098.eos.dip.sina.com.cn
d216103.eos.dip.sina.com.cn
d077114138.dip.weibo.com
d077114139.dip.weibo.com
d216117.eos.dip.sina.com.cn


==========================================================

1 com.weibo.dip.databus.kafka.ConsumeTestMain功能说明
测试消费kafka_0.8.2.1的实例

2 部署方式（10.13.4.44）

2.1 编译(root用户)
rsync /Users/jianhong1/git-workspace/portal 10.13.4.44::move//data0/jianhong1/workspace/ -arv

cd /data0/jianhong1/workspace/portal
mvn clean package -pl databus -am

2.2 启动程序
java -cp databus/target/databus-2.0.0.jar:databus/target/databus-2.0.0-lib/*:databus/target/test-classes/ com.weibo.dip.databus.kafka.ConsumeTestMain > /dev/null 2>&1 &
java -cp databus/target/databus-2.0.0.jar:databus/target/databus-2.0.0-lib/*:databus/target/test-classes/ com.weibo.dip.databus.kafka.ProduceTestMain > /dev/null 2>&1 &
tailf /var/log/databus/kafka-consumer.log