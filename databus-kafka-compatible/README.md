Pipeline相互之前不影响，包括：启动、停止、重启、新增、删除；

1.部署databus-kafka-compatible(单台服务器)
(1)在10.13.4.44编译打包(root用户)
cd /data0/jianhong1/git-repo/portal
git pull origin develop
mvn clean package -pl databus-kafka-compatible -am

(2)停止程序
echo "" > /data0/workspace/portal/databus-kafka-compatible/pipelines/stop.file
或
ps aux | grep DatabusDriver | grep -v grep | awk '{print $2}' | xargs kill

(3)把编译好的databus-kafka-compatible拷贝到服务器
mv /data0/workspace/portal /data0/workspace/portal.v2    #备份最近的portal，注意修改版本号
rsync 10.13.4.44::move/data0/jianhong1/git-repo/portal /data0/workspace -arv

(4)拷贝pipeline配置文件
cp /data0/workspace/portal.v2/databus-kafka-compatible/pipelines /data0/workspace/portal/databus-kafka-compatible/ -r

(5)启动程序
rm -rf /data0/workspace/portal/databus-kafka-compatible/pipelines/stop.file
sudo -u hdfs /data0/datalinker/jdk1.8.0_05/bin/java -Xms1G -Xmx4G -cp /data0/workspace/portal/databus-kafka-compatible/hadoop-conf-nyx/:/data0/workspace/portal/databus-kafka-compatible/conf/:/data0/workspace/portal/databus-kafka-compatible/target/databus-kafka-compatible-2.0.0.jar:/data0/workspace/portal/databus-kafka-compatible/target/databus-kafka-compatible-2.0.0-lib/* com.weibo.dip.databus.DatabusDriver > /dev/null 2>&1 &
tail -f /var/log/data-platform/databus-kafka-compatible.log  #查看日志
sar -n DEV 1 1

-------------------------------------

2.部署databus-kafka-compatible(多台服务器)
(1)把服务器已经部署的databus-kafka-compatible拷贝到10.13.4.44
rsync /data0/workspace/portal 10.13.4.44::move//data0/jianhong1/rsync-dir -arv

(2)停止程序
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "echo '' > /data0/workspace/portal/databus-kafka-compatible/pipelines/stop.file"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ps aux | grep databus-kafka-compatible"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "tail -n 20 /var/log/data-platform/databus-kafka-compatible.log"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "rm -f /data0/workspace/portal/databus-kafka-compatible/pipelines/stop.file"   #确定程序停止后在删除文件

(3)把编译好的databus-kafka-compatible下发到各个服务器
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ls /data0/workspace/"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "mv /data0/workspace/portal /data0/workspace/portal.v2"   #备份最近的portal，注意修改版本号
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "rsync 10.13.4.44::move/data0/jianhong1/rsync-dir/portal /data0/workspace -arv"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ls -l /data0/workspace/portal/databus-kafka-compatible/pipelines"  #查看日期确认是否最新

(4)启动程序
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "sudo -u hdfs /data0/datalinker/jdk1.8.0_05/bin/java -Xms1G -Xmx4G -cp /data0/workspace/portal/databus-kafka-compatible/hadoop-conf-nyx/:/data0/workspace/portal/databus-kafka-compatible/conf/:/data0/workspace/portal/databus-kafka-compatible/target/databus-kafka-compatible-2.0.0.jar:/data0/workspace/portal/databus-kafka-compatible/target/databus-kafka-compatible-2.0.0-lib/* com.weibo.dip.databus.DatabusDriver > /dev/null 2>&1 &"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "ps aux | grep databus-kafka-compatible"
/usr/local/saltstack/python2.7/bin/salt -L "d216096.eos.dip.sina.com.cn" cmd.run "sar -n DEV 1 1"

-------------------------------------

3.databus-kafka-compatible添加配置文件
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
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cp.get_file salt://databus/kafka-to-hdfs-test.properties /data0/workspace/portal/databus-kafka-compatible/pipelines/kafka-to-hdfs-test.properties

(3)确认下是否拷贝
/usr/local/saltstack/python2.7/bin/salt -L "d216103.eos.dip.sina.com.cn,d216093.eos.dip.sina.com.cn" cmd.run "ls /data0/workspace/portal/databus-kafka-compatible/pipelines"