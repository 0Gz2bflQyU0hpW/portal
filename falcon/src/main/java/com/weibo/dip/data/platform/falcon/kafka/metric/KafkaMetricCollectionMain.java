package com.weibo.dip.data.platform.falcon.kafka.metric;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.weibo.dip.data.platform.commons.metric.Metric;
import com.weibo.dip.data.platform.commons.metric.MetricStore;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class KafkaMetricCollectionMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricCollectionMain.class);

    private static final long TIMESTAMP = System.currentTimeMillis()/60000*60000;
    private static ArrayList clusterList;

    static{
        try {
            Reader reader = new BufferedReader(new InputStreamReader(KafkaMetricCollectionMain.class.getClassLoader().getResourceAsStream("kafka-cluster.json")));
            LinkedTreeMap map = new Gson().fromJson(reader, LinkedTreeMap.class);
            clusterList = (ArrayList)map.get("kafkaCluster");
            LOGGER.info("cluster number:{}", clusterList.size());
        } catch (Exception e) {
            throw new ExceptionInInitializerError("load properties file error" + ExceptionUtils.getFullStackTrace(e));
        }
    }


    // get metrics of every broker
    public static class IpWorker implements Runnable{
        private LinkedBlockingQueue<String> queue;
        private JmxOperation jmxOperation;
        private String clusterName;

        public IpWorker(LinkedBlockingQueue<String> queue, JmxOperation jmxOperation, String clusterName){
            this.queue = queue;
            this.jmxOperation = jmxOperation;
            this.clusterName = clusterName;
        }

        @Override
        public void run() {
            while (!queue.isEmpty()) {
                String ip = null;
                try {
                    ip = queue.poll(1, TimeUnit.SECONDS);
                    if (ip == null) {
                        continue;
                    }
                    //kafka broker
                    storeBrokerMetric(ip);
                    //kafka jvm
                    storeJvmMetric(ip);
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("get BrokerIp error, BrokerIp={} \n{}", ip, ExceptionUtils.getFullStackTrace(e));
                } catch (MalformedObjectNameException | AttributeNotFoundException | MBeanException | ReflectionException | InstanceNotFoundException | IOException e) {
                    LOGGER.error("get metrics of broker error, BrokerIp={} \n{}", ip, ExceptionUtils.getFullStackTrace(e));
                } catch (Exception e) {
                    LOGGER.error("store broker metrics error, BrokerIp={} \n{}", ip, ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

        public void storeBrokerMetric(String ip) throws Exception {
            String business = "dip-kafka-broker-monitor";

            Metric<String> clusterInfo = Metric.newEntity("cluster", clusterName);
            Metric<String> brokerIp = Metric.newEntity("broker", ip);

            Metric<Float> messagesInPerSec = Metric.newEntity("MessagesInPerSec", jmxOperation.getBrokerAttribute(ip, "kafka.server", "BrokerTopicMetrics", "MessagesInPerSec", "OneMinuteRate").floatValue());
            Metric<Float> bytesInPerSec = Metric.newEntity("BytesInPerSec", jmxOperation.getBrokerAttribute(ip, "kafka.server", "BrokerTopicMetrics", "BytesInPerSec", "OneMinuteRate").floatValue());
            Metric<Float> bytesOutPerSec = Metric.newEntity("BytesOutPerSec", jmxOperation.getBrokerAttribute(ip, "kafka.server", "BrokerTopicMetrics", "BytesOutPerSec", "OneMinuteRate").floatValue());

            Metric<Long> leaderCount = Metric.newEntity("LeaderCount", jmxOperation.getBrokerAttribute(ip, "kafka.server", "ReplicaManager", "LeaderCount", "Value").longValue());
            Metric<Long> partitionCount = Metric.newEntity("PartitionCount", jmxOperation.getBrokerAttribute(ip, "kafka.server", "ReplicaManager", "PartitionCount", "Value").longValue());
            Metric<Long> underReplicated = Metric.newEntity("UnderReplicatedPartitions", jmxOperation.getBrokerAttribute(ip, "kafka.server", "ReplicaManager", "UnderReplicatedPartitions", "Value").longValue());

            Metric<Long> activeControllerCount = Metric.newEntity("ActiveControllerCount", jmxOperation.getBrokerAttribute(ip, "kafka.controller","KafkaController", "ActiveControllerCount", "Value").longValue());
            Metric<Long> offlinePartitionsCount = Metric.newEntity("OfflinePartitionsCount", jmxOperation.getBrokerAttribute(ip, "kafka.controller","KafkaController", "OfflinePartitionsCount", "Value").longValue());
            Metric<Long> preferredReplicaImbalance = Metric.newEntity("PreferredReplicaImbalanceCount", jmxOperation.getBrokerAttribute(ip, "kafka.controller","KafkaController", "PreferredReplicaImbalanceCount", "Value").longValue());

            Metric<Float> requestHandlerAvgIdlePercent = Metric.newEntity("RequestHandlerAvgIdlePercent", jmxOperation.getBrokerAttribute(ip, "kafka.server", "KafkaRequestHandlerPool", "RequestHandlerAvgIdlePercent", "OneMinuteRate").floatValue());



            MetricStore.store(business, TIMESTAMP, clusterInfo, brokerIp, messagesInPerSec, bytesInPerSec, bytesOutPerSec,
                leaderCount, partitionCount, underReplicated, activeControllerCount, offlinePartitionsCount, preferredReplicaImbalance, requestHandlerAvgIdlePercent
               );
        }

        /**
        * @Description:  kafka jvm 监控指标数据采集
        * @Date: 下午4:42 2018/9/19
        * @Param: [ip]
        * @return: void
        */
        public void storeJvmMetric(String ip) throws Exception {
            String business = "dip-kafka-jvm-monitor";
            double MB=1024*1024*1.0;

            Metric<String> clusterInfo = Metric.newEntity("cluster", clusterName);
            Metric<String> brokerIp = Metric.newEntity("broker", ip);
            // JVM Collection
            CompositeData fullGcData=jmxOperation.getCompositeDataAttribute(ip,"java.lang","GarbageCollector","ConcurrentMarkSweep","LastGcInfo");
            long jst=jmxOperation.getBrokerAttribute(ip, "java.lang", "Runtime", "", "StartTime").longValue();
            long id = (long) fullGcData.get("id");
            long duration = (long) fullGcData.get("duration");
            long startTime = (long) fullGcData.get("startTime")+jst;
            long endTime = (long) fullGcData.get("endTime")+jst;
            SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String st = sdf.format(new Date(startTime));
            String et = sdf.format(new Date(endTime));

            Metric<Long> fullGcId = Metric.newEntity("FullGcId",id);
            Metric<Long> fullGcDuration = Metric.newEntity("FullGcDuration", duration);
            Metric<String> fullGcStartTime = Metric.newEntity("FullGcStartTime1", st);
            Metric<String> fullGcEndTime = Metric.newEntity("FullGcEndTime1",et );

            Metric<Long> fullGcCollectionCount = Metric.newEntity("FullGcCollectionCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "GarbageCollector", "ConcurrentMarkSweep", "CollectionCount").longValue());
            Metric<Long> fullGcCollectionTime = Metric.newEntity("FullGcCollectionTime", jmxOperation.getBrokerAttribute(ip, "java.lang", "GarbageCollector", "ConcurrentMarkSweep", "CollectionTime").longValue());
            Metric<Long> youngGcCollectionCount = Metric.newEntity("YoungGcCollectionCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "GarbageCollector", "ParNew", "CollectionCount").longValue());
            Metric<Long> youngGcCollectionTime = Metric.newEntity("YoungGcCollectionTime", jmxOperation.getBrokerAttribute(ip, "java.lang", "GarbageCollector", "ParNew", "CollectionTime").longValue());
            //class load info
            Metric<Long> loadedClassCount = Metric.newEntity("LoadedClassCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "ClassLoading", "", "LoadedClassCount").longValue());
            Metric<Long> unloadedClassCount = Metric.newEntity("UnloadedClassCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "ClassLoading", "", "UnloadedClassCount").longValue());
            // thread info
            Metric<Long> activeThreadCount = Metric.newEntity("ActiveThreadCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "Threading", "", "ThreadCount").longValue());
            Metric<Long> peakThreadCount = Metric.newEntity("PeakThreadCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "Threading", "", "PeakThreadCount").longValue());
            //os info
            Metric<Long> openFileCount = Metric.newEntity("OpenFileCount", jmxOperation.getBrokerAttribute(ip, "java.lang", "OperatingSystem", "", "OpenFileDescriptorCount").longValue());
            //memory info
            CompositeData heapData=jmxOperation.getCompositeDataAttribute(ip,"java.lang","Memory","","HeapMemoryUsage");
            long heap_used= (long) heapData.get("used");
            long heap_committed= (long) heapData.get("committed");
            float heap_percent= (float) (heap_used/(heap_committed*1.0));
            Metric<Float> heapUsed = Metric.newEntity("HeapMemoryUsed", (float) (heap_used/MB));
            Metric<Float> heapUsedPercent = Metric.newEntity("HeapMemoryUtilization", heap_percent);

            CompositeData nonHeapData=jmxOperation.getCompositeDataAttribute(ip,"java.lang","Memory","","NonHeapMemoryUsage");
            long non_used= (long) nonHeapData.get("used");
            long non_committed= (long) nonHeapData.get("committed");
            float non_percent= (float) (non_used/(non_committed*1.0));
            Metric<Float> nonHeapUsed = Metric.newEntity("NonHeapMemoryUsed", (float) (non_used/MB));
            Metric<Float> nonHeapUsedPercent = Metric.newEntity("NonHeapMemoryUtilization", non_percent);
            //cpu info
            Metric<Float> cpuLoad = Metric.newEntity("CpuLoad",jmxOperation.getBrokerAttribute(ip, "java.lang", "OperatingSystem", "", "ProcessCpuLoad").floatValue());

            MetricStore.store(business, TIMESTAMP,clusterInfo,brokerIp,fullGcCollectionCount, fullGcCollectionTime, youngGcCollectionCount, youngGcCollectionTime,loadedClassCount,unloadedClassCount,activeThreadCount,peakThreadCount,
                    fullGcId,fullGcDuration,fullGcStartTime,fullGcEndTime,openFileCount,heapUsed,heapUsedPercent,nonHeapUsed,nonHeapUsedPercent,cpuLoad);
        }
    }



    //get metrics of every topic
    public static class TopicWorker implements Runnable{
        private LinkedBlockingQueue<String> queue;
        private JmxOperation jmxOperation;
        private ZookeeperOperation zkOperation;
        private String clusterName;

        public TopicWorker(LinkedBlockingQueue<String> queue, JmxOperation jmxOperation, ZookeeperOperation zkOperation, String clusterName){
            this.queue = queue;
            this.jmxOperation = jmxOperation;
            this.zkOperation = zkOperation;
            this.clusterName = clusterName;
        }

        public void storeKafkaServerInfo(String topic, Set<String> ips, String category, String type) throws Exception {
            String business = "dip-kafka-monitor";

            Metric<String> clusterInfo = Metric.newEntity("cluster", clusterName);
            Metric<String> topicInfo = Metric.newEntity("topic", topic);

            Metric<Float> messagesInPerSec = Metric.newEntity("MessagesInPerSec", (float) jmxOperation.getAttribute(topic, ips, category, type, "MessagesInPerSec", "OneMinuteRate"));
            Metric<Float> bytesInPerSec = Metric.newEntity("BytesInPerSec", (float) jmxOperation.getAttribute(topic, ips, category, type, "BytesInPerSec", "OneMinuteRate"));
            Metric<Float> bytesOutPerSec = Metric.newEntity("BytesOutPerSec", (float) jmxOperation.getAttribute(topic, ips, category, type, "BytesOutPerSec", "OneMinuteRate"));

            MetricStore.store(business, TIMESTAMP, clusterInfo, topicInfo, messagesInPerSec, bytesInPerSec, bytesOutPerSec);
        }

        @Override
        public void run() {
            while(!queue.isEmpty()) {
                String topic = null;
                try {
                    topic = queue.poll(1, TimeUnit.SECONDS);
                    if(topic == null) {
                        continue;
                    }

                    Set<String> ips = zkOperation.getBrokerIps(topic);

                    storeKafkaServerInfo(topic, ips, "kafka.server", "BrokerTopicMetrics");
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("get BrokerIps of topic error, topic={} \n{}", topic, ExceptionUtils.getFullStackTrace(e));
                } catch (MalformedObjectNameException | AttributeNotFoundException | MBeanException | ReflectionException | InstanceNotFoundException | IOException e) {
                    LOGGER.error("get metrics of topic error, topic={} \n{}", topic, ExceptionUtils.getFullStackTrace(e));
                } catch (Exception e) {
                    LOGGER.error("store topic metrics error, topic={} \n{}", topic, ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
    }


    //get consumer offset of partitions
    public static class ConsumerGroupWorker implements Runnable{
        private LinkedBlockingQueue<String> consumerGroupsQueue;
        private ZookeeperOperation zkOperation;
        private String clusterName;

        public ConsumerGroupWorker(LinkedBlockingQueue<String> consumerGroupsQueue, ZookeeperOperation zkOperation, String clusterName){
            this.consumerGroupsQueue = consumerGroupsQueue;
            this.zkOperation = zkOperation;
            this.clusterName = clusterName;
        }

        @Override
        public void run() {
            while(!consumerGroupsQueue.isEmpty()) {
                String consumerGroup = null;
                try {
                    consumerGroup = consumerGroupsQueue.poll(1, TimeUnit.SECONDS);
                    if (consumerGroup == null) {
                        continue;
                    }

                    //get topics of ConsumerGroup
                    List<String> topicsOfConsumer;
                    topicsOfConsumer = zkOperation.getTopicsOfConsumer(consumerGroup);

                    for (String topic : topicsOfConsumer) {
                        //get partitions of topic
                        List<String> partitionIds;
                        try {
                            partitionIds = zkOperation.getPartitionsOfTopic(consumerGroup, topic);
                        } catch (KeeperException | InterruptedException e) {
                            LOGGER.error("get partitions of topic error, consumer={}, topic={} \n{}", consumerGroup, topic, ExceptionUtils.getFullStackTrace(e));
                            continue;
                        }

                        //get consumer offset of partition
                        //把分区的consumer offset最近一次修改时间超过7天的topic过滤掉
                        Map<String, Long> consumerOffsetMap = new HashMap<>();
                        boolean isJump = false;
                        for (String partitionId : partitionIds) {
                            Long consumerOffset;
                            try {
                                consumerOffset = zkOperation.getPartitionInfo(consumerGroup, topic, partitionId);
                                if (consumerOffset == null) {
                                    isJump = true;
                                    break;
                                }
                            } catch (KeeperException | InterruptedException e) {
                                LOGGER.error("get partitions of topic error, consumer={}, topic={}, partition={} \n{}", consumerGroup, topic, partitionId, ExceptionUtils.getFullStackTrace(e));
                                continue;
                            }
                            consumerOffsetMap.put(partitionId, consumerOffset);
                        }
                        //若达到跳出条件(最近一次修改时间超过7天), 则跳出本次循环
                        if (isJump) {
                            continue;
                        }

                        //get producer LogEndOffset of partition
                        Map<String, Long> logEndOffsetMap;     //key:partition id, value:log end offset
                        Map<String, String> leaderPartitions;  //key:partition id, value:leader ip
                        try {
                            leaderPartitions = zkOperation.getLeaderPartitionsOfTopic(topic);
//                            logEndOffsetMap = jmxOperation.getLogAttribute(topic, leaderPartitions, "kafka.log", "Log", "LogEndOffset", "Value");
                            logEndOffsetMap = KafkaState.getLogEndOffsetsOfTopic(topic, leaderPartitions);
                        } catch (Exception e) {
                            LOGGER.error("get LogEndOffset error, topic={} \n{}", topic, ExceptionUtils.getFullStackTrace(e));
                            continue;
                        }

                        storeConsumerOffset(consumerGroup, topic, consumerOffsetMap, logEndOffsetMap, leaderPartitions);
                    }
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("get topics of consumer error, consumer={} \n{}", consumerGroup, ExceptionUtils.getFullStackTrace(e));
                } catch (Exception e) {
                    LOGGER.error("store consumer offset error \n{}", ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

        private void storeConsumerOffset(String consumerGroup, String topic, Map<String, Long> consumerOffsetMap, Map<String, Long> logEndOffsetMap, Map<String, String> leaderPartitions) throws Exception {
            Metric<String> clusterInfo = Metric.newEntity("cluster", clusterName);
            Metric<String> consumerGroupInfo = Metric.newEntity("consumer", consumerGroup);
            Metric<String> topicInfo = Metric.newEntity("topic", topic);
            long totalLag = 0;

            for (Map.Entry<String, String> entry: leaderPartitions.entrySet()) {
                String partitionId = entry.getKey();
                String ip = entry.getValue();
                Long consumerOffset = consumerOffsetMap.get(partitionId);
                Long logEndOffset = logEndOffsetMap.get(partitionId);

                //若没采集到consumerOffset和logEndOffset,则丢弃该分区数据
                if(consumerOffset == null || logEndOffset == null){
                    continue;
                }
                Metric<String> partitionInfo = Metric.newEntity("partition", partitionId);
                Metric<String> ipInfo = Metric.newEntity("ip", ip);
                Metric<Long> consumerOffsetInfo = Metric.newEntity("ConsumerOffset", consumerOffset);
                Metric<Long> logEndOffsetInfo = Metric.newEntity("LogEndOffset", logEndOffset);
                long lag = logEndOffset - consumerOffset;
                Metric<Long> lagInfo = Metric.newEntity("Lag", lag);
                totalLag = totalLag + lag;

                MetricStore.store("dip-kafka-partition-monitor", TIMESTAMP, clusterInfo, consumerGroupInfo, topicInfo, partitionInfo, ipInfo, consumerOffsetInfo, logEndOffsetInfo, lagInfo);
            }
            Metric<Long> totalLagInfo = Metric.newEntity("TotalLag", totalLag);
            MetricStore.store("dip-kafka-consumer-lag-monitor", TIMESTAMP, clusterInfo, consumerGroupInfo, topicInfo, totalLagInfo);
        }
    }


    public static void main(String[] args) {
        for (Object cluster : clusterList) {
            LinkedTreeMap clusterMap = (LinkedTreeMap)cluster;
            String clusterName = (String)clusterMap.get("clusterName");
            LOGGER.info("clusterName={}", clusterName);
            String zookeeperAddress = (String)clusterMap.get("zookeeperAddress");
            LOGGER.info("zookeeperAddress={}", zookeeperAddress);
            int threadNumber = ((Double)clusterMap.get("threadNumber")).intValue();
            LOGGER.info("threadNumber={}", threadNumber);
            String jmxPort = (String)clusterMap.get("jmxPort");
            LOGGER.info("jmxPort={}", jmxPort);

            long startTime = System.currentTimeMillis();

            ZookeeperOperation zkOperation = null;
            JmxOperation jmxOperation = null;
            try {
                //init zookeeper
                zkOperation = new ZookeeperOperation(zookeeperAddress);

                Set<String> topics;
                Set<String> ips;
                List<String> consumerGroups;
                try {
                    ips = zkOperation.getAllBrokerIps();
                    topics = zkOperation.getAllTopics();
                    consumerGroups = zkOperation.getConsumerGroups();

                    //init all JmxBroker
                    jmxOperation = new JmxOperation(ips, jmxPort);
                } catch (KeeperException | InterruptedException | UnknownHostException | TimeoutException e) {
                    LOGGER.error("init JmxBroker error: {}", ExceptionUtils.getStackTrace(e));
                    return;
                }
                LOGGER.info("broker number: {}, topic number: {}", ips.size(), topics.size());


                LinkedBlockingQueue<String> topicsQueue = new LinkedBlockingQueue<>(topics);
                LinkedBlockingQueue<String> consumerGroupsQueue = new LinkedBlockingQueue<>(consumerGroups);
                LinkedBlockingQueue<String> ipsQueue = new LinkedBlockingQueue<>(ips);

                ExecutorService executor = Executors.newCachedThreadPool();
                for (int i = 0; i < threadNumber; i++) {
                    //获取所有topic的生产消费速率
                    executor.execute(new TopicWorker(topicsQueue, jmxOperation, zkOperation, clusterName));
                    //获取active consumer分区堆积量
                    executor.execute(new ConsumerGroupWorker(consumerGroupsQueue, zkOperation, clusterName));
                    //获取所有broker流量
                    executor.execute(new IpWorker(ipsQueue, jmxOperation, clusterName));
                }
                executor.shutdown();
                try {
                    boolean flag = executor.awaitTermination(30, TimeUnit.SECONDS);
                    if (flag) {
                        LOGGER.info("ExecutorService is terminated, all tasks have completed execution");
                    } else {
                        LOGGER.error("ExecutorService is terminated, the timeout occurs");
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("wait ExecutorService terminated error: {}", ExceptionUtils.getStackTrace(e));
                }
            }finally {
                //close jmx connection
                if(jmxOperation != null) {
                    jmxOperation.close();
                }

                //close zookeeper
                if(zkOperation != null) {
                    zkOperation.close();
                }
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info("total time: {}ms", endTime - startTime);
        }
    }
}
