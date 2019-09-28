package com.weibo.dip.data.platform.falcon.kafka.metric;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ZookeeperOperation implements Closeable{
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperOperation.class);
    private Map<String, Map<String, String>> topicPartitionMap = new Hashtable<>();
    private ZooKeeper zk;
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {}
    };

    public ZookeeperOperation(String zkAddress){
        try {
            zk = new ZooKeeper(zkAddress, 4000, watcher);
        } catch (IOException e) {
            LOGGER.error("can not connect Zookeeper: {}", ExceptionUtils.getFullStackTrace(e));
        }
    }

    public Set<String> domainNames2ips(Set<String> domainNames) throws UnknownHostException {
        Set<String> ips = new HashSet<>();
        for(String domainName : domainNames){
            ips.add(InetAddress.getByName(domainName).getHostAddress());
        }
        return ips;
    }

    public Set<String> getAllBrokerIps() throws InterruptedException, UnknownHostException, KeeperException {
        Set<String> allIds = getAllBrokerIds();
        Set<String> hosts = getHosts(allIds);
        return domainNames2ips(hosts);
    }

    public Set<String> getBrokerIps(String topic) throws InterruptedException, UnknownHostException, KeeperException {
        Set<String> brokerIds =  getBrokerIdsOfTopic(topic);
        Set<String> hosts = getHosts(brokerIds);
        return domainNames2ips(hosts);
    }

    public String getHost(String brokerId) throws KeeperException, InterruptedException {
        String zkPath = "/brokers/ids/" + brokerId;
        String hostname = null;

        if(zk.exists(zkPath, watcher) != null){
            String brokerStr = new String(zk.getData(zkPath, watcher, new Stat()));
            Map<String, Object> brokerMap = GsonUtil.fromJson(brokerStr, HashMap.class);
            hostname = (String)brokerMap.get("host");
        }
        return hostname;
    }

    public Set<String> getHosts(Set<String> brokerIds) throws KeeperException, InterruptedException {
        Set<String> hosts = new HashSet<>();
        for (String brokerId: brokerIds) {
            String hostname = getHost(brokerId);

            if(hostname == null){
                LOGGER.warn("could not found the hostname of broker, BrokerId={}", brokerId);
            }else{
                hosts.add(hostname);
            }
        }
        return hosts;
    }

    public Set<String> getBrokerIdsOfTopic(String topicName) throws KeeperException, InterruptedException {
        String zkPath = "/brokers/topics/" + topicName + "/partitions";
        Set<String> brokerIds = new HashSet<>();

        if(zk.exists(zkPath, watcher) != null) {
            List<String> partitions = zk.getChildren(zkPath, watcher);

            for (String partition : partitions) {
                String state = new String(zk.getData(zkPath + "/" + partition + "/state", watcher, new Stat()));
                Double leader = (Double) GsonUtil.fromJson(state, HashMap.class).get("leader");
                brokerIds.add(String.valueOf(leader.intValue()));
            }
        }
        return brokerIds;
    }

    public Set<String> getAllTopics() throws KeeperException, InterruptedException {
        String zkPath = "/brokers/topics";
        Set<String> topics = new HashSet<>();

        if(zk.exists(zkPath, watcher) != null) {
            List<String> topicsList = zk.getChildren(zkPath, watcher);
            topics.addAll(topicsList);
        }
        return topics;
    }

    public Set<String> getAllBrokerIds() throws KeeperException, InterruptedException {
        String zkPath = "/brokers/ids";
        Set<String> ids = new HashSet<>();

        if(zk.exists(zkPath, watcher) != null) {
            List<String> idsList = zk.getChildren(zkPath, watcher);
            ids.addAll(idsList);
        }
        return ids;
    }

    public List<String> getConsumerGroups() throws KeeperException, InterruptedException {
        String zkPath = "/consumers";
        List<String> consumers = new ArrayList<>();

        if(zk.exists(zkPath, watcher) != null) {
            consumers = zk.getChildren(zkPath, watcher);
        }
        return consumers;
    }

    public List<String> getTopicsOfConsumer(String consumerGroup) throws KeeperException, InterruptedException {
        String zkPath = "/consumers" + "/" + consumerGroup + "/offsets";
        List<String> topicsOfConsumer = new ArrayList<>();

        if(zk.exists(zkPath, watcher) == null){
            LOGGER.warn("could not found topics of consumer, consumer={}", consumerGroup);
        }else{
            topicsOfConsumer = zk.getChildren(zkPath, watcher);
        }
        return topicsOfConsumer;
    }


    public List<String> getPartitionsOfTopic(String consumerGroup, String topic) throws KeeperException, InterruptedException {
        String zkPath = "/consumers" + "/" + consumerGroup + "/offsets" + "/" + topic;
        List<String> partitionsOfTopic = new ArrayList<>();

        if(zk.exists(zkPath, watcher) != null) {
            partitionsOfTopic = zk.getChildren(zkPath, watcher);
        }
        return partitionsOfTopic;
    }

    public Long getPartitionInfo(String consumerGroup, String topic, String partitionId) throws KeeperException, InterruptedException {
        String zkPath = "/consumers" + "/" + consumerGroup + "/offsets" + "/" + topic + "/" + partitionId;
        Long offset = null;

        if(zk.exists(zkPath, watcher) != null) {
            Stat stat = new Stat();
            offset = new Long(new String(zk.getData(zkPath, watcher, stat)));

            //若offset最近一次修改超过7天，则认为该ConsumerGroup失效
            if (stat.getMtime() < (System.currentTimeMillis() - 7 * 24 * 3600 * 1000)) {
                return null;
            }
        }
        return offset;
    }

    //key:partition, value:ip
    public Map<String, String> getLeaderPartitionsOfTopic(String topicName) throws KeeperException, InterruptedException, UnknownHostException {
        if(!topicPartitionMap.containsKey(topicName)) {
            String zkPath = "/brokers/topics/" + topicName + "/partitions";

            if(zk.exists(zkPath, watcher) != null) {
                List<String> partitions = zk.getChildren(zkPath, watcher);
                Map<String, String> leaderPartitions = new HashMap<>();

                for (String partition : partitions) {
                    String state = new String(zk.getData(zkPath + "/" + partition + "/state", watcher, new Stat()));
                    Double leader = (Double) GsonUtil.fromJson(state, HashMap.class).get("leader");
                    String brokerId = String.valueOf(leader.intValue());
                    String ip = InetAddress.getByName(getHost(brokerId)).getHostAddress();
                    leaderPartitions.put(partition, ip);
                }
                topicPartitionMap.put(topicName, leaderPartitions);
            }
        }
        return topicPartitionMap.get(topicName);
    }


    @Override
    public void close() {
        try {
            if(zk != null) {
                zk.close();
            }
        } catch (InterruptedException e) {
            LOGGER.error("close Zookeeper error: {}", ExceptionUtils.getStackTrace(e));
        }

    }
}

