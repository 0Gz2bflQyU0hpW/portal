package com.weibo.dip.data.platform.falcon.kafka.metric;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class JmxOperation implements Closeable{
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxOperation.class);
    private static final String DEFAULT_JMX_PORT = "7999";
    private static final int DEFAULT_THREAD_NUMBER = 10;

    private Map<String, JmxBroker> brokersMap = new Hashtable<>();  //存储所有broker
    private String jmxPort;

    public JmxOperation(Set<String> ips) throws TimeoutException, InterruptedException {
        this(ips, DEFAULT_JMX_PORT);
    }

    public JmxOperation(Set<String> ips, String jmxPort) throws TimeoutException, InterruptedException {
        this(ips, jmxPort, DEFAULT_THREAD_NUMBER);
    }

    public JmxOperation(Set<String> ips, String jmxPort, int threadNumber) throws InterruptedException, TimeoutException {
        this.jmxPort = jmxPort;
        LinkedBlockingQueue<String> ipsQueue = new LinkedBlockingQueue<>(ips);


        ExecutorService ipExecutor = Executors.newFixedThreadPool(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            ipExecutor.execute(new IpConsumeWorker(ipsQueue));
        }
        ipExecutor.shutdown();
        try {
            boolean flag = ipExecutor.awaitTermination(1, TimeUnit.MINUTES);
            if (flag) {
                LOGGER.info("ExecutorService(consume brokerIp) is terminated, all tasks have completed execution");
            } else {
                throw new TimeoutException("ExecutorService(consume brokerIp) is terminated, the timeout occurs");
            }
        } catch (InterruptedException e) {
            throw(e);
        }
    }

    //init JmxBroker
    private class IpConsumeWorker implements Runnable {
        private LinkedBlockingQueue<String> queue;

        public IpConsumeWorker(LinkedBlockingQueue<String> queue){
            this.queue = queue;
        }

        @Override
        public void run() {
            while(!queue.isEmpty()){
                try {
                    String ip = queue.poll(1, TimeUnit.SECONDS);
                    if(ip != null) {
                        initBroker(ip);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("fetch ip from LinkedBlockingQueue error: {}", ExceptionUtils.getStackTrace(e));
                } catch (IOException e) {
                    LOGGER.error("init jmxConnection error: {}", ExceptionUtils.getStackTrace(e));
                }
            }
        }
    }

    private void initBroker(String ip) throws IOException {
        String ipPort = ip + ":" + jmxPort;
        JmxBroker jmxBroker = new JmxBroker(ipPort);
        jmxBroker.initConnection();
        brokersMap.put(ipPort, jmxBroker);
    }

    private Set<JmxBroker> getBrokersOfTopic(Set<String> ips){
        Set<String> ipPorts = new HashSet<>();
        for (String ip: ips) {
            ipPorts.add(ip + ":" + jmxPort);
        }

        Set<JmxBroker> brokersOfTopic = new HashSet<>();     //存储某个topic对应的broker
        for(String ipPort : ipPorts){
            //若ipPort未在预初始化的broker池子,则创建这个ipPort对应的JmxBroker对象,并加入池子
            if(!brokersMap.containsKey(ipPort)){
                JmxBroker jmxBroker = new JmxBroker(ipPort);
                brokersMap.put(ipPort, jmxBroker);
            }
            brokersOfTopic.add(brokersMap.get(ipPort));
        }
        return brokersOfTopic;
    }

    public <T extends Number> T getBrokerAttribute(String ip, String category, String type, String metric, String attribute) throws AttributeNotFoundException, InstanceNotFoundException, IOException, ReflectionException, MalformedObjectNameException, MBeanException {
        String ipPort = ip + ":" + jmxPort;
        if(!brokersMap.containsKey(ipPort)){
            JmxBroker jmxBroker = new JmxBroker(ipPort);
            brokersMap.put(ipPort, jmxBroker);
        }
        JmxBroker jmxBroker = brokersMap.get(ipPort);

        T val = null;
        try{
            Object obj = jmxBroker.getBrokerAttribute(category, type, metric, attribute);
            if(obj == null){
                LOGGER.warn("the MBean has not registered in the MBean server, ip={}, metric={}", ip, metric);
            }else {
                val = (T) obj;
            }
        }catch (MalformedObjectNameException | AttributeNotFoundException | MBeanException | ReflectionException | InstanceNotFoundException | IOException e) {
            throw e;
        }
        return val;
    }

    public CompositeData getCompositeDataAttribute(String ip, String category, String type, String metric, String attribute) throws AttributeNotFoundException, InstanceNotFoundException, IOException, ReflectionException, MalformedObjectNameException, MBeanException {
        String ipPort = ip + ":" + jmxPort;
        if(!brokersMap.containsKey(ipPort)){
            JmxBroker jmxBroker = new JmxBroker(ipPort);
            brokersMap.put(ipPort, jmxBroker);
        }
        JmxBroker jmxBroker = brokersMap.get(ipPort);

        CompositeData val = null;
        try{
            Object obj = jmxBroker.getBrokerAttribute(category, type, metric, attribute);
            if(obj == null){
                LOGGER.warn("the MBean has not registered in the MBean server, ip={}, metric={}", ip, metric);
            }else {
                val = (CompositeData) obj;
            }
        } catch (MalformedObjectNameException | AttributeNotFoundException| MBeanException | ReflectionException | InstanceNotFoundException | IOException e) {
            throw e;
        }
        return val;
    }

    public double getAttribute(String topic, Set<String> ips, String category, String type, String metric, String attribute) throws MalformedObjectNameException, InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        double val = 0;
        Set<JmxBroker> brokersOfTopic = this.getBrokersOfTopic(ips);
        for(JmxBroker jmxBroker : brokersOfTopic){
            try {
                Object obj = jmxBroker.getAttribute(category, type, metric, topic, attribute);
                if(obj == null){
                    LOGGER.warn("the MBean has not registered in the MBean server, topic={}, metric={}", topic, metric);
                    break;
                }
                val += (double) obj;
            } catch (MalformedObjectNameException | AttributeNotFoundException | MBeanException | ReflectionException | InstanceNotFoundException | IOException e) {
                throw e;
            }
        }
        return val;
    }

    public Map<String, Long> getLogAttribute(String topic, Map<String, String> leaderPartitions, String category, String type, String metric, String attribute) throws MalformedObjectNameException, InstanceNotFoundException, IOException, ReflectionException, AttributeNotFoundException, MBeanException {
        Map<String, Long> logMap = new HashMap<>();
        for (Map.Entry<String, String> entry: leaderPartitions.entrySet()) {
            String partitionId = entry.getKey();
            String ipPort = entry.getValue() + ":" + jmxPort;

            //若ipPort未在预初始化的broker池子,则创建这个ipPort对应的JmxBroker对象,并加入池子
            if(!brokersMap.containsKey(ipPort)){
                brokersMap.put(ipPort, new JmxBroker(ipPort));
            }

            JmxBroker jmxBroker = brokersMap.get(ipPort);
            Object obj = jmxBroker.getLogAttribute(category, type, metric, topic, partitionId, attribute);
            if(obj == null){
                LOGGER.warn("the MBean has not registered in the MBean server, topic={}, metric={}, partition={}", topic, metric, partitionId);
                continue;
            }
            logMap.put(partitionId, Long.valueOf(obj.toString()));
        }
        return logMap;
    }

    @Override
    public void close() {
        for (JmxBroker jmxBroker : brokersMap.values()) {
            try {
                jmxBroker.close();
            } catch (IOException e) {
                LOGGER.error("close JmxBroker error: {}", ExceptionUtils.getStackTrace(e));
            }
        }
    }
}
