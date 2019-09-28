package com.weibo.dip.data.platform.falcon.kafka.KafkaTools;

import java.io.IOException;
//import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Wen on 2016/12/22.
 *
 */
public class KafkaTools implements Watcher{

    private ZooKeeper zk = null;
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
//    private Producer producer = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTools.class);
    //private final static String zookeeper="10.210.136.61:2181,10.210.136.62:2181,10.210.136.64:2181/kafka/test01";
    //Consumer
    public static ConsumerConnector GetKafkaConsumer(String zookeeper,String groupId){
        return kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
    }
    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "100000");
//        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    private void createConnection(String zookeeper,int sessionTimeout) throws IOException, InterruptedException {
        if (zk!=null){
            return;
        }
        this.releaseConnection();
        zk = new ZooKeeper(zookeeper,sessionTimeout,this);
        connectedSemaphore.await();
    }
    private void releaseConnection() {
        if(zk!=null){
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOGGER.error("连接释放失败,exception:"+e);
            }
        }
    }

    public void removeInvalidConsumer(String zookeeper,String groupId) throws Exception {
        createConnection(zookeeper,100000);
        rmr(zk,"/consumers/"+groupId);
        releaseConnection();
    }

    private  void rmr(ZooKeeper zk,String path) throws Exception {
        //获取路径下的节点
        List<String> children = zk.getChildren(path, false);

        for (String pathCd : children) {
            rmr(zk,path.equals("/")? "/"+pathCd : path+"/"+pathCd);
        }
        //删除节点,并过滤zookeeper节点和 /节点
        if (path != null && !path.trim().startsWith("/zookeeper") && !path.trim().equals("/")) {
            zk.delete(path, -1);
            //打印删除的节点路径
            System.out.println("被删除的节点为：" + path);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(KeeperState.SyncConnected == watchedEvent.getState()){
            connectedSemaphore.countDown();
        }
    }
    //Kafka Producer
//    public void sendMessgage(String topic,String message) throws IOException {
//        if (producer == null){
//            producer = createProducer();
//        }
//        producer.send(new KeyedMessage<String,String>(topic,message));
//    }
//
//    private Producer createProducer() throws IOException {
//        Properties prop = new Properties();
//        ClassLoader classLoader = KafkaTools.class.getClassLoader();
//        InputStream is = classLoader.getResourceAsStream("server.properties");
//        prop.load(is);
//        return new Producer<String,String>(new ProducerConfig(prop));
//    }
}
