package com.weibo.dip.data.platform.falcon.kafka.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.IOException;

public class JmxBroker implements Closeable{
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxBroker.class);
    private MBeanServerConnection conn;
    private String ipAndPort;
    private JMXConnector connector;

    public JmxBroker(String ipAndPort){
        this.ipAndPort = ipAndPort;
    }

    public void initConnection() throws IOException {
        String jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
        connector = JMXConnectorFactory.connect(serviceURL, null);
        conn = connector.getMBeanServerConnection();

        LOGGER.info("have connected JMX, jmxUrl: {}", jmxURL);
    }

    public Object getBrokerAttribute(String category, String type, String metric, String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException, MalformedObjectNameException {
        String objectNameStr;
        if(metric!=null&&metric.length()==0)
            objectNameStr = category + ":type=" + type;
        else
            objectNameStr = category + ":type=" + type + ",name=" + metric;

        ObjectName objectName = new ObjectName(objectNameStr);

        if(!conn.isRegistered(objectName)){
            return null;
        }
        return  conn.getAttribute(objectName, attribute);
    }

    public Object getAttribute(String category, String type, String metric, String topic, String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException, IOException, MalformedObjectNameException {
        String objectNameStr = category + ":type=" + type + ",name=" + metric + ",topic=" + topic;
        ObjectName objectName = new ObjectName(objectNameStr);

        if(!conn.isRegistered(objectName)){
            return null;
        }
        return  conn.getAttribute(objectName, attribute);
    }

    public Object getLogAttribute(String category, String type, String metric, String topic, String partition, String attribute) throws MalformedObjectNameException, IOException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
        String objectNameStr = category + ":type=" + type + ",name=" + metric + ",topic=" + topic + ",partition=" + partition;

        ObjectName objectName = new ObjectName(objectNameStr);

        if(!conn.isRegistered(objectName)){
            return null;
        }
        return conn.getAttribute(objectName, attribute);
    }

    @Override
    public void close() throws IOException {
        if(connector != null){
            connector.close();
        }
        LOGGER.debug("JMXConnector has been closed");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JmxBroker jmxBroker = (JmxBroker) o;

        return ipAndPort != null ? ipAndPort.equals(jmxBroker.ipAndPort) : jmxBroker.ipAndPort == null;
    }

    @Override
    public int hashCode() {
        return ipAndPort != null ? ipAndPort.hashCode() : 0;
    }

}
