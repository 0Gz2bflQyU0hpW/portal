package com.weibo.dip.platform.zookeeper;

import com.weibo.dip.data.platform.commons.Symbols;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientServer {
  private static Logger LOGGER = LoggerFactory.getLogger(ZkClientServer.class);

  private static final String CONNECT_ADDR = "10.13.4.44:2181";
  private static final String UNKNOW_HOST = "UnknownHost";
  private static final String PARENT_PATH = "/test";
  private static final int SESSION_TIMEOUT = 10000;
  private static final int CONNECTION_TIMEOUT = 10000;
  private static final int RETRY_TIMES = 3;
  private static final int SLEEP_SECONDS = 1;

  private ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR, SESSION_TIMEOUT),
      CONNECTION_TIMEOUT);

  private String getHostName() {
    String hostName;
    try {
      InetAddress ia = InetAddress.getLocalHost();
      hostName = ia.getHostName();
    } catch (UnknownHostException e) {
      LOGGER.error("UnknownHostException {}", e);
      hostName = UNKNOW_HOST;
    }
    return hostName;
  }

  /**
   * create node.
   */
  public void createNode() {
    for (int times = 0; times < RETRY_TIMES; times++) {
      if (!UNKNOW_HOST.equals(getHostName())) {
        String path = PARENT_PATH + Symbols.SLASH + getHostName() + Symbols.AMPERSAND;
        LOGGER.info("PATH: {}", path);
        zkc.createEphemeralSequential(path, null);
        LOGGER.info("create node {}", PARENT_PATH + Symbols.SLASH + getHostName());
        break;
      }
      try {
        TimeUnit.SECONDS.sleep(SLEEP_SECONDS);
      } catch (InterruptedException e) {
        ExceptionUtils.getFullStackTrace(e);
      }
    }
  }

  /**
   * judge node is active or not.
   * @return isActive
   */
  public boolean isActiveNode() {
    List<String> list = zkc.getChildren(PARENT_PATH);
    if (list.size() == 0) {
      return false;
    }
    TreeMap<Integer, String> map = new TreeMap<>();
    for (String node : list) {
      String[] arr = node.split(Symbols.AMPERSAND);
      String hostName = arr[0];
      Integer index = Integer.valueOf(arr[1]);
      map.put(index, hostName);
    }

    String localHost = map.firstEntry().getValue();
    if (null != localHost && localHost.equals(getHostName())) {
      return true;
    }
    return false;
  }

  public List<String> getAliveNodes() {
    return zkc.getChildren(PARENT_PATH);
  }

}
