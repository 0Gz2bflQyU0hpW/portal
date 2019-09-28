package com.weibo.dip.web.model.realtime;

import com.caucho.hessian.client.HessianProxyFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Created by haisen on 2018/7/5. */
public class RpcService implements Watcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcService.class);

  private static HessianProxyFactory factory = new HessianProxyFactory();

  private static final String CONNECT_ADDR = "10.13.4.44:2181";
  private static final String PARENT_PATH = "/testHA";
  private static final int SESSION_TIMEOUT = 10000;

  private static ZooKeeper zooKeeper;

  private static String rpcaddress;

  private static ClientService clientService = null;

  private static RpcService rpcService = null;

  /** 构造函数. */
  private RpcService() {
    connectServer();
    rpcaddress = getAddress();
    createFactory();
  }

  /**
   * get RpcService.
   *
   * @return single
   */
  public static RpcService getRpcService() {
    if (rpcaddress == null) {
      synchronized (RpcService.class) {
        if (rpcaddress == null) {
          rpcService = new RpcService();
        }
      }
    }
    return rpcService;
  }

  /**
   * start.
   *
   * @param appName appname
   * @return success or not 1:success
   */
  public int start(String appName, String creator) {
    if (clientService == null) {
      return -2;
    }
    return clientService.start(appName, creator);
  }

  /**
   * stop.
   *
   * @param appName appname
   * @return success or not 1:success
   */
  public int stop(String appName) {
    if (clientService == null) {
      return -2;
    }
    return clientService.stop(appName);
  }

  /**
   * restart.
   *
   * @param appName appname
   * @return success or not 1:success
   */
  public int restart(String appName) {
    if (clientService == null) {
      return -2;
    }
    return clientService.restart(appName);
  }

  /**
   * list all app info to show.
   *
   * @return list
   */
  public List<StreamingInfo> list() {
    if (clientService == null) {
      return new ArrayList<>();
    }
    return clientService.list();
  }
  /**
   * insert.
   *
   * @param streamingInfo info
   */
  public int insert(StreamingInfo streamingInfo) {
    if (clientService == null) {
      return -2;
    }
    return clientService.insert(streamingInfo);
  }

  /**
   * delete by name.
   *
   * @param name name
   */
  public int delete(String name) {
    if (clientService == null) {
      return -2;
    }
    return clientService.delete(name);
  }

  /**
   * update.
   *
   * @param streamingInfo info
   */
  public int update(StreamingInfo streamingInfo) {
    if (clientService == null) {
      return -2;
    }
    return clientService.update(streamingInfo);
  }

  /**
   * find by name.
   *
   * @param name name
   */
  public StreamingInfo findByName(String name) {
    if (clientService == null) {
      return null;
    }
    return clientService.findByName(name);
  }

  /**
   * is exist or not .
   *
   * @param name name
   */
  public boolean isExist(String name) {
    if (clientService == null) {
      return true;
    }
    return clientService.isExist(name);
  }

  private void connectServer() {
    try {
      zooKeeper = new ZooKeeper(CONNECT_ADDR, SESSION_TIMEOUT, this);
    } catch (IOException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  private String getAddress() {
    try {
      byte[] bytes = zooKeeper.getData(PARENT_PATH, this, new Stat());
      return new String(bytes);
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return null;
  }

  private void createFactory() {
    try {
      clientService = (ClientService) factory.create(ClientService.class, rpcaddress);
    } catch (MalformedURLException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      case NodeCreated:
        rpcaddress = getAddress();
        createFactory();
        break;
      case None:
        switch (watchedEvent.getState()) {
          case SyncConnected:
            break;
          default:
            connectServer();
            rpcaddress = getAddress();
            createFactory();
            break;
        }
        break;
      case NodeDataChanged:
        rpcaddress = getAddress();
        createFactory();
        break;
      default:
        break;
    }
    boolean flage = true;
    while (flage) {
      try {
        zooKeeper.exists(PARENT_PATH, true);
        flage = false;
      } catch (KeeperException | InterruptedException e) {
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        flage = true;
        try {
          TimeUnit.MILLISECONDS.sleep(10000);
        } catch (InterruptedException e1) {
          LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
  }

  /*public static void main(String[] args) {
    RpcService rpcService = RpcService.getRpcService();
    System.out.println(rpcService.list());
  }*/
}
