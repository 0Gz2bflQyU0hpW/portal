package com.weibo.dip.platform.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by haisen on 2018/7/30. */
public class ZkExecutor implements Watcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkExecutor.class);

  private static final String CONNECT_ADDR = "10.13.4.44:2181";
  private static final String PARENT_PATH = "/testHA";
  private static final String HTTP = "http://";
  private static final String ADDRESS_HES = ":8088/service/clientservice";
  private static final int SESSION_TIMEOUT = 10000;

  private static ZooKeeper zooKeeper;

  /** initial connection. */
  public ZkExecutor() {
    connectServer();
  }

  private void connectServer() {
    try {
      zooKeeper = new ZooKeeper(CONNECT_ADDR, SESSION_TIMEOUT, this);
      LOGGER.info(
          "connection {} success , and session is {}", CONNECT_ADDR, zooKeeper.getSessionId());
    } catch (IOException e) {
      LOGGER.error("connection {} errror , {}", CONNECT_ADDR, ExceptionUtils.getFullStackTrace(e));
    }
  }

  /** create node. */
  public boolean createNode() {
    if (zooKeeper == null) {
      return false;
    }
    boolean bool = false;
    try {
      String data = HTTP + InetAddress.getLocalHost().getHostAddress() + ADDRESS_HES;
      zooKeeper.create(PARENT_PATH, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      LOGGER.info("Create znode {} success with data {}", PARENT_PATH, data);
      addWatcher();
      bool = true;
      ProgramControl.start();
    } catch (KeeperException e) {
      bool = getKeeperException(e);
    } catch (InterruptedException | UnknownHostException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
    return bool;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      case None:
        switch (watchedEvent.getState()) {
          case Expired:
            // The serving cluster has expired this session. 重连 可能会一直重连
            ProgramControl.stop();
            connectServer();
            LOGGER.info(
                "The serving cluster has expired a session ,"
                    + " connection again to get a new seeeion of {}",
                zooKeeper.getSessionId());
            break;
          case Disconnected:
            ProgramControl.stop();
            LOGGER.error("{} connection broken ", watchedEvent.getPath());
            //  send a watcher
            break;
          case SyncConnected:
            break;
          default:
            LOGGER.error(String.valueOf(watchedEvent.getState()));
            // include AuthFailed / ConnectedReadOnly / SaslAuthenticated
            //  send a watch
            break;
        }
        break;
      case NodeDeleted:
        LOGGER.info("node was delete try to create new znode ");
        ProgramControl.stop();
        while (!createNode()) {
          LOGGER.info("create znode again ");
        }
        break;
      default:
        LOGGER.info(String.valueOf(watchedEvent.getType()));
        break;
    }
    addWatcher();
  }

  /** remove znode , for test ,it's cause NodeDeleted. */
  private void removeZnode() {
    try {
      zooKeeper.delete(PARENT_PATH, -1);
    } catch (InterruptedException | KeeperException e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }

  private void addWatcher() {
    if (zooKeeper != null) {
      try {
        zooKeeper.exists(PARENT_PATH, true);
        LOGGER.info("add watch to {} success", PARENT_PATH);
      } catch (KeeperException e) {
        getKeeperException(e);
      } catch (InterruptedException e) {
        LOGGER.error("add watcher error : {}", ExceptionUtils.getFullStackTrace(e));
        // send a watch
      }
    }
  }

  private boolean getKeeperException(KeeperException e) {
    boolean bool = false;
    LOGGER.info(String.valueOf(e.code()));
    switch (e.code()) {
      case NODEEXISTS:
        ProgramControl.stop();
        bool = true;
        break;
      case SESSIONEXPIRED:
        connectServer();
        break;
      case SESSIONMOVED:
        connectServer();
        break;
      case OK:
        bool = true;
        break;
      default:
        break;
    }
    addWatcher();
    return bool;
  }

  public static void main(String[] args) {

    Thread thread =
        new Thread(
            () -> {
              ZkExecutor zkExecutor = new ZkExecutor();
              zkExecutor.createNode();
              while (!Thread.interrupted()) {
               /* try {
                  TimeUnit.MILLISECONDS.sleep(15000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                System.out.println("sleep 1 end");
                zkExecutor.removeZnode();
                try {
                  TimeUnit.MILLISECONDS.sleep(15000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                System.out.println("sleep 2 end");*/
              }
            });
    thread.setName("ZkExecutor");
    thread.start();
  }
}
