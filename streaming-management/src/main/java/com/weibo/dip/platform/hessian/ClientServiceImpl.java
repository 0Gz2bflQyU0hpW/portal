package com.weibo.dip.platform.hessian;

import com.caucho.hessian.server.HessianServlet;
import com.weibo.dip.platform.client.Client;
import com.weibo.dip.web.model.realtime.StreamingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClientServiceImpl extends HessianServlet implements ClientService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientServiceImpl.class);

  private Client client = new Client();

  @Override
  public int start(String appName, String creator) {
    LOGGER.info("receive request start application {} ", appName);
    return client.startApplication(appName, creator);
  }

  @Override
  public int stop(String appName) {
    LOGGER.info("receive request stop application {} ", appName);
    return client.killApplication(appName);
  }

  @Override
  public int restart(String appName) {
    LOGGER.info("receive request restart application {} ", appName);
    return client.restartApplication(appName);
  }

  @Override
  public List<StreamingInfo> list() {
    return client.listAllApplication();
  }

  @Override
  public int insert(StreamingInfo streamingInfo) {
    return client.insert(streamingInfo);
  }

  @Override
  public int delete(String name) {
    return client.delete(name);
  }

  @Override
  public int update(StreamingInfo streamingInfo) {
    return client.update(streamingInfo);
  }

  @Override
  public StreamingInfo findByName(String name) {
    return client.findByName(name);
  }

  @Override
  public boolean isExist(String name) {
    return client.isExit(name);
  }
}
