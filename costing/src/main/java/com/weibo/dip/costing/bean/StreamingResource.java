package com.weibo.dip.costing.bean;

import java.util.Date;

/**
 * Created by yurun on 18/4/25.
 */
public class StreamingResource {

  private int id;
  private String appName;
  private String productUuid;
  private int cores;
  private int memories;
  private int server;
  private Date collectTimestamp;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getProductUuid() {
    return productUuid;
  }

  public void setProductUuid(String productUuid) {
    this.productUuid = productUuid;
  }

  public int getCores() {
    return cores;
  }

  public void setCores(int cores) {
    this.cores = cores;
  }

  public int getMemories() {
    return memories;
  }

  public void setMemories(int memories) {
    this.memories = memories;
  }

  public int getServer() {
    return server;
  }

  public void setServer(int server) {
    this.server = server;
  }

  public Date getCollectTimestamp() {
    return collectTimestamp;
  }

  public void setCollectTimestamp(Date collectTimestamp) {
    this.collectTimestamp = collectTimestamp;
  }

}
