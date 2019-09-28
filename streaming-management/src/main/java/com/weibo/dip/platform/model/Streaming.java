package com.weibo.dip.platform.model;

import java.util.Date;

public class Streaming {

  private int id;

  private String appName;

  private String appId;

  private int state;

  private Date lastUpdateTime;

  private Date createTime;

  private String creator;

  private String killer;

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

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getState() {
    return state;
  }

  public void setState(int state) {
    this.state = state;
  }

  public Date getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(Date lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public String getCreator() {
    return creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public String getKiller() {
    return killer;
  }

  public void setKiller(String killer) {
    this.killer = killer;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Streaming)) {
      return false;
    }
    Streaming streaming = (Streaming) obj;
    return streaming.id == id
        && streaming.appName.equals(appName)
        && streaming.state == state;
  }

  @Override
  public String toString() {
    return appName + "->" + state;
  }
}
