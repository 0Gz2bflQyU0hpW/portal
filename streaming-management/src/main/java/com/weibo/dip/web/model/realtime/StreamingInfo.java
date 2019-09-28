package com.weibo.dip.web.model.realtime;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by haisen on 2018/8/15.
 */
public class StreamingInfo implements Serializable {
  private int id;

  private String appName;

  private String imageName;

  private String imageTag;

  private int driverCores;

  private String driverMemory;

  private int executorNums;

  private int executorCores;

  private String executorMems;

  private String confs;

  private String files;

  private String queue;

  private String className;

  private String jarName;

  private String department;

  private int isAvailable;

  private int collection;

  private int alertAlive;

  private int alertRepetitive;

  private String alertRepetitiveGroup;

  private int alertActive;

  private int alertActiveThreshold;

  private int alertAccumulation;

  private int alertAccumulationThreshold;

/*  private int alertRecentlyReceive;

  private float alertRecentlyReceiveThreshold;*/

  private int alertReceive;

  private float alertReceiveThreshold;

  private int alertSchedulingDelay;

  private int alertSchedulingDelayThreshold;

  private int alertProcessing;

  private int alertProcessingThreshold;

  private int alertActiveProcessing;

  private int alertActiveProcessingTimeThreshold;

  private int alertActiveProcessingNumThreshold;

  private int alertInactiveReceivers;

  private int alertInactiveReceiversThreshold;

  private int alertInactiveExecutors;

  private int alertInactiveExecutorsThreshold;

  private int alertError;

  private int alertErrorThreshold;

  private int alertInterval;

  private String alertGroup;

  private int alertState;

  private String appId;

  private int appstate;

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

  public String getImageName() {
    return imageName;
  }

  public void setImageName(String imageName) {
    this.imageName = imageName;
  }

  public String getImageTag() {
    return imageTag;
  }

  public void setImageTag(String imageTag) {
    this.imageTag = imageTag;
  }

  public int getDriverCores() {
    return driverCores;
  }

  public void setDriverCores(int driverCores) {
    this.driverCores = driverCores;
  }

  public String getDriverMemory() {
    return driverMemory;
  }

  public void setDriverMemory(String driverMemory) {
    this.driverMemory = driverMemory;
  }

  public int getExecutorNums() {
    return executorNums;
  }

  public void setExecutorNums(int executorNums) {
    this.executorNums = executorNums;
  }

  public int getExecutorCores() {
    return executorCores;
  }

  public void setExecutorCores(int executorCores) {
    this.executorCores = executorCores;
  }

  public String getExecutorMems() {
    return executorMems;
  }

  public void setExecutorMems(String executorMems) {
    this.executorMems = executorMems;
  }

  public String getConfs() {
    return confs;
  }

  public void setConfs(String confs) {
    this.confs = confs;
  }

  public String getFiles() {
    return files;
  }

  public void setFiles(String files) {
    this.files = files;
  }

  public String getQueue() {
    return queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getJarName() {
    return jarName;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public String getDepartment() {
    return department;
  }

  public void setDepartment(String department) {
    this.department = department;
  }

  public int getIsAvailable() {
    return isAvailable;
  }

  public void setIsAvailable(int isAvailable) {
    this.isAvailable = isAvailable;
  }

  public int getCollection() {
    return collection;
  }

  public void setCollection(int collection) {
    this.collection = collection;
  }

  public int getAlertAlive() {
    return alertAlive;
  }

  public void setAlertAlive(int alertAlive) {
    this.alertAlive = alertAlive;
  }

  public int getAlertRepetitive() {
    return alertRepetitive;
  }

  public void setAlertRepetitive(int alertRepetitive) {
    this.alertRepetitive = alertRepetitive;
  }

  public String getAlertRepetitiveGroup() {
    return alertRepetitiveGroup;
  }

  public void setAlertRepetitiveGroup(String alertRepetitiveGroup) {
    this.alertRepetitiveGroup = alertRepetitiveGroup;
  }

  public int getAlertActive() {
    return alertActive;
  }

  public void setAlertActive(int alertActive) {
    this.alertActive = alertActive;
  }

  public int getAlertActiveThreshold() {
    return alertActiveThreshold;
  }

  public void setAlertActiveThreshold(int alertActiveThreshold) {
    this.alertActiveThreshold = alertActiveThreshold;
  }

  public int getAlertAccumulation() {
    return alertAccumulation;
  }

  public void setAlertAccumulation(int alertAccumulation) {
    this.alertAccumulation = alertAccumulation;
  }

  public int getAlertAccumulationThreshold() {
    return alertAccumulationThreshold;
  }

  public void setAlertAccumulationThreshold(int alertAccumulationThreshold) {
    this.alertAccumulationThreshold = alertAccumulationThreshold;
  }

 /* public int getAlertRecentlyReceive() {
    return alertRecentlyReceive;
  }

  public void setAlertRecentlyReceive(int alertRecentlyReceive) {
    this.alertRecentlyReceive = alertRecentlyReceive;
  }

  public float getAlertRecentlyReceiveThreshold() {
    return alertRecentlyReceiveThreshold;
  }

  public void setAlertRecentlyReceiveThreshold(float alertRecentlyReceiveThreshold) {
    this.alertRecentlyReceiveThreshold = alertRecentlyReceiveThreshold;
  }*/

  public int getAlertReceive() {
    return alertReceive;
  }

  public void setAlertReceive(int alertReceive) {
    this.alertReceive = alertReceive;
  }

  public float getAlertReceiveThreshold() {
    return alertReceiveThreshold;
  }

  public void setAlertReceiveThreshold(float alertReceiveThreshold) {
    this.alertReceiveThreshold = alertReceiveThreshold;
  }

  public int getAlertSchedulingDelay() {
    return alertSchedulingDelay;
  }

  public void setAlertSchedulingDelay(int alertSchedulingDelay) {
    this.alertSchedulingDelay = alertSchedulingDelay;
  }

  public int getAlertSchedulingDelayThreshold() {
    return alertSchedulingDelayThreshold;
  }

  public void setAlertSchedulingDelayThreshold(int alertSchedulingDelayThreshold) {
    this.alertSchedulingDelayThreshold = alertSchedulingDelayThreshold;
  }

  public int getAlertProcessing() {
    return alertProcessing;
  }

  public void setAlertProcessing(int alertProcessing) {
    this.alertProcessing = alertProcessing;
  }

  public int getAlertProcessingThreshold() {
    return alertProcessingThreshold;
  }

  public void setAlertProcessingThreshold(int alertProcessingThreshold) {
    this.alertProcessingThreshold = alertProcessingThreshold;
  }

  public int getAlertActiveProcessing() {
    return alertActiveProcessing;
  }

  public void setAlertActiveProcessing(int alertActiveProcessing) {
    this.alertActiveProcessing = alertActiveProcessing;
  }

  public int getAlertActiveProcessingTimeThreshold() {
    return alertActiveProcessingTimeThreshold;
  }

  public void setAlertActiveProcessingTimeThreshold(int alertActiveProcessingTimeThreshold) {
    this.alertActiveProcessingTimeThreshold = alertActiveProcessingTimeThreshold;
  }

  public int getAlertActiveProcessingNumThreshold() {
    return alertActiveProcessingNumThreshold;
  }

  public void setAlertActiveProcessingNumThreshold(int alertActiveProcessingNumThreshold) {
    this.alertActiveProcessingNumThreshold = alertActiveProcessingNumThreshold;
  }

  public int getAlertInactiveReceivers() {
    return alertInactiveReceivers;
  }

  public void setAlertInactiveReceivers(int alertInactiveReceivers) {
    this.alertInactiveReceivers = alertInactiveReceivers;
  }

  public int getAlertInactiveReceiversThreshold() {
    return alertInactiveReceiversThreshold;
  }

  public void setAlertInactiveReceiversThreshold(int alertInactiveReceiversThreshold) {
    this.alertInactiveReceiversThreshold = alertInactiveReceiversThreshold;
  }

  public int getAlertInactiveExecutors() {
    return alertInactiveExecutors;
  }

  public void setAlertInactiveExecutors(int alertInactiveExecutors) {
    this.alertInactiveExecutors = alertInactiveExecutors;
  }

  public int getAlertInactiveExecutorsThreshold() {
    return alertInactiveExecutorsThreshold;
  }

  public void setAlertInactiveExecutorsThreshold(int alertInactiveExecutorsThreshold) {
    this.alertInactiveExecutorsThreshold = alertInactiveExecutorsThreshold;
  }

  public int getAlertError() {
    return alertError;
  }

  public void setAlertError(int alertError) {
    this.alertError = alertError;
  }

  public int getAlertErrorThreshold() {
    return alertErrorThreshold;
  }

  public void setAlertErrorThreshold(int alertErrorThreshold) {
    this.alertErrorThreshold = alertErrorThreshold;
  }

  public int getAlertInterval() {
    return alertInterval;
  }

  public void setAlertInterval(int alertInterval) {
    this.alertInterval = alertInterval;
  }

  public String getAlertGroup() {
    return alertGroup;
  }

  public void setAlertGroup(String alertGroup) {
    this.alertGroup = alertGroup;
  }

  public int getAlertState() {
    return alertState;
  }

  public void setAlertState(int alertState) {
    this.alertState = alertState;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getAppstate() {
    return appstate;
  }

  public void setAppstate(int appstate) {
    this.appstate = appstate;
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
  public String toString() {
    return "StreamingInfo{"
        + "id="
        + id
        + ", appName='"
        + appName
        + '\''
        + ", imageName='"
        + imageName
        + '\''
        + ", imageTag='"
        + imageTag
        + '\''
        + ", driverCores="
        + driverCores
        + ", driverMemory='"
        + driverMemory
        + '\''
        + ", executorNums="
        + executorNums
        + ", executorCores="
        + executorCores
        + ", executorMems='"
        + executorMems
        + '\''
        + ", confs='"
        + confs
        + '\''
        + ", files='"
        + files
        + '\''
        + ", queue='"
        + queue
        + '\''
        + ", className='"
        + className
        + '\''
        + ", jarName='"
        + jarName
        + '\''
        + ", department='"
        + department
        + '\''
        + ", isAvailable="
        + isAvailable
        + ", collection="
        + collection
        + ", alertAlive="
        + alertAlive
        + ", alertRepetitive="
        + alertRepetitive
        + ", alertRepetitiveGroup='"
        + alertRepetitiveGroup
        + '\''
        + ", alertActive="
        + alertActive
        + ", alertActiveThreshold="
        + alertActiveThreshold
        + ", alertAccumulation="
        + alertAccumulation
        + ", alertAccumulationThreshold="
        + alertAccumulationThreshold
        /*+ ", alertRecentlyReceive="
        + alertRecentlyReceive
        + ", alertRecentlyReceiveThreshold="
        + alertRecentlyReceiveThreshold*/
        + ", alertReceive="
        + alertReceive
        + ", alertReceiveThreshold="
        + alertReceiveThreshold
        + ", alertSchedulingDelay="
        + alertSchedulingDelay
        + ", alertSchedulingDelayThreshold="
        + alertSchedulingDelayThreshold
        + ", alertProcessing="
        + alertProcessing
        + ", alertProcessingThreshold="
        + alertProcessingThreshold
        + ", alertActiveProcessing="
        + alertActiveProcessing
        + ", alertActiveProcessingTimeThreshold="
        + alertActiveProcessingTimeThreshold
        + ", alertActiveProcessingNumThreshold="
        + alertActiveProcessingNumThreshold
        + ", alertInactiveReceivers="
        + alertInactiveReceivers
        + ", alertInactiveReceiversThreshold="
        + alertInactiveReceiversThreshold
        + ", alertInactiveExecutors="
        + alertInactiveExecutors
        + ", alertInactiveExecutorsThreshold="
        + alertInactiveExecutorsThreshold
        + ", alertError="
        + alertError
        + ", alertErrorThreshold="
        + alertErrorThreshold
        + ", alertInterval="
        + alertInterval
        + ", alertGroup='"
        + alertGroup
        + '\''
        + ", alertState="
        + alertState
        + ", appId='"
        + appId
        + '\''
        + ", appstate="
        + appstate
        + ", lastUpdateTime="
        + lastUpdateTime
        + ", createTime="
        + createTime
        + ", creator='"
        + creator
        + '\''
        + ", killer='"
        + killer
        + '\''
        + '}';
  }
}
