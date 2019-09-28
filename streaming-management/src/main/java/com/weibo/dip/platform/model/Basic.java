package com.weibo.dip.platform.model;

import java.io.Serializable;

public class Basic implements Serializable {

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
}
